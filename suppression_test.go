package incidentescalation

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// newSuppressionStore 建好策略并创建一个 firing 事件，策略共 3 步：
// step0 立即通知 alice；step1（+1 分钟）通知 bob；step2（再 +1 分钟）通知 carol。
func newSuppressionStore(t *testing.T) (*Store, string, time.Time) {
	t.Helper()
	s := NewMemoryStore()
	now := time.Unix(10_000, 0)
	if _, err := s.CreatePolicy(PolicyInput{
		ID: "p", Name: "p",
		Steps: []Step{
			{WaitBefore: 0, Targets: []string{"alice"}},
			{WaitBefore: time.Minute, Targets: []string{"bob"}},
			{WaitBefore: time.Minute, Targets: []string{"carol"}},
		},
	}, now); err != nil {
		t.Fatalf("seed policy: %v", err)
	}
	inc, err := s.CreateIncident(CreateIncidentRequest{
		RequestID: "inc-req", IncidentID: "inc-1", PolicyID: "p",
		Severity: "sev1", Detail: "db down",
	}, now)
	if err != nil {
		t.Fatalf("create incident: %v", err)
	}
	return s, inc.ID, now
}

func countByStatus(items []*OutboxItem) map[OutboxStatus]int {
	out := map[OutboxStatus]int{}
	for _, it := range items {
		out[it.Status]++
	}
	return out
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// TestSuppressionHoldsAndResumes 窗口内触发的升级被 held（带原因、不派发）；
// 窗口到期自动解除后，按最新级别裁决并把未处理级别一次性补回，每级只通知一次。
func TestSuppressionHoldsAndResumes(t *testing.T) {
	s, incID, t0 := newSuppressionStore(t)

	// step0 在抑制开始前已经触发并产生 pending 意图：规则不溯及既往。
	if n, err := s.ProcessDue(t0); err != nil || n != 1 {
		t.Fatalf("step0 before window: n=%d err=%v", n, err)
	}

	// 维护窗口 [t0+30s, t0+2h)：覆盖期间到期的 step1/step2。
	rule, err := s.CreateSuppression(CreateSuppressionRequest{
		RequestID: "sup-req", RuleID: "mw-1",
		Scope:      SuppressionScope{IncidentIDs: []string{incID}},
		Reason:     "数据库故障演练维护窗口",
		ValidFrom:  t0.Add(30 * time.Second),
		ValidUntil: t0.Add(2 * time.Hour),
	}, t0.Add(10*time.Second))
	if err != nil {
		t.Fatalf("create suppression: %v", err)
	}
	if rule.Status != SuppressionActive || rule.ReleaseCondition != ReleaseWindowEnd {
		t.Fatalf("unexpected rule: %+v", rule)
	}

	// 窗口内把时间推到很远：每次仍只进一步，触发的全部 held，绝不派发。
	if n, _ := s.ProcessDue(t0.Add(30 * time.Minute)); n != 1 {
		t.Fatalf("step1 in window advanced %d, want 1", n)
	}
	if n, _ := s.ProcessDue(t0.Add(40 * time.Minute)); n != 1 {
		t.Fatalf("step2 in window advanced %d, want 1", n)
	}
	items := s.ListOutbox(incID)
	counts := countByStatus(items)
	if counts[OutboxPending] != 1 || counts[OutboxHeld] != 2 {
		t.Fatalf("in-window outbox = %+v, want 1 pending + 2 held", counts)
	}
	if got := s.PendingOutbox(); len(got) != 1 {
		t.Fatalf("pending visible during window = %d, want only pre-window step0", len(got))
	}
	for _, it := range items {
		if it.Status == OutboxHeld {
			if it.SuppressionID != "mw-1" || it.SuppressionReason == "" {
				t.Fatalf("held item missing suppression reason: %+v", it)
			}
		}
	}

	// 窗口到期后扫描：自动解除，held 按最新级别补发为 pending，每级一条。
	n, err := s.ProcessDue(t0.Add(3 * time.Hour))
	if err != nil || n != 0 {
		t.Fatalf("post-window scan advanced %d (no timers left), err=%v", n, err)
	}
	items = s.ListOutbox(incID)
	counts = countByStatus(items)
	if counts[OutboxPending] != 3 || counts[OutboxHeld] != 0 || counts[OutboxCanceled] != 0 {
		t.Fatalf("after window outbox = %+v, want 3 pending, 0 held", counts)
	}
	kinds := map[OutboxKind]int{}
	for _, it := range items {
		kinds[it.Kind]++
	}
	if kinds[OutboxKindSuppressionResume] != 2 {
		t.Fatalf("resumed intents = %d, want 2", kinds[OutboxKindSuppressionResume])
	}

	// 解除后重复扫描：每级最多一次有效通知，不产生重复意图。
	if _, err := s.ProcessDue(t0.Add(4 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	if len(s.ListOutbox(incID)) != 3 {
		t.Fatalf("duplicate scan created extra intents")
	}

	got, err := s.GetSuppression("mw-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != SuppressionReleased || got.ReleaseCause != "window_end" {
		t.Fatalf("rule not auto-released: %+v", got)
	}
}

// TestSuppressionManualRelease 手工解除立即补发；重复解除报冲突。
func TestSuppressionManualRelease(t *testing.T) {
	s, incID, t0 := newSuppressionStore(t)
	if _, err := s.CreateSuppression(CreateSuppressionRequest{
		RequestID: "sup-req", RuleID: "mw-1",
		Scope:  SuppressionScope{IncidentIDs: []string{incID}},
		Reason: "维护", ValidFrom: t0, ValidUntil: t0.Add(2 * time.Hour),
		ReleaseCondition: ReleaseManual,
	}, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessDue(t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := s.PendingOutbox(); len(got) != 0 {
		t.Fatalf("held step leaked to pending: %d", len(got))
	}

	rule, err := s.ReleaseSuppression(ReleaseSuppressionRequest{
		RequestID: "rel-req", RuleID: "mw-1", ReleasedBy: "oncall-lead",
	}, t0.Add(30*time.Minute))
	if err != nil {
		t.Fatalf("manual release: %v", err)
	}
	if rule.Status != SuppressionReleased || rule.ReleaseCause != "manual" ||
		rule.ReleasedBy != "oncall-lead" {
		t.Fatalf("bad released rule: %+v", rule)
	}
	if pending := s.PendingOutbox(); len(pending) != 1 || pending[0].Target != "alice" {
		t.Fatalf("resumed pending = %+v", pending)
	}

	// 再次手工解除：冲突。
	if _, err := s.ReleaseSuppression(ReleaseSuppressionRequest{
		RequestID: "rel-req-2", RuleID: "mw-1",
	}, t0.Add(31*time.Minute)); !errors.Is(err, ErrConflict) {
		t.Fatalf("double release: want ErrConflict, got %v", err)
	}
	// 幂等重放返回同一已解除规则，不再变化。
	replay, err := s.ReleaseSuppression(ReleaseSuppressionRequest{
		RequestID: "rel-req", RuleID: "mw-1", ReleasedBy: "oncall-lead",
	}, t0.Add(40*time.Minute))
	if err != nil || replay.Status != SuppressionReleased {
		t.Fatalf("release replay: %v %+v", err, replay)
	}

	// manual 也受 ValidUntil 硬结束点兜底：另一条 manual 规则到期扫描即解除。
	s2, incID2, t1 := newSuppressionStore(t)
	if _, err := s2.CreateSuppression(CreateSuppressionRequest{
		RequestID: "sup-2", RuleID: "mw-2",
		Scope:  SuppressionScope{IncidentIDs: []string{incID2}},
		Reason: "维护2", ValidFrom: t1, ValidUntil: t1.Add(time.Hour),
		ReleaseCondition: ReleaseManual,
	}, t1); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.ProcessDue(t1.Add(2 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	r2, _ := s2.GetSuppression("mw-2")
	if r2.Status != SuppressionReleased {
		t.Fatalf("manual rule must still end at ValidUntil: %+v", r2)
	}
}

// TestSuppressionResolvedNotNotified 已解决事件不会因解除抑制重新发通知。
func TestSuppressionResolvedNotNotified(t *testing.T) {
	s, incID, t0 := newSuppressionStore(t)
	if _, err := s.CreateSuppression(CreateSuppressionRequest{
		RequestID: "sup-req", RuleID: "mw-1",
		Scope:  SuppressionScope{IncidentIDs: []string{incID}},
		Reason: "维护", ValidFrom: t0, ValidUntil: t0.Add(2 * time.Hour),
	}, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessDue(t0.Add(30 * time.Minute)); err != nil { // step0 held
		t.Fatal(err)
	}
	// 窗口内解决：held 全部撤销。
	if _, err := s.Resolve(ResolveRequest{
		RequestID: "res-req", IncidentID: incID, ResolvedBy: "alice",
	}, t0.Add(40*time.Minute)); err != nil {
		t.Fatal(err)
	}
	items := s.ListOutbox(incID)
	counts := countByStatus(items)
	if counts[OutboxHeld] != 0 || counts[OutboxPending] != 0 || counts[OutboxCanceled] != 1 {
		t.Fatalf("after resolve in window: %+v", counts)
	}
	// 即使窗口到期再扫，已解决事件也绝不补发。
	if _, err := s.ProcessDue(t0.Add(3 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	if pending := s.PendingOutbox(); len(pending) != 0 {
		t.Fatalf("resolved incident produced notifications: %+v", pending)
	}
}

// TestSuppressionAckedNotNotified 抑制期间确认的事件，held 升级随确认撤销。
func TestSuppressionAckedNotNotified(t *testing.T) {
	s, incID, t0 := newSuppressionStore(t)
	if _, err := s.CreateSuppression(CreateSuppressionRequest{
		RequestID: "sup-req", RuleID: "mw-1",
		Scope:  SuppressionScope{IncidentIDs: []string{incID}},
		Reason: "维护", ValidFrom: t0, ValidUntil: t0.Add(2 * time.Hour),
	}, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessDue(t0.Add(30 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acknowledge(AckRequest{
		RequestID: "ack-req", IncidentID: incID, AcknowledgedBy: "bob",
	}, t0.Add(35*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessDue(t0.Add(3 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	items := s.ListOutbox(incID)
	counts := countByStatus(items)
	if counts[OutboxPending] != 0 || counts[OutboxCanceled] != 1 {
		t.Fatalf("acked held items: %+v, want 1 canceled 0 pending", counts)
	}
}

// TestDuplicateSuppressionRejected 内容相同（范围+原因）即使解除时间不同，
// 也必须明确报 ErrAlreadyExists 并指出已有规则，不能静默延长/覆盖。
func TestDuplicateSuppressionRejected(t *testing.T) {
	s, incID, t0 := newSuppressionStore(t)
	base := CreateSuppressionRequest{
		RequestID: "sup-req", RuleID: "mw-1",
		Scope:      SuppressionScope{IncidentIDs: []string{incID}, Severities: []string{"sev1"}},
		Reason:     "维护窗口",
		ValidFrom:  t0,
		ValidUntil: t0.Add(time.Hour),
	}
	if _, err := s.CreateSuppression(base, t0); err != nil {
		t.Fatal(err)
	}

	// 相同范围+原因，仅解除时间不同：拒绝，错误里带已有规则 ID。
	dup := base
	dup.RequestID = "sup-req-2"
	dup.RuleID = "mw-2"
	dup.ValidUntil = t0.Add(3 * time.Hour)
	_, err := s.CreateSuppression(dup, t0)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("dup window: want ErrAlreadyExists, got %v", err)
	}
	if !contains(err.Error(), "mw-1") {
		t.Fatalf("error must name existing rule: %v", err)
	}

	// 仅解除条件不同也一样拒绝。
	dup2 := base
	dup2.RequestID = "sup-req-3"
	dup2.RuleID = "mw-3"
	dup2.ReleaseCondition = ReleaseManual
	if _, err := s.CreateSuppression(dup2, t0); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("dup content different condition: want ErrAlreadyExists, got %v", err)
	}

	// 原因不同 → 不是同一条规则，允许创建。
	other := base
	other.RequestID = "sup-req-4"
	other.RuleID = "mw-4"
	other.Reason = "另一个变更窗口"
	if _, err := s.CreateSuppression(other, t0); err != nil {
		t.Fatalf("different reason should be allowed: %v", err)
	}
	// 范围不同 → 允许创建。
	other2 := base
	other2.RequestID = "sup-req-5"
	other2.RuleID = "mw-5"
	other2.Scope = SuppressionScope{IncidentIDs: []string{incID}, Severities: []string{"sev2"}}
	if _, err := s.CreateSuppression(other2, t0); err != nil {
		t.Fatalf("different scope should be allowed: %v", err)
	}

	// 原窗口解除后，同样内容可以再建（之前没有被静默延长）。
	if _, err := s.ReleaseSuppression(ReleaseSuppressionRequest{
		RequestID: "rel-1", RuleID: "mw-1",
	}, t0.Add(30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	again := base
	again.RequestID = "sup-req-again"
	again.RuleID = "mw-1-again"
	again.ValidUntil = t0.Add(2 * time.Hour)
	if _, err := s.CreateSuppression(again, t0.Add(40*time.Minute)); err != nil {
		t.Fatalf("same content after release should be allowed: %v", err)
	}

	// 校验类错误。
	bad := CreateSuppressionRequest{RequestID: "bad-1", Reason: "", ValidFrom: t0, ValidUntil: t0.Add(time.Hour)}
	if _, err := s.CreateSuppression(bad, t0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty reason: want ErrInvalidArgument, got %v", err)
	}
	bad = CreateSuppressionRequest{RequestID: "bad-2", Reason: "x", ValidFrom: t0.Add(time.Hour), ValidUntil: t0}
	if _, err := s.CreateSuppression(bad, t0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("until before from: want ErrInvalidArgument, got %v", err)
	}
	if _, err := s.GetSuppression("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get missing: want ErrNotFound, got %v", err)
	}
}

// TestSeverityChangeDuringSuppression 抑制期间级别变化持续可见：
// 级别仍在抑制范围内时 held 不发；逃出范围时真正需要的升级立即恢复；
// 解除抑制时按最新级别而不是最初记录判断。
func TestSeverityChangeDuringSuppression(t *testing.T) {
	s, incID, t0 := newSuppressionStore(t)
	// 规则只抑制 sev1 级别。
	if _, err := s.CreateSuppression(CreateSuppressionRequest{
		RequestID: "sup-req", RuleID: "mw-1",
		Scope:  SuppressionScope{IncidentIDs: []string{incID}, Severities: []string{"sev1"}},
		Reason: "维护", ValidFrom: t0, ValidUntil: t0.Add(2 * time.Hour),
	}, t0); err != nil {
		t.Fatal(err)
	}
	// step0 触发：sev1 被 held。
	if _, err := s.ProcessDue(t0); err != nil {
		t.Fatal(err)
	}
	if pending := s.PendingOutbox(); len(pending) != 0 {
		t.Fatalf("sev1 step should be held, pending=%d", len(pending))
	}

	// 抑制期间级别变化为另一个仍被抑制范围内的级别？这里范围只有 sev1，
	// 先变成 sev1'（同值不变不允许），改为 sev-low 会逃出范围。
	// 1) 先记录一次“仍在范围内”的变化：调整规则范围含 sev2 不可行（规则不可变），
	//    改为验证直接升级到 sev2（逃出）→ held 立即补发。
	if _, err := s.UpdateSeverity(UpdateSeverityRequest{
		RequestID: "sev-req-1", IncidentID: incID, Severity: "sev2",
		Reason: "影响面扩大", UpdatedBy: "monitor",
	}, t0.Add(10*time.Minute)); err != nil {
		t.Fatalf("update severity: %v", err)
	}
	pending := s.PendingOutbox()
	if len(pending) != 1 || pending[0].Target != "alice" {
		t.Fatalf("escape-to-sev2 must resume held step: %+v", pending)
	}

	// 级别轨迹持续可见，且标记了发生时处于抑制窗口。
	inc, _ := s.GetIncident(incID)
	if inc.Severity != "sev2" || len(inc.SeverityChanges) != 2 {
		t.Fatalf("severity history wrong: %+v", inc.SeverityChanges)
	}
	last := inc.SeverityChanges[1]
	if last.From != "sev1" || last.To != "sev2" || !last.Suppressed {
		t.Fatalf("last severity change wrong: %+v", last)
	}

	// 再降回 sev1：此时没有 held 了；后续 step1 在 sev1 下再次被 held。
	if _, err := s.UpdateSeverity(UpdateSeverityRequest{
		RequestID: "sev-req-2", IncidentID: incID, Severity: "sev1",
		Reason: "回落", UpdatedBy: "monitor",
	}, t0.Add(11*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessDue(t0.Add(12 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	items := s.ListOutbox(incID)
	if countByStatus(items)[OutboxHeld] != 1 {
		t.Fatalf("step1 should be held again at sev1: %+v", countByStatus(items))
	}

	// 窗口到期解除：按最新级别 sev1（仍在原范围内但规则已结束）补发，
	// step0 曾因逃出范围补发、step1 held 解除补发；同时 step2 定时器在
	// 3h 扫描时到期且规则已解除，直接形成新的 pending——共 3 条，无重复。
	if _, err := s.ProcessDue(t0.Add(3 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	items = s.ListOutbox(incID)
	counts := countByStatus(items)
	if counts[OutboxPending] != 3 || counts[OutboxHeld] != 0 {
		t.Fatalf("after release at latest severity: %+v", counts)
	}
	seen := map[string]bool{}
	for _, it := range items {
		key := fmt.Sprintf("%d/%s", it.StepIndex, it.Target)
		if seen[key] {
			t.Fatalf("duplicate effective notification for %s", key)
		}
		seen[key] = true
	}
}

// TestSeverityChangeIdempotentAndRejectsResolved 级别变更请求号幂等，
// 已解决事件拒绝再改级别。
func TestSeverityChangeIdempotentAndRejectsResolved(t *testing.T) {
	s, incID, t0 := newSuppressionStore(t)
	req := UpdateSeverityRequest{
		RequestID: "sev-req", IncidentID: incID, Severity: "sev9", Reason: "x",
	}
	if _, err := s.UpdateSeverity(req, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateSeverity(req, t0.Add(2*time.Minute)); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	diff := req
	diff.Severity = "sev8"
	if _, err := s.UpdateSeverity(diff, t0.Add(3*time.Minute)); !errors.Is(err, ErrConflict) {
		t.Fatalf("same request id different content: want ErrConflict, got %v", err)
	}
	if _, err := s.Resolve(ResolveRequest{RequestID: "r1", IncidentID: incID, ResolvedBy: "a"}, t0.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateSeverity(UpdateSeverityRequest{
		RequestID: "sev-req-2", IncidentID: incID, Severity: "sev0",
	}, t0.Add(5*time.Minute)); !errors.Is(err, ErrIncidentResolved) {
		t.Fatalf("severity change on resolved: want ErrIncidentResolved, got %v", err)
	}
}

// TestSuppressionOnResolveRelease on_resolve 规则在事件解决时解除且不补发；
// 未解决则在 ValidUntil 兜底解除。
func TestSuppressionOnResolveRelease(t *testing.T) {
	s, incID, t0 := newSuppressionStore(t)
	if _, err := s.CreateSuppression(CreateSuppressionRequest{
		RequestID: "sup-req", RuleID: "mw-1",
		Scope:  SuppressionScope{IncidentIDs: []string{incID}},
		Reason: "维护", ValidFrom: t0, ValidUntil: t0.Add(2 * time.Hour),
		ReleaseCondition: ReleaseOnResolve,
	}, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessDue(t0.Add(30 * time.Minute)); err != nil { // step0 held
		t.Fatal(err)
	}
	if _, err := s.Resolve(ResolveRequest{
		RequestID: "res-req", IncidentID: incID, ResolvedBy: "alice",
	}, t0.Add(40*time.Minute)); err != nil {
		t.Fatal(err)
	}
	rule, err := s.GetSuppression("mw-1")
	if err != nil {
		t.Fatal(err)
	}
	if rule.Status != SuppressionReleased || rule.ReleaseCause != "on_resolve" {
		t.Fatalf("rule should release on resolve: %+v", rule)
	}
	if pending := s.PendingOutbox(); len(pending) != 0 {
		t.Fatalf("resolved must not resume notifications: %+v", pending)
	}

	// 另一条 on_resolve 规则始终不解决：ValidUntil 到期兜底解除。
	inc2, err := s.CreateIncident(CreateIncidentRequest{
		RequestID: "inc-req-2", IncidentID: "inc-2", PolicyID: "p", Severity: "sev1",
	}, t0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSuppression(CreateSuppressionRequest{
		RequestID: "sup-req-2", RuleID: "mw-2",
		Scope:  SuppressionScope{IncidentIDs: []string{inc2.ID}},
		Reason: "维护2", ValidFrom: t0, ValidUntil: t0.Add(2 * time.Hour),
		ReleaseCondition: ReleaseOnResolve,
	}, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessDue(t0.Add(30 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessDue(t0.Add(3 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	rule2, _ := s.GetSuppression("mw-2")
	if rule2.Status != SuppressionReleased || rule2.ReleaseCause != "window_end" {
		t.Fatalf("unresolved on_resolve rule should fall back to window_end: %+v", rule2)
	}
	// inc-2 的策略有 2 步，扫描到 3h 时第一次扫描触发 step0 held，
	// 同一次 ProcessDue 的阶段 1 已先解除规则，因此 step0 直接补发为 pending；
	// 下一次扫描再补发/推进 step1，窗口结束后未处理级别都会补回。
	if _, err := s.ProcessDue(t0.Add(4 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	// inc-1 已解决（0 条）；inc-2 策略 3 步，窗口结束后 held 的补回、
	// 到期的步骤正常触发，全部未处理级别都补回（3 条）。
	if pending := s.PendingOutbox(); len(pending) != 3 {
		t.Fatalf("unresolved incident resumes all levels after window: pending=%d", len(pending))
	}
}

// TestSuppressionScopeFiltering 范围过滤：策略/级别/事件 ID 三维“与”。
func TestSuppressionScopeFiltering(t *testing.T) {
	s := NewMemoryStore()
	t0 := time.Unix(1000, 0)
	if _, err := s.CreatePolicy(PolicyInput{
		ID: "p", Name: "p",
		Steps: []Step{{WaitBefore: 0, Targets: []string{"a"}}},
	}, t0); err != nil {
		t.Fatal(err)
	}
	i1, _ := s.CreateIncident(CreateIncidentRequest{
		RequestID: "c1", IncidentID: "i1", PolicyID: "p", Severity: "sev1",
	}, t0)
	i2, _ := s.CreateIncident(CreateIncidentRequest{
		RequestID: "c2", IncidentID: "i2", PolicyID: "p", Severity: "sev2",
	}, t0)
	// 只抑制 sev1。
	if _, err := s.CreateSuppression(CreateSuppressionRequest{
		RequestID: "r1", RuleID: "mw",
		Scope:  SuppressionScope{Severities: []string{"sev1"}},
		Reason: "m", ValidFrom: t0, ValidUntil: t0.Add(time.Hour),
	}, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessDue(t0); err != nil {
		t.Fatal(err)
	}
	if c := countByStatus(s.ListOutbox(i1.ID)); c[OutboxHeld] != 1 {
		t.Fatalf("sev1 incident must be held: %+v", c)
	}
	if c := countByStatus(s.ListOutbox(i2.ID)); c[OutboxPending] != 1 {
		t.Fatalf("sev2 incident must notify normally: %+v", c)
	}
}

// TestGetIncidentView 查询视图展示事件、级别轨迹、每次交接、每级升级与抑制原因。
func TestGetIncidentView(t *testing.T) {
	s, incID, t0 := newSuppressionStore(t)
	// 先让 step0 触发，再开窗口抑制 step1，并做两次交接。
	if _, err := s.ProcessDue(t0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Handoff(HandoffRequest{
		RequestID: "h1", FromGroup: DefaultGroup, ToGroup: "g2",
	}, t0.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSuppression(CreateSuppressionRequest{
		RequestID: "sup-1", RuleID: "mw-1",
		Scope:  SuppressionScope{IncidentIDs: []string{incID}},
		Reason: "维护窗口", ValidFrom: t0.Add(2 * time.Second), ValidUntil: t0.Add(2 * time.Hour),
	}, t0.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessDue(t0.Add(30 * time.Minute)); err != nil { // step1 held
		t.Fatal(err)
	}
	if _, err := s.Handoff(HandoffRequest{
		RequestID: "h2", FromGroup: "g2", ToGroup: "g3",
	}, t0.Add(31*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateSeverity(UpdateSeverityRequest{
		RequestID: "sev-1", IncidentID: incID, Severity: "sev2", Reason: "升级",
	}, t0.Add(32*time.Minute)); err != nil {
		t.Fatal(err)
	}

	view, err := s.GetIncidentView(incID, t0.Add(33*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if view.Incident.OwnerGroup != "g3" {
		t.Fatalf("owner = %q, want g3", view.Incident.OwnerGroup)
	}
	// 每次交接的接手人/时间都保留（共 2 次）。
	if len(view.Incident.Handoffs) != 2 {
		t.Fatalf("handoff trail = %+v", view.Incident.Handoffs)
	}
	if view.Incident.Handoffs[0].ToGroup != "g2" || view.Incident.Handoffs[1].ToGroup != "g3" {
		t.Fatalf("handoff trail order wrong: %+v", view.Incident.Handoffs)
	}
	// 级别变化持续可见。
	if len(view.Incident.SeverityChanges) != 2 ||
		view.Incident.SeverityChanges[1].To != "sev2" {
		t.Fatalf("severity trail = %+v", view.Incident.SeverityChanges)
	}
	// 两级升级可见：step0 已派发，step1 held 且带规则与原因。
	var held *EscalationView
	for i := range view.Escalations {
		if view.Escalations[i].StepIndex == 1 {
			held = &view.Escalations[i]
		}
	}
	if held == nil || held.Status != OutboxHeld || held.SuppressionID != "mw-1" ||
		held.SuppressionReason != "维护窗口" {
		t.Fatalf("held escalation view wrong: %+v", view.Escalations)
	}
	if len(view.ActiveSuppressions) != 1 || view.ActiveSuppressions[0].ID != "mw-1" {
		t.Fatalf("active suppressions = %+v", view.ActiveSuppressions)
	}
	// 窗口结束后的视图不再展示生效规则。
	view2, err := s.GetIncidentView(incID, t0.Add(3*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(view2.ActiveSuppressions) != 0 {
		t.Fatalf("expired rule must not be active in view: %+v", view2.ActiveSuppressions)
	}
}

// TestSuppressionEscalationHandoffReleaseRace 升级触发、交接、级别变更、
// 手工解除并发到达：最终状态一致，每级最多一条有效意图，旧班组不再派发。
func TestSuppressionEscalationHandoffReleaseRace(t *testing.T) {
	s := NewMemoryStore()
	t0 := time.Unix(1000, 0)
	if _, err := s.CreatePolicy(PolicyInput{
		ID: "p", Name: "p",
		Steps: []Step{
			{WaitBefore: 0, Targets: []string{"a"}},
			{WaitBefore: 0, Targets: []string{"b"}},
			{WaitBefore: 0, Targets: []string{"c"}},
		},
	}, t0); err != nil {
		t.Fatal(err)
	}
	const n = 24
	for i := 0; i < n; i++ {
		if _, err := s.CreateIncident(CreateIncidentRequest{
			RequestID: fmt.Sprintf("c-%d", i), IncidentID: fmt.Sprintf("inc-%d", i),
			PolicyID: "p", OwnerGroup: "g1",
		}, t0); err != nil {
			t.Fatal(err)
		}
		if _, err := s.CreateSuppression(CreateSuppressionRequest{
			RequestID: fmt.Sprintf("sup-%d", i), RuleID: fmt.Sprintf("mw-%d", i),
			Scope:  SuppressionScope{IncidentIDs: []string{fmt.Sprintf("inc-%d", i)}},
			Reason: "mw", ValidFrom: t0, ValidUntil: t0.Add(time.Hour),
		}, t0); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	race := func(fn func()) {
		wg.Add(1)
		go func() { defer wg.Done(); fn() }()
	}
	for i := 0; i < n; i++ {
		i := i
		id := fmt.Sprintf("inc-%d", i)
		race(func() { _, _ = s.ProcessDue(t0.Add(time.Minute)) })
		race(func() {
			_, _ = s.Handoff(HandoffRequest{
				RequestID: fmt.Sprintf("h-%d", i), FromGroup: "g1", ToGroup: "g2",
			}, t0.Add(time.Minute))
		})
		race(func() {
			_, _ = s.ReleaseSuppression(ReleaseSuppressionRequest{
				RequestID: fmt.Sprintf("rel-%d", i), RuleID: fmt.Sprintf("mw-%d", i),
				ReleasedBy: "lead",
			}, t0.Add(time.Minute))
		})
		race(func() {
			_, _ = s.Acknowledge(AckRequest{
				RequestID: fmt.Sprintf("ack-%d", i), IncidentID: id, AcknowledgedBy: "u",
			}, t0.Add(time.Minute))
		})
	}
	wg.Wait()

	// 收敛后不变。
	before := len(s.ListOutbox(""))
	for i := 0; i < 5; i++ {
		if _, err := s.ProcessDue(t0.Add(2 * time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	if after := len(s.ListOutbox("")); after != before {
		t.Fatalf("state changed after convergence: %d -> %d", before, after)
	}

	// 每个 (事件,级别,目标) 至多一条记录；held/pending 的 owner 必须等于事件 owner，
	// 旧班组 g1 绝不能再派发任何东西。
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("inc-%d", i)
		inc, err := s.GetIncident(id)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]int{}
		for _, it := range s.ListOutbox(id) {
			if it.Kind == OutboxKindHandoff {
				continue
			}
			key := fmt.Sprintf("%d/%s", it.StepIndex, it.Target)
			seen[key]++
			if (it.Status == OutboxPending || it.Status == OutboxHeld) && it.OwnerGroup != inc.OwnerGroup {
				t.Fatalf("%s item owner %q != incident owner %q", id, it.OwnerGroup, inc.OwnerGroup)
			}
		}
		for key, c := range seen {
			if c > 1 {
				t.Fatalf("%s intent %s x%d", id, key, c)
			}
		}
	}
	// g1 若仍持有 pending，只可能是“未被交接且仍归 g1”的事件；
	// 已交接给 g2 的事件，g1 绝不能再派发。
	owners := map[string]string{}
	for _, inc := range s.ListIncidents() {
		owners[inc.ID] = inc.OwnerGroup
	}
	for _, it := range s.PendingOutboxFor("g1") {
		if owners[it.IncidentID] != "g1" {
			t.Fatalf("g1 dispatches intent of %s owned by %s", it.IncidentID, owners[it.IncidentID])
		}
	}
}

// TestSuppressionRestartResume 抑制规则、held 意图落盘后重启：
// 窗口到期后仍按最新级别补发，幂等表恢复，不重复通知。
func TestSuppressionRestartResume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	t0 := time.Unix(50_000, 0)

	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreatePolicy(PolicyInput{
		ID: "p", Name: "p",
		Steps: []Step{
			{WaitBefore: 0, Targets: []string{"alice"}},
			{WaitBefore: time.Minute, Targets: []string{"bob"}},
		},
	}, t0); err != nil {
		t.Fatal(err)
	}
	inc, err := s.CreateIncident(CreateIncidentRequest{
		RequestID: "c1", IncidentID: "inc-1", PolicyID: "p", Severity: "sev1",
	}, t0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSuppression(CreateSuppressionRequest{
		RequestID: "sup-1", RuleID: "mw-1",
		Scope:  SuppressionScope{IncidentIDs: []string{inc.ID}},
		Reason: "维护", ValidFrom: t0, ValidUntil: t0.Add(2 * time.Hour),
	}, t0); err != nil {
		t.Fatal(err)
	}
	// 两步都在窗口内触发为 held。
	if _, err := s.ProcessDue(t0.Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessDue(t0.Add(2 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	if c := countByStatus(s.ListOutbox(inc.ID)); c[OutboxHeld] != 2 {
		t.Fatalf("before restart held = %+v", c)
	}

	// 重启。
	s2, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	rule, err := s2.GetSuppression("mw-1")
	if err != nil {
		t.Fatalf("suppression not recovered: %v", err)
	}
	if rule.Status != SuppressionActive {
		t.Fatalf("recovered rule status = %s", rule.Status)
	}
	// 窗口内重启后扫描：仍不派发。
	if _, err := s2.ProcessDue(t0.Add(30 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	if pending := s2.PendingOutbox(); len(pending) != 0 {
		t.Fatalf("held leaked across restart: %+v", pending)
	}
	// 窗口到期后补发，每级一条。
	if _, err := s2.ProcessDue(t0.Add(3 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	if c := countByStatus(s2.ListOutbox(inc.ID)); c[OutboxPending] != 2 || c[OutboxHeld] != 0 {
		t.Fatalf("resume after restart: %+v", c)
	}
	// 幂等表恢复：同号异内容冲突；同号重放不重复。
	if _, err := s2.CreateSuppression(CreateSuppressionRequest{
		RequestID: "sup-1", RuleID: "mw-x",
		Reason: "不同内容", ValidFrom: t0, ValidUntil: t0.Add(time.Hour),
	}, t0); !errors.Is(err, ErrConflict) {
		t.Fatalf("idempotency table lost: %v", err)
	}
}

// TestServiceSuppressionDispatch 后台服务在抑制期间不投递 held 意图，
// 解除后自动补发。
func TestServiceSuppressionDispatch(t *testing.T) {
	s := NewMemoryStore()
	t0 := time.Unix(1_700_000_000, 0)
	clock := t0
	var nowMu sync.Mutex
	nowFn := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return clock
	}
	if _, err := s.CreatePolicy(PolicyInput{
		ID: "p", Name: "p",
		Steps: []Step{
			{WaitBefore: 0, Targets: []string{"alice"}},
			{WaitBefore: time.Minute, Targets: []string{"bob"}},
		},
	}, t0); err != nil {
		t.Fatal(err)
	}
	inc, err := s.CreateIncident(CreateIncidentRequest{
		RequestID: "c1", IncidentID: "inc-1", PolicyID: "p",
	}, t0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSuppression(CreateSuppressionRequest{
		RequestID:        "sup-1",
		RuleID:           "mw-1",
		Scope:            SuppressionScope{IncidentIDs: []string{inc.ID}},
		Reason:           "维护",
		ValidFrom:        t0,
		ValidUntil:       t0.Add(time.Hour),
		ReleaseCondition: ReleaseManual,
	}, t0); err != nil {
		t.Fatal(err)
	}

	disp := &recordingDispatcher{}
	svc := NewService(s, disp, Config{
		TickInterval:     5 * time.Millisecond,
		DispatchInterval: 5 * time.Millisecond,
		Now:              nowFn,
	})
	svc.Start()
	defer svc.Stop()

	// 推进注入时钟到两步都到期（仍在抑制窗口内）。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		nowMu.Lock()
		clock = clock.Add(2 * time.Minute)
		nowMu.Unlock()
		time.Sleep(20 * time.Millisecond)
		if c := countByStatus(s.ListOutbox(inc.ID)); c[OutboxHeld] == 2 {
			break
		}
	}
	time.Sleep(100 * time.Millisecond)
	if got := disp.delivered(); len(got) != 0 {
		t.Fatalf("nothing should be dispatched during suppression, got %v", got)
	}
	if c := countByStatus(s.ListOutbox(inc.ID)); c[OutboxHeld] != 2 {
		t.Fatalf("want 2 held, got %+v", c)
	}

	if _, err := s.ReleaseSuppression(ReleaseSuppressionRequest{
		RequestID: "rel-1", RuleID: "mw-1",
	}, nowFn()); err != nil {
		t.Fatal(err)
	}
	dispatchDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(dispatchDeadline) && len(disp.delivered()) < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	got := disp.delivered()
	if len(got) != 2 {
		t.Fatalf("after release dispatched %v, want 2", got)
	}
}

// TestOverlappingSuppressionsFollowEarliest 多条规则重叠覆盖同一事件时，
// held 归属/原因始终跟随最早生效的覆盖规则；该规则解除但另一条仍覆盖时
// 继续 held 并换挂到新规则；全部解除后才补发。
func TestOverlappingSuppressionsFollowEarliest(t *testing.T) {
	s, incID, t0 := newSuppressionStore(t)
	// mw-a：[t0, t0+1h)；mw-b：[t0, t0+2h)，范围相同、原因不同故允许并存。
	if _, err := s.CreateSuppression(CreateSuppressionRequest{
		RequestID: "a", RuleID: "mw-a",
		Scope:  SuppressionScope{IncidentIDs: []string{incID}},
		Reason: "窗口A", ValidFrom: t0, ValidUntil: t0.Add(time.Hour),
	}, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSuppression(CreateSuppressionRequest{
		RequestID: "b", RuleID: "mw-b",
		Scope:  SuppressionScope{IncidentIDs: []string{incID}},
		Reason: "窗口B", ValidFrom: t0, ValidUntil: t0.Add(2 * time.Hour),
	}, t0); err != nil {
		t.Fatal(err)
	}
	// step0 触发：挂到最早生效的 mw-a（ValidFrom 相同则按规则 ID 取 a）。
	if _, err := s.ProcessDue(t0.Add(30 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	items := s.ListOutbox(incID)
	if c := countByStatus(items); c[OutboxHeld] != 1 || items[0].SuppressionID != "mw-a" {
		t.Fatalf("held should attach to mw-a: %+v", items)
	}

	// mw-a 到期：仍被 mw-b 覆盖，继续 held，但归属换成 mw-b。
	if _, err := s.ProcessDue(t0.Add(90 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	items = s.ListOutbox(incID)
	var held *OutboxItem
	for _, it := range items {
		if it.Status == OutboxHeld {
			held = it
		}
	}
	if held == nil || held.SuppressionID != "mw-b" || held.SuppressionReason != "窗口B" {
		t.Fatalf("held should re-attach to mw-b: %+v", held)
	}

	// 手工解除 mw-b：step0/step1 两条 held 立即补发（每级仍各只有一条）。
	if _, err := s.ReleaseSuppression(ReleaseSuppressionRequest{
		RequestID: "rel-b", RuleID: "mw-b",
	}, t0.Add(100*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if c := countByStatus(s.ListOutbox(incID)); c[OutboxPending] != 2 || c[OutboxHeld] != 0 {
		t.Fatalf("after both windows: %+v", c)
	}
}
