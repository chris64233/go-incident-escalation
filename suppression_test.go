package incidentescalation

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// suppressPolicy 是抑制相关测试共用的 3 级策略：立即通知 L1，之后 L2、L3。
func suppressPolicy() PolicyInput {
	return PolicyInput{
		ID:   "p",
		Name: "suppress-policy",
		Steps: []Step{
			{WaitBefore: 0, Targets: []string{"l1"}},
			{WaitBefore: 5 * time.Minute, Targets: []string{"l2"}},
			{WaitBefore: 10 * time.Minute, Targets: []string{"l3"}},
		},
	}
}

func newSuppressStore(t *testing.T, now time.Time) *Store {
	t.Helper()
	s := NewMemoryStore()
	if _, err := s.CreatePolicy(suppressPolicy(), now); err != nil {
		t.Fatalf("seed policy: %v", err)
	}
	return s
}

func windowRule(ruleID, incidentID string, start, end time.Time) SuppressionRequest {
	return SuppressionRequest{
		RequestID: "create-" + ruleID,
		SuppressionInput: SuppressionInput{
			RuleID:   ruleID,
			Scope:    SuppressionScope{IncidentIDs: []string{incidentID}},
			Reason:   "维护窗口：数据库升级",
			StartsAt: start,
			EndsAt:   end,
		},
	}
}

func escalationByIndex(view *IncidentView, idx int) EscalationInfo {
	for _, e := range view.Escalations {
		if e.StepIndex == idx {
			return e
		}
	}
	return EscalationInfo{StepIndex: -1}
}

func TestSuppressionValidationAndDuplicate(t *testing.T) {
	s := newSuppressStore(t, time.Unix(1000, 0))
	t0 := time.Unix(2000, 0)

	missingReason := windowRule("r1", "inc-x", t0, t0.Add(time.Hour))
	missingReason.Reason = ""
	if _, err := s.CreateSuppression(missingReason, t0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing reason: want ErrInvalidArgument, got %v", err)
	}

	badWindow := windowRule("r1", "inc-x", t0, t0)
	if _, err := s.CreateSuppression(badWindow, t0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("end<=start: want ErrInvalidArgument, got %v", err)
	}

	manual := windowRule("r1", "inc-x", t0, t0.Add(time.Hour))
	manual.Condition = "bogus"
	if _, err := s.CreateSuppression(manual, t0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("bad condition: want ErrInvalidArgument, got %v", err)
	}

	missingIncident := windowRule("r1", "ghost", t0, t0.Add(time.Hour))
	if _, err := s.CreateSuppression(missingIncident, t0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown incident in scope: want ErrNotFound, got %v", err)
	}

	inc, err := s.CreateIncident(CreateIncidentRequest{RequestID: "r-inc", PolicyID: "p"}, t0)
	if err != nil {
		t.Fatal(err)
	}

	rule := windowRule("r1", inc.ID, t0, t0.Add(time.Hour))
	created, err := s.CreateSuppression(rule, t0)
	if err != nil {
		t.Fatalf("create rule: %v", err)
	}
	if created.Status != SuppressionActive || created.Condition != ReleaseAtWindowEnd {
		t.Fatalf("new rule wrong: %+v", created)
	}

	// 同 RequestID 重放返回原规则。
	replay, err := s.CreateSuppression(rule, t0.Add(time.Minute))
	if err != nil || replay.ID != "r1" {
		t.Fatalf("idempotent replay: %v %+v", err, replay)
	}

	// 相同内容、仅解除时间不同：必须明确报“已有规则”，不能静默延长/覆盖。
	longerWindow := windowRule("r2", inc.ID, t0, t0.Add(2*time.Hour))
	longerWindow.RequestID = "create-r2"
	if _, err := s.CreateSuppression(longerWindow, t0); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("same scope different end: want ErrAlreadyExists, got %v", err)
	}

	// 原窗口必须原封不动。
	got, _ := s.GetSuppression("r1")
	if !got.EndsAt.Equal(t0.Add(time.Hour)) {
		t.Fatalf("original window was modified: ends=%s", got.EndsAt)
	}

	// 解除条件不同属于不同规则内容，允许创建。
	manualRule := windowRule("r3", inc.ID, t0, t0.Add(3*time.Hour))
	manualRule.RequestID = "create-r3"
	manualRule.Condition = ReleaseManual
	if _, err := s.CreateSuppression(manualRule, t0); err != nil {
		t.Fatalf("manual rule is distinct content: %v", err)
	}

	// 已解除的旧规则不再阻止同内容新规则。
	if _, err := s.ReleaseSuppression(ReleaseSuppressionRequest{
		RequestID: "rel-r1", RuleID: "r1", ReleasedBy: "oncall",
	}, t0.Add(90*time.Minute)); err != nil {
		t.Fatal(err)
	}
	again := windowRule("", inc.ID, t0.Add(2*time.Hour), t0.Add(3*time.Hour))
	again.RequestID = "create-r4"
	if _, err := s.CreateSuppression(again, t0); err != nil {
		t.Fatalf("rule after previous released: %v", err)
	}
}

func TestSuppressionHoldsAndReleaseCatchesUp(t *testing.T) {
	t0 := time.Unix(10_000, 0)
	s := newSuppressStore(t, t0)
	inc, _ := s.CreateIncident(CreateIncidentRequest{RequestID: "r-inc", PolicyID: "p"}, t0)

	// 维护窗口覆盖整个升级链。
	if _, err := s.CreateSuppression(windowRule("sup", inc.ID, t0, t0.Add(time.Hour)), t0); err != nil {
		t.Fatal(err)
	}

	// 即使时间跨过全部步骤：每级照常推进，但不产生任何通知。
	// 每次扫描间隔需大于步骤等待（step1 +5m、step2 +10m）。
	if n, _ := s.ProcessDue(t0.Add(10 * time.Minute)); n != 1 {
		t.Fatalf("suppressed scan should still advance one step")
	}
	if n, _ := s.ProcessDue(t0.Add(20 * time.Minute)); n != 1 {
		t.Fatalf("second scan should advance one step")
	}
	if n, _ := s.ProcessDue(t0.Add(40 * time.Minute)); n != 1 {
		t.Fatalf("third scan should advance one step")
	}
	if items := s.ListOutbox(inc.ID); len(items) != 0 {
		t.Fatalf("suppressed window produced %d notifications", len(items))
	}
	got, _ := s.GetIncident(inc.ID)
	if got.NextStepIndex != 3 || len(got.SuppressedSteps) != 3 {
		t.Fatalf("progress during suppression wrong: %+v", got)
	}

	view, err := s.IncidentViewAt(inc.ID, t0.Add(40*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for idx := 0; idx < 3; idx++ {
		info := escalationByIndex(view, idx)
		if info.State != EscalationSuppressed || info.Reason == "" {
			t.Fatalf("step %d state=%s reason=%q, want suppressed with reason", idx, info.State, info.Reason)
		}
	}
	if len(view.Suppressions) != 1 || !view.Suppressions[0].Active {
		t.Fatalf("view suppression wrong: %+v", view.Suppressions)
	}

	// 手工解除：按最新状态补发三级通知（每级一次），并标记 caught_up。
	if _, err := s.ReleaseSuppression(ReleaseSuppressionRequest{
		RequestID: "rel", RuleID: "sup", ReleasedBy: "oncall",
	}, t0.Add(45*time.Minute)); err != nil {
		t.Fatal(err)
	}
	items := s.ListOutbox(inc.ID)
	if len(items) != 3 {
		t.Fatalf("catch-up outbox = %d items, want 3", len(items))
	}
	for i, item := range items {
		if item.StepIndex != i || !item.CatchUp {
			t.Fatalf("catch-up item %d wrong: %+v", i, item)
		}
	}
	view, _ = s.IncidentViewAt(inc.ID, t0.Add(46*time.Minute))
	for idx := 0; idx < 3; idx++ {
		if info := escalationByIndex(view, idx); info.State != EscalationCaughtUp {
			t.Fatalf("step %d state=%s, want caught_up", idx, info.State)
		}
	}
	if view.Suppressions[0].Active || view.Suppressions[0].Status != SuppressionReleased {
		t.Fatalf("rule should be released: %+v", view.Suppressions[0])
	}

	// 重复解除（新请求号）报错；同请求号重放安全。
	if _, err := s.ReleaseSuppression(ReleaseSuppressionRequest{
		RequestID: "rel-again", RuleID: "sup", ReleasedBy: "oncall",
	}, t0.Add(47*time.Minute)); !errors.Is(err, ErrConflict) {
		t.Fatalf("double release: want ErrConflict, got %v", err)
	}
	if _, err := s.ReleaseSuppression(ReleaseSuppressionRequest{
		RequestID: "rel", RuleID: "sup", ReleasedBy: "oncall",
	}, t0.Add(48*time.Minute)); err != nil {
		t.Fatalf("idempotent release replay: %v", err)
	}

	// 之后任何扫描都不再产生新通知：每级最多一次。
	if _, err := s.ProcessDue(t0.Add(10 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	if len(s.ListOutbox(inc.ID)) != 3 {
		t.Fatalf("post-release scan duplicated notifications")
	}
}

func TestSuppressionReleaseSkipsResolvedAndAcknowledged(t *testing.T) {
	t0 := time.Unix(10_000, 0)
	s := newSuppressStore(t, t0)
	resInc, _ := s.CreateIncident(CreateIncidentRequest{
		RequestID: "inc-res", PolicyID: "p", IncidentID: "inc-res",
	}, t0)
	ackInc, _ := s.CreateIncident(CreateIncidentRequest{
		RequestID: "inc-ack", PolicyID: "p", IncidentID: "inc-ack",
	}, t0)

	rule := SuppressionRequest{
		RequestID: "create-global",
		SuppressionInput: SuppressionInput{
			RuleID: "global", Scope: SuppressionScope{},
			Reason: "全网维护", StartsAt: t0, EndsAt: t0.Add(time.Hour),
		},
	}
	if _, err := s.CreateSuppression(rule, t0); err != nil {
		t.Fatal(err)
	}

	// 两个事件都在窗口内触发第 0 级（被抑制）。
	if _, err := s.ProcessDue(t0.Add(30 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Resolve(ResolveRequest{
		RequestID: "resolve", IncidentID: resInc.ID, ResolvedBy: "alice",
	}, t0.Add(31*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acknowledge(AckRequest{
		RequestID: "ack", IncidentID: ackInc.ID, AcknowledgedBy: "bob",
	}, t0.Add(32*time.Minute)); err != nil {
		t.Fatal(err)
	}

	if _, err := s.ReleaseSuppression(ReleaseSuppressionRequest{
		RequestID: "rel", RuleID: "global", ReleasedBy: "oncall",
	}, t0.Add(40*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if items := s.ListOutbox(""); len(items) != 0 {
		t.Fatalf("resolved/acknowledged incidents must not get catch-up, got %d items", len(items))
	}

	// 抑制记录仍保留在视图中，但不会变成 caught_up。
	for _, id := range []string{resInc.ID, ackInc.ID} {
		view, _ := s.IncidentViewAt(id, t0.Add(41*time.Minute))
		if info := escalationByIndex(view, 0); info.State != EscalationSuppressed {
			t.Fatalf("%s step0 state=%s, want suppressed (retained, not notified)", id, info.State)
		}
	}
}

func TestSuppressionAutoReleasesAtWindowEnd(t *testing.T) {
	t0 := time.Unix(10_000, 0)
	s := newSuppressStore(t, t0)
	inc, _ := s.CreateIncident(CreateIncidentRequest{RequestID: "r-inc", PolicyID: "p"}, t0)
	end := t0.Add(time.Hour)
	if _, err := s.CreateSuppression(windowRule("sup", inc.ID, t0, end), t0); err != nil {
		t.Fatal(err)
	}

	// 窗口内连续推进三级，零通知。
	for _, ts := range []time.Duration{10 * time.Minute, 20 * time.Minute, 30 * time.Minute} {
		if _, err := s.ProcessDue(t0.Add(ts)); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.ListOutbox(inc.ID)) != 0 {
		t.Fatalf("notifications leaked inside window")
	}

	// 窗口结束后的第一次扫描：自动解除并补发全部三级。
	n, err := s.ProcessDue(end.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("all steps already advanced, catch-up scan should advance 0, got %d", n)
	}
	if items := s.ListOutbox(inc.ID); len(items) != 3 {
		t.Fatalf("auto-release catch-up = %d items, want 3", len(items))
	}
	rule, _ := s.GetSuppression("sup")
	if rule.Status != SuppressionReleased || rule.ReleasedBy != "system" ||
		!rule.ReleasedAt.Equal(end.Add(time.Second)) {
		t.Fatalf("rule not auto-released: %+v", rule)
	}

	// manual 解除条件的规则不会在窗口结束时自动解除。
	manualInc, _ := s.CreateIncident(CreateIncidentRequest{
		RequestID: "r-inc-2", PolicyID: "p", IncidentID: "inc-manual",
	}, end.Add(2*time.Hour))
	manualRule := windowRule("manual", manualInc.ID,
		manualInc.StartedAt, manualInc.StartedAt.Add(time.Hour))
	manualRule.Condition = ReleaseManual
	if _, err := s.CreateSuppression(manualRule, manualInc.StartedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessDue(manualInc.StartedAt.Add(2 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetSuppression("manual")
	if got.Status != SuppressionActive {
		t.Fatalf("manual rule must survive window end, got %s", got.Status)
	}
	if items := s.ListOutbox(manualInc.ID); len(items) != 0 {
		t.Fatalf("manual rule past window still suppresses, leaked %d items", len(items))
	}
}

func TestOverlappingRulesOldCannotBypassCurrent(t *testing.T) {
	t0 := time.Unix(10_000, 0)
	s := newSuppressStore(t, t0)
	inc, _ := s.CreateIncident(CreateIncidentRequest{RequestID: "r-inc", PolicyID: "p"}, t0)

	// 窄规则覆盖前半段，全局规则覆盖后半段，二者叠加。
	narrow := windowRule("narrow", inc.ID, t0, t0.Add(20*time.Minute))
	narrow.Reason = "窄窗口"
	narrow.Condition = ReleaseManual // 窄规则只能手工解除，避免在 +20m 被自动解除
	if _, err := s.CreateSuppression(narrow, t0); err != nil {
		t.Fatal(err)
	}
	wide := SuppressionRequest{
		RequestID: "create-wide",
		SuppressionInput: SuppressionInput{
			RuleID:   "wide",
			Scope:    SuppressionScope{},
			Reason:   "全局窗口",
			StartsAt: t0.Add(10 * time.Minute),
			EndsAt:   t0.Add(time.Hour),
		},
	}
	if _, err := s.CreateSuppression(wide, t0); err != nil {
		t.Fatal(err)
	}

	// step0 在窄窗口内触发（抑制）。
	if _, err := s.ProcessDue(t0.Add(5 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	// step1 触发时两条规则都覆盖：窄规则手工解除不能绕过仍有效的全局规则。
	if n, err := s.ProcessDue(t0.Add(20 * time.Minute)); err != nil || n != 1 {
		t.Fatalf("step1 should advance under both rules: n=%d err=%v", n, err)
	}
	if items := s.ListOutbox(inc.ID); len(items) != 0 {
		t.Fatalf("release of old rule must not bypass current rule: %d items", len(items))
	}

	// 手工解除窄规则也不能补发（全局规则仍有效）——旧班次/旧规则不能绕过当前规则。
	if _, err := s.ReleaseSuppression(ReleaseSuppressionRequest{
		RequestID: "rel-narrow", RuleID: "narrow", ReleasedBy: "oncall",
	}, t0.Add(25*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if items := s.ListOutbox(inc.ID); len(items) != 0 {
		t.Fatalf("manual release with active rule leaked %d items", len(items))
	}

	// step2 在叠加期间触发。
	if _, err := s.ProcessDue(t0.Add(30 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	// 全局窗口结束：三级一起补发。
	if _, err := s.ProcessDue(t0.Add(time.Hour).Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	items := s.ListOutbox(inc.ID)
	if len(items) != 3 {
		t.Fatalf("final catch-up = %d items, want 3", len(items))
	}
	counts := map[string]int{}
	for _, it := range items {
		counts[fmt.Sprintf("%d/%s", it.StepIndex, it.Target)]++
	}
	for key, c := range counts {
		if c != 1 {
			t.Fatalf("intent %s emitted %d times", key, c)
		}
	}
}

func TestPartialWindowNotifiesNormallyBeforeAndAfter(t *testing.T) {
	t0 := time.Unix(10_000, 0)
	s := newSuppressStore(t, t0)
	inc, _ := s.CreateIncident(CreateIncidentRequest{RequestID: "r-inc", PolicyID: "p"}, t0)

	// step0 在窗口前正常通知。
	if _, err := s.ProcessDue(t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	// 窗口只盖住 step1 的触发时刻。
	if _, err := s.CreateSuppression(windowRule(
		"sup", inc.ID, t0.Add(2*time.Minute), t0.Add(8*time.Minute)), t0.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if n, err := s.ProcessDue(t0.Add(6 * time.Minute)); err != nil || n != 1 {
		t.Fatalf("step1 during window: n=%d err=%v", n, err)
	}
	if items := s.ListOutbox(inc.ID); len(items) != 1 {
		t.Fatalf("only pre-window step0 should exist, got %d", len(items))
	}

	// 窗口结束自动解除：只补发 step1 这一级；step2 尚未触发。
	if _, err := s.ProcessDue(t0.Add(9 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	if items := s.ListOutbox(inc.ID); len(items) != 2 {
		t.Fatalf("catch-up should add only step1, got %d items", len(items))
	}
	// step2 定时器在 step1 实际触发时已登记（基于实际触发时间 + 等待），补发只补通知不改定时器。
	tmr, ok := s.Timer(inc.ID)
	if !ok || tmr.StepIndex != 2 || !tmr.FireAt.Equal(t0.Add(16*time.Minute)) {
		t.Fatalf("step2 timer not rearmed after catch-up: %+v ok=%v", tmr, ok)
	}

	// step2 在窗口后按正常升级链触发（非补发消息）。
	if _, err := s.ProcessDue(t0.Add(20 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	items := s.ListOutbox(inc.ID)
	if len(items) != 3 {
		t.Fatalf("step2 normal notify missing: %d items", len(items))
	}
	if items[2].CatchUp {
		t.Fatalf("post-window step2 must be a normal notification, got catch-up")
	}
	if !items[1].CatchUp {
		t.Fatalf("suppressed step1 release must be marked catch-up")
	}
}

func TestSeverityChangeVisibleAndReevaluatedAtRelease(t *testing.T) {
	t0 := time.Unix(10_000, 0)
	s := newSuppressStore(t, t0)
	inc, _ := s.CreateIncident(CreateIncidentRequest{
		RequestID: "r-inc", PolicyID: "p", Severity: "warning",
	}, t0)

	// 只抑制 critical：创建时事件为 warning，规则不覆盖它，升级照常通知。
	criticalOnly := SuppressionRequest{
		RequestID: "create-crit",
		SuppressionInput: SuppressionInput{
			RuleID:   "crit",
			Scope:    SuppressionScope{Severities: []string{"critical"}},
			Reason:   "critical 维护窗口",
			StartsAt: t0,
			EndsAt:   t0.Add(time.Hour),
		},
	}
	if _, err := s.CreateSuppression(criticalOnly, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessDue(t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if items := s.ListOutbox(inc.ID); len(items) != 1 {
		t.Fatalf("warning incident not in scope, want step0 notified, got %d", len(items))
	}

	// 抑制期间升级为 critical：变化持续可见，后续级别立即开始被抑制。
	if _, err := s.ChangeSeverity(ChangeSeverityRequest{
		RequestID: "sev-1", IncidentID: inc.ID, Severity: "critical",
		Reason: "影响扩大", ChangedBy: "alice",
	}, t0.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetIncident(inc.ID)
	if got.Severity != "critical" || len(got.SeverityHistory) != 1 {
		t.Fatalf("severity not changed: %+v", got)
	}
	if n, _ := s.ProcessDue(t0.Add(6 * time.Minute)); n != 1 {
		t.Fatalf("step1 should advance")
	}
	if items := s.ListOutbox(inc.ID); len(items) != 1 {
		t.Fatalf("after escalation to critical step1 must be suppressed, got %d", len(items))
	}

	// 解除前降回 warning：按“最新级别”判断，规则已不覆盖，应立即补发 step1。
	if _, err := s.ChangeSeverity(ChangeSeverityRequest{
		RequestID: "sev-2", IncidentID: inc.ID, Severity: "warning",
		Reason: "影响收敛", ChangedBy: "alice",
	}, t0.Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReleaseSuppression(ReleaseSuppressionRequest{
		RequestID: "rel-crit", RuleID: "crit", ReleasedBy: "oncall",
	}, t0.Add(11*time.Minute)); err != nil {
		t.Fatal(err)
	}
	items := s.ListOutbox(inc.ID)
	if len(items) != 2 || !items[1].CatchUp {
		t.Fatalf("catch-up based on latest severity missing: %d items, last=%+v", len(items), lastItem(items))
	}

	// 同号重放；级别没变再改返回冲突；已解决事件不允许改级别。
	replay, err := s.ChangeSeverity(ChangeSeverityRequest{
		RequestID: "sev-2", IncidentID: inc.ID, Severity: "warning",
		Reason: "影响收敛", ChangedBy: "alice",
	}, t0.Add(12*time.Minute))
	if err != nil || replay.Severity != "warning" {
		t.Fatalf("severity replay: %v %+v", err, replay)
	}
	if _, err := s.ChangeSeverity(ChangeSeverityRequest{
		RequestID: "sev-3", IncidentID: inc.ID, Severity: "warning",
	}, t0.Add(12*time.Minute)); !errors.Is(err, ErrConflict) {
		t.Fatalf("same severity: want ErrConflict, got %v", err)
	}

	resInc, _ := s.CreateIncident(CreateIncidentRequest{
		RequestID: "r-inc-2", PolicyID: "p", IncidentID: "inc-res-sev",
	}, t0)
	if _, err := s.Resolve(ResolveRequest{
		RequestID: "res-2", IncidentID: resInc.ID, ResolvedBy: "bob",
	}, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ChangeSeverity(ChangeSeverityRequest{
		RequestID: "sev-4", IncidentID: resInc.ID, Severity: "critical",
	}, t0); !errors.Is(err, ErrIncidentResolved) {
		t.Fatalf("severity change on resolved: want ErrIncidentResolved, got %v", err)
	}
}

func lastItem(items []*OutboxItem) *OutboxItem {
	if len(items) == 0 {
		return nil
	}
	return items[len(items)-1]
}

func TestHandoverKeepsEveryTransferAndTagsIntents(t *testing.T) {
	t0 := time.Unix(10_000, 0)
	s := newSuppressStore(t, t0)
	inc, _ := s.CreateIncident(CreateIncidentRequest{RequestID: "r-inc", PolicyID: "p"}, t0)

	// step0 在第一次交接前触发：无接手人标签。
	if _, err := s.ProcessDue(t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	// 第一次交接：alice -> bob。
	if _, err := s.Handover(HandoverRequest{
		RequestID: "h1", IncidentID: inc.ID, From: "alice", To: "bob",
	}, t0.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	// 第二次交接：bob -> carol。每次交接都必须保留。
	if _, err := s.Handover(HandoverRequest{
		RequestID: "h2", IncidentID: inc.ID, From: "bob", To: "carol", Note: "bob 下班",
	}, t0.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetIncident(inc.ID)
	if len(got.Handovers) != 2 || got.CurrentHandler() != "carol" {
		t.Fatalf("handovers wrong: %+v handler=%q", got.Handovers, got.CurrentHandler())
	}

	// step1 在 carol 当班时触发：意图带上当前接手人。
	if _, err := s.ProcessDue(t0.Add(7 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	items := s.ListOutbox(inc.ID)
	if len(items) != 2 || items[0].Handler != "" || items[1].Handler != "carol" {
		t.Fatalf("handler tags wrong: %+v %+v", items[0], items[1])
	}

	// 同号重放返回同一事件且不新增交接记录。
	replay, err := s.Handover(HandoverRequest{
		RequestID: "h2", IncidentID: inc.ID, From: "bob", To: "carol", Note: "bob 下班",
	}, t0.Add(4*time.Minute))
	if err != nil || len(replay.Handovers) != 2 {
		t.Fatalf("handover replay: %v handovers=%d", err, len(replay.Handovers))
	}
	// 接手人为空非法。
	if _, err := s.Handover(HandoverRequest{
		RequestID: "h3", IncidentID: inc.ID, From: "carol", To: "  ",
	}, t0.Add(5*time.Minute)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty to: want ErrInvalidArgument, got %v", err)
	}

	// 已解决事件不能再交接。
	if _, err := s.Resolve(ResolveRequest{
		RequestID: "res", IncidentID: inc.ID, ResolvedBy: "carol",
	}, t0.Add(8*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Handover(HandoverRequest{
		RequestID: "h4", IncidentID: inc.ID, From: "carol", To: "dave",
	}, t0.Add(9*time.Minute)); !errors.Is(err, ErrIncidentResolved) {
		t.Fatalf("handover resolved: want ErrIncidentResolved, got %v", err)
	}

	view, _ := s.IncidentViewAt(inc.ID, t0.Add(9*time.Minute))
	if len(view.Handovers) != 2 {
		t.Fatalf("view handovers = %d, want 2", len(view.Handovers))
	}
	if view.Handovers[0].To != "bob" || view.Handovers[1].To != "carol" {
		t.Fatalf("handover chain order wrong: %+v", view.Handovers)
	}
}

func TestIncidentViewCombinesEscalationHandoverAndSuppression(t *testing.T) {
	t0 := time.Unix(10_000, 0)
	s := newSuppressStore(t, t0)
	inc, _ := s.CreateIncident(CreateIncidentRequest{
		RequestID: "r-inc", PolicyID: "p", Severity: "critical", Detail: "主库故障",
	}, t0)

	// step0 正常通知。
	if _, err := s.ProcessDue(t0); err != nil {
		t.Fatal(err)
	}
	// 交接。
	if _, err := s.Handover(HandoverRequest{
		RequestID: "h1", IncidentID: inc.ID, From: "alice", To: "bob",
	}, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	// 创建窗口并让 step1 在窗口内触发（抑制）。
	if _, err := s.CreateSuppression(windowRule("sup", inc.ID,
		t0.Add(2*time.Minute), t0.Add(time.Hour)), t0.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	// step1 定时器在 step0 触发时刻登记为 t0+5m，落在窗口内。
	if _, err := s.ProcessDue(t0.Add(6 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	// 窗口内级别变化。
	if _, err := s.ChangeSeverity(ChangeSeverityRequest{
		RequestID: "sev-1", IncidentID: inc.ID, Severity: "blocker", ChangedBy: "bob",
	}, t0.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}

	view, err := s.IncidentViewAt(inc.ID, t0.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if view.Incident.Severity != "blocker" {
		t.Fatalf("latest severity not in view: %s", view.Incident.Severity)
	}
	if len(view.Escalations) != 3 {
		t.Fatalf("escalations = %d, want 3", len(view.Escalations))
	}
	if escalationByIndex(view, 0).State != EscalationNotified {
		t.Fatalf("step0 should be notified")
	}
	s1 := escalationByIndex(view, 1)
	if s1.State != EscalationSuppressed || s1.RuleID != "sup" {
		t.Fatalf("step1 should be suppressed by sup: %+v", s1)
	}
	if escalationByIndex(view, 2).State != EscalationPending {
		t.Fatalf("step2 should be pending")
	}
	if len(view.Handovers) != 1 || view.Handovers[0].To != "bob" {
		t.Fatalf("handovers wrong: %+v", view.Handovers)
	}
	if len(view.SeverityHistory) != 1 {
		t.Fatalf("severity history wrong: %+v", view.SeverityHistory)
	}
	if len(view.Suppressions) != 1 || !view.Suppressions[0].Active ||
		view.Suppressions[0].Reason == "" {
		t.Fatalf("suppression reason/state wrong: %+v", view.Suppressions)
	}

	// 窗口外的时间点查询：规则仍在视图中，但 Active 为 false。
	pastView, _ := s.IncidentViewAt(inc.ID, t0.Add(2*time.Hour))
	if pastView.Suppressions[0].Active {
		t.Fatalf("rule past its window must show inactive")
	}

	// 全量视图同样可用。
	if all := s.IncidentViewsAt(t0.Add(5 * time.Minute)); len(all) != 1 {
		t.Fatalf("IncidentViewsAt = %d, want 1", len(all))
	}
}

func TestCatchUpIntentsCarryCurrentHandler(t *testing.T) {
	t0 := time.Unix(10_000, 0)
	s := newSuppressStore(t, t0)
	inc, _ := s.CreateIncident(CreateIncidentRequest{RequestID: "r-inc", PolicyID: "p"}, t0)
	if _, err := s.CreateSuppression(windowRule("sup", inc.ID, t0, t0.Add(time.Hour)), t0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessDue(t0.Add(10 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	// 抑制期间交接：补发的通知必须属于新班次（carol），而不是触发时的旧班次。
	if _, err := s.Handover(HandoverRequest{
		RequestID: "h1", IncidentID: inc.ID, From: "alice", To: "carol",
	}, t0.Add(20*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReleaseSuppression(ReleaseSuppressionRequest{
		RequestID: "rel", RuleID: "sup", ReleasedBy: "carol",
	}, t0.Add(30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	items := s.ListOutbox(inc.ID)
	if len(items) != 1 || items[0].Handler != "carol" || !items[0].CatchUp {
		t.Fatalf("catch-up must target current shift carol: %+v", items)
	}
}

func TestConcurrentEscalationHandoverReleaseConverges(t *testing.T) {
	// 零等待 4 级策略：每个定时器一扫描就到期，最大化触发与交接/解除的竞争。
	s := NewMemoryStore()
	now := time.Unix(1000, 0)
	if _, err := s.CreatePolicy(PolicyInput{
		ID: "p", Name: "p",
		Steps: []Step{
			{Targets: []string{"a"}},
			{Targets: []string{"b"}},
			{Targets: []string{"c"}},
			{Targets: []string{"d"}},
		},
	}, now); err != nil {
		t.Fatal(err)
	}
	inc, err := s.CreateIncident(CreateIncidentRequest{RequestID: "r1", PolicyID: "p"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSuppression(SuppressionRequest{
		RequestID: "create-sup",
		SuppressionInput: SuppressionInput{
			RuleID: "sup",
			Scope:  SuppressionScope{IncidentIDs: []string{inc.ID}},
			Reason: "维护窗口",
			// 窗口在并发时间点之后不久结束：部分扫描可能触发自动解除。
			StartsAt: now,
			EndsAt:   now.Add(time.Hour),
		},
	}, now); err != nil {
		t.Fatal(err)
	}

	const workers = 16
	var wg sync.WaitGroup
	raceNow := now.Add(time.Second)
	for w := 0; w < workers; w++ {
		wg.Add(3)
		w := w
		go func() {
			defer wg.Done()
			_, _ = s.ProcessDue(raceNow.Add(time.Duration(w) * time.Microsecond))
		}()
		go func() {
			defer wg.Done()
			_, _ = s.Handover(HandoverRequest{
				RequestID:  fmt.Sprintf("handover-%d", w),
				IncidentID: inc.ID,
				From:       fmt.Sprintf("shift-%d", w),
				To:         fmt.Sprintf("shift-%d", w+1),
			}, raceNow)
		}()
		go func() {
			defer wg.Done()
			_, _ = s.ReleaseSuppression(ReleaseSuppressionRequest{
				RequestID:  fmt.Sprintf("release-%d", w),
				RuleID:     "sup",
				ReleasedBy: fmt.Sprintf("oncall-%d", w),
			}, raceNow)
		}()
	}
	wg.Wait()

	// 收敛后把剩余级别扫完：无论谁赢，最终每个 (级别,目标) 最多一条意图。
	for i := 0; i < 10; i++ {
		if _, err := s.ProcessDue(raceNow.Add(time.Hour * 2)); err != nil {
			t.Fatal(err)
		}
	}

	counts := map[string]int{}
	for _, it := range s.ListOutbox(inc.ID) {
		counts[fmt.Sprintf("%d/%s", it.StepIndex, it.Target)]++
	}
	for key, c := range counts {
		if c > 1 {
			t.Fatalf("intent %s emitted %d times under concurrency", key, c)
		}
	}

	final, _ := s.GetIncident(inc.ID)
	fired := map[int]bool{}
	for key := range counts {
		var idx int
		fmt.Sscanf(key, "%d/", &idx)
		fired[idx] = true
	}
	// 已通知的级别必须是从 0 开始的连续前缀，且不超过实际进度。
	for i := 0; i < final.NextStepIndex; i++ {
		if !fired[i] {
			// 允许被抑制但未补发的级别（事件仍 firing 且规则恰好在最后一次扫描时重新生效不会发生，
			// 因为规则只能解除不能复活）；规则已解除，所有抑制级别最终都应补发。
			t.Fatalf("step %d missing while progress reached %d", i, final.NextStepIndex)
		}
	}

	// 多次解除只有一次成功：规则必然是 released，且只有一条解除历史。
	rule, _ := s.GetSuppression("sup")
	if rule.Status != SuppressionReleased {
		t.Fatalf("rule must be released after concurrent releases")
	}
	history, _ := s.History(inc.ID)
	releases := 0
	for _, h := range history {
		if h.Kind == "suppression_released" {
			releases++
		}
	}
	if releases != 1 {
		t.Fatalf("suppression_released history = %d, want exactly 1", releases)
	}

	// 收敛后世界不再变化。
	before := len(s.ListOutbox(inc.ID))
	for i := 0; i < 5; i++ {
		if _, err := s.ProcessDue(raceNow.Add(time.Hour * 10)); err != nil {
			t.Fatal(err)
		}
	}
	if after := len(s.ListOutbox(inc.ID)); after != before {
		t.Fatalf("state kept changing after convergence: %d -> %d", before, after)
	}
}

func TestSuppressionStateRestartsFromDisk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	t0 := time.Unix(50_000, 0)

	s, err := OpenStore(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.CreatePolicy(suppressPolicy(), t0); err != nil {
		t.Fatal(err)
	}
	inc, err := s.CreateIncident(CreateIncidentRequest{
		RequestID: "r-inc", PolicyID: "p", Severity: "critical",
	}, t0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSuppression(windowRule("sup", inc.ID, t0, t0.Add(time.Hour)), t0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Handover(HandoverRequest{
		RequestID: "h1", IncidentID: inc.ID, From: "alice", To: "bob",
	}, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	// step0 在窗口内触发：抑制记录落盘。
	if _, err := s.ProcessDue(t0.Add(10 * time.Minute)); err != nil {
		t.Fatal(err)
	}

	// 模拟重启：重新打开同一文件。
	s2, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	rule, err := s2.GetSuppression("sup")
	if err != nil || rule.Status != SuppressionActive {
		t.Fatalf("suppression not recovered: %v %+v", err, rule)
	}
	restored, err := s2.GetIncident(inc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if restored.NextStepIndex != 1 || len(restored.SuppressedSteps) != 1 {
		t.Fatalf("suppressed progress not recovered: %+v", restored)
	}
	if len(restored.Handovers) != 1 || restored.CurrentHandler() != "bob" {
		t.Fatalf("handover not recovered: %+v", restored.Handovers)
	}

	// 重启后窗口结束自动解除，补发 step0，且意图属于当前班次 bob。
	if _, err := s2.ProcessDue(t0.Add(time.Hour).Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	items := s2.ListOutbox(inc.ID)
	if len(items) != 2 {
		t.Fatalf("post-restart scan should catch up step0 and advance step1, got %d", len(items))
	}
	if !items[0].CatchUp || items[0].Handler != "bob" {
		t.Fatalf("post-restart catch-up item wrong: %+v", items[0])
	}
	// step1 在窗口结束后正常触发，不是补发。
	if items[1].CatchUp || items[1].StepIndex != 1 {
		t.Fatalf("post-window step1 should be a normal notify: %+v", items[1])
	}

	// 幂等表恢复：同号异内容仍冲突。
	if _, err := s2.Handover(HandoverRequest{
		RequestID:  "h1",
		IncidentID: inc.ID,
		From:       "alice",
		To:         "someone-else",
	}, t0.Add(2*time.Hour)); !errors.Is(err, ErrConflict) {
		t.Fatalf("handover idempotency lost across restart: %v", err)
	}
}
