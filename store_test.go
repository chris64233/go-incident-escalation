package incidentescalation

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

func testPolicy(id string) PolicyInput {
	return PolicyInput{
		ID:   id,
		Name: "policy-" + id,
		Steps: []Step{
			{WaitBefore: time.Minute, Targets: []string{"alice", "bob"}},
			{WaitBefore: 2 * time.Minute, Targets: []string{"carol"}},
			{WaitBefore: 3 * time.Minute, Targets: []string{"dave", "erin"}},
		},
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s := NewMemoryStore()
	if _, err := s.CreatePolicy(testPolicy("p1"), time.Unix(1000, 0)); err != nil {
		t.Fatalf("seed policy: %v", err)
	}
	return s
}

func TestPolicyCRUDAndValidation(t *testing.T) {
	s := NewMemoryStore()
	now := time.Unix(1000, 0)

	if _, err := s.CreatePolicy(PolicyInput{ID: "x", Name: "n", Steps: nil}, now); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty steps: want ErrInvalidArgument, got %v", err)
	}
	if _, err := s.CreatePolicy(PolicyInput{ID: "x", Name: "n", Steps: []Step{{WaitBefore: -1, Targets: []string{"a"}}}}, now); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative wait: want ErrInvalidArgument, got %v", err)
	}
	if _, err := s.CreatePolicy(PolicyInput{ID: "x", Name: "n", Steps: []Step{{WaitBefore: 0, Targets: []string{"a", "a"}}}}, now); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("dup target: want ErrInvalidArgument, got %v", err)
	}

	p, err := s.CreatePolicy(testPolicy("p1"), now)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if p.Version != 1 {
		t.Fatalf("new policy version = %d, want 1", p.Version)
	}
	if _, err := s.CreatePolicy(testPolicy("p1"), now); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("dup create: want ErrAlreadyExists, got %v", err)
	}

	upd := testPolicy("p1")
	upd.Steps = upd.Steps[:1]
	p2, err := s.UpdatePolicy(upd, now.Add(time.Second))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if p2.Version != 2 || len(p2.Steps) != 1 {
		t.Fatalf("updated policy = %+v", p2)
	}
	if _, err := s.UpdatePolicy(PolicyInput{ID: "missing", Name: "m", Steps: []Step{{Targets: []string{"a"}}}}, now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update missing: want ErrNotFound, got %v", err)
	}
	if _, err := s.GetPolicy("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get missing: want ErrNotFound, got %v", err)
	}
}

func TestCreateIncidentFreezesPolicy(t *testing.T) {
	s := newTestStore(t)
	now := time.Unix(10_000, 0)

	inc, err := s.CreateIncident(CreateIncidentRequest{
		RequestID: "req-1", PolicyID: "p1", Severity: "sev1", Detail: "db down",
	}, now)
	if err != nil {
		t.Fatalf("create incident: %v", err)
	}
	if len(inc.FrozenSteps) != 3 || inc.NextStepIndex != 0 {
		t.Fatalf("incident snapshot wrong: %+v", inc)
	}
	tmr, ok := s.Timer(inc.ID)
	if !ok || tmr.StepIndex != 0 || !tmr.FireAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("step0 timer wrong: %+v ok=%v", tmr, ok)
	}

	// 事后修改策略：存量事件的冻结快照不变。
	upd := testPolicy("p1")
	upd.Steps = []Step{{WaitBefore: time.Second, Targets: []string{"zzz"}}}
	if _, err := s.UpdatePolicy(upd, now); err != nil {
		t.Fatalf("update policy: %v", err)
	}
	got, err := s.GetIncident(inc.ID)
	if err != nil {
		t.Fatalf("get incident: %v", err)
	}
	if len(got.FrozenSteps) != 3 || got.FrozenSteps[0].Targets[0] != "alice" {
		t.Fatalf("frozen snapshot was mutated by policy update: %+v", got.FrozenSteps)
	}
}

func TestCreateIncidentIdempotentAndConflict(t *testing.T) {
	s := newTestStore(t)
	now := time.Unix(10_000, 0)
	req := CreateIncidentRequest{RequestID: "r1", PolicyID: "p1", Severity: "sev1", Detail: "d"}

	first, err := s.CreateIncident(req, now)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	again, err := s.CreateIncident(req, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if again.ID != first.ID {
		t.Fatalf("replay returned different incident: %s vs %s", again.ID, first.ID)
	}
	if len(s.ListIncidents()) != 1 {
		t.Fatalf("replay must not create a second incident")
	}

	// 同号异内容 → 冲突。
	diff := req
	diff.Detail = "different detail"
	if _, err := s.CreateIncident(diff, now); !errors.Is(err, ErrConflict) {
		t.Fatalf("same request id different content: want ErrConflict, got %v", err)
	}

	// 请求号不能跨操作类型复用。
	if _, err := s.Acknowledge(AckRequest{RequestID: "r1", IncidentID: first.ID, AcknowledgedBy: "a"}, now); !errors.Is(err, ErrConflict) {
		t.Fatalf("request id reused across operation: want ErrConflict, got %v", err)
	}

	if _, err := s.CreateIncident(CreateIncidentRequest{PolicyID: "p1"}, now); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing request id: want ErrInvalidArgument, got %v", err)
	}
	if _, err := s.CreateIncident(CreateIncidentRequest{RequestID: "r9", PolicyID: "ghost"}, now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing policy: want ErrNotFound, got %v", err)
	}
}

func TestProcessDueAdvancesOneStepAndDedupes(t *testing.T) {
	s := newTestStore(t)
	t0 := time.Unix(10_000, 0)
	inc, err := s.CreateIncident(CreateIncidentRequest{RequestID: "r1", PolicyID: "p1"}, t0)
	if err != nil {
		t.Fatal(err)
	}

	// 未到期：什么都不做。
	if n, err := s.ProcessDue(t0.Add(30 * time.Second)); err != nil || n != 0 {
		t.Fatalf("before fire: n=%d err=%v", n, err)
	}
	if got := s.ListOutbox(inc.ID); len(got) != 0 {
		t.Fatalf("outbox before due = %d items", len(got))
	}

	// 到期：即使时间已经远超所有步骤，一次调用也只能推进一步。
	farFuture := t0.Add(10 * time.Hour)
	n, err := s.ProcessDue(farFuture)
	if err != nil || n != 1 {
		t.Fatalf("first scan: n=%d err=%v, want 1", n, err)
	}
	got, _ := s.GetIncident(inc.ID)
	if got.NextStepIndex != 1 || !got.FiredAt[0].Equal(farFuture) {
		t.Fatalf("after first scan: %+v", got)
	}
	items := s.ListOutbox(inc.ID)
	if len(items) != 2 { // step0: alice, bob
		t.Fatalf("step0 outbox = %d items, want 2", len(items))
	}
	// 下一步定时器必须按“实际触发时间 + 等待”重新登记。
	tmr, ok := s.Timer(inc.ID)
	if !ok || tmr.StepIndex != 1 || !tmr.FireAt.Equal(farFuture.Add(2*time.Minute)) {
		t.Fatalf("step1 timer wrong: %+v ok=%v", tmr, ok)
	}

	// 重复扫描同一时刻：step1 尚未到期，绝不重复 step0，也不产生重复 outbox。
	n, _ = s.ProcessDue(farFuture)
	if n != 0 {
		t.Fatalf("repeated scan advanced %d steps, want 0", n)
	}
	if len(s.ListOutbox(inc.ID)) != 2 {
		t.Fatalf("repeated scan duplicated outbox items")
	}

	// 继续逐次推进，每次恰好一步。
	for i, cumTargets := range []int{3, 5} {
		advanceTo := farFuture.Add(time.Duration(i+1) * 10 * time.Minute)
		n, err := s.ProcessDue(advanceTo)
		if err != nil || n != 1 {
			t.Fatalf("step %d: n=%d err=%v", i+1, n, err)
		}
		if items := s.ListOutbox(inc.ID); len(items) != cumTargets {
			t.Fatalf("after step %d outbox = %d items, want %d", i+1, len(items), cumTargets)
		}
	}
	got, _ = s.GetIncident(inc.ID)
	if got.NextStepIndex != 3 {
		t.Fatalf("final step index = %d, want 3", got.NextStepIndex)
	}
	if _, ok := s.Timer(inc.ID); ok {
		t.Fatalf("timer must be gone after last step")
	}

	// 全部步骤结束后再扫：无变化、无重复。
	n, _ = s.ProcessDue(t0.Add(100 * time.Hour))
	if n != 0 || len(s.ListOutbox(inc.ID)) != 5 {
		t.Fatalf("post-completion scan: n=%d items=%d", n, len(s.ListOutbox(inc.ID)))
	}

	// 每个 (事件,步骤,目标) 组合恰好一条意图。
	seen := map[string]bool{}
	for _, it := range s.ListOutbox(inc.ID) {
		key := fmt.Sprintf("%d/%s", it.StepIndex, it.Target)
		if seen[key] {
			t.Fatalf("duplicate intent %s", key)
		}
		seen[key] = true
	}
}

func TestAcknowledgeStopsEscalation(t *testing.T) {
	s := newTestStore(t)
	t0 := time.Unix(10_000, 0)
	inc, _ := s.CreateIncident(CreateIncidentRequest{RequestID: "r1", PolicyID: "p1"}, t0)

	acked, err := s.Acknowledge(AckRequest{RequestID: "a1", IncidentID: inc.ID, AcknowledgedBy: "alice"}, t0.Add(10*time.Second))
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if acked.Status != StatusAcknowledged || acked.AcknowledgedBy != "alice" {
		t.Fatalf("acked incident wrong: %+v", acked)
	}
	if _, ok := s.Timer(inc.ID); ok {
		t.Fatalf("ack must delete the timer")
	}

	// 幂等重放（同号同内容）。
	replay, err := s.Acknowledge(AckRequest{RequestID: "a1", IncidentID: inc.ID, AcknowledgedBy: "alice"}, t0.Add(20*time.Second))
	if err != nil || replay.Status != StatusAcknowledged {
		t.Fatalf("ack replay: %v %+v", err, replay)
	}
	// 新请求号再确认：已确认冲突。
	if _, err := s.Acknowledge(AckRequest{RequestID: "a2", IncidentID: inc.ID, AcknowledgedBy: "bob"}, t0); !errors.Is(err, ErrAlreadyAcknowledged) {
		t.Fatalf("second ack: want ErrAlreadyAcknowledged, got %v", err)
	}

	// 定时器即使残留到期（此处模拟重启后扫描）也不会产生通知。
	if n, _ := s.ProcessDue(t0.Add(10 * time.Hour)); n != 0 {
		t.Fatalf("acked incident advanced %d steps", n)
	}
	if len(s.ListOutbox(inc.ID)) != 0 {
		t.Fatalf("acked incident must produce no notifications")
	}

	// 已确认事件仍可解决。
	res, err := s.Resolve(ResolveRequest{RequestID: "x1", IncidentID: inc.ID, ResolvedBy: "alice"}, t0.Add(30*time.Second))
	if err != nil || res.Status != StatusResolved {
		t.Fatalf("resolve acked incident: %v %+v", err, res)
	}
}

func TestResolveBlocksAckAndNotifications(t *testing.T) {
	s := newTestStore(t)
	t0 := time.Unix(10_000, 0)
	inc, _ := s.CreateIncident(CreateIncidentRequest{RequestID: "r1", PolicyID: "p1"}, t0)

	if _, err := s.Resolve(ResolveRequest{RequestID: "x1", IncidentID: inc.ID, ResolvedBy: "bob"}, t0.Add(5*time.Second)); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	// 幂等重放。
	if _, err := s.Resolve(ResolveRequest{RequestID: "x1", IncidentID: inc.ID, ResolvedBy: "bob"}, t0); err != nil {
		t.Fatalf("resolve replay: %v", err)
	}
	// 新请求号重复解决。
	if _, err := s.Resolve(ResolveRequest{RequestID: "x2", IncidentID: inc.ID, ResolvedBy: "bob"}, t0); !errors.Is(err, ErrAlreadyResolved) {
		t.Fatalf("second resolve: want ErrAlreadyResolved, got %v", err)
	}
	// 解决后不能再确认。
	if _, err := s.Acknowledge(AckRequest{RequestID: "a1", IncidentID: inc.ID, AcknowledgedBy: "alice"}, t0); !errors.Is(err, ErrIncidentResolved) {
		t.Fatalf("ack after resolve: want ErrIncidentResolved, got %v", err)
	}
	// 解决后不再通知。
	if n, _ := s.ProcessDue(t0.Add(10 * time.Hour)); n != 0 || len(s.ListOutbox(inc.ID)) != 0 {
		t.Fatalf("resolved incident produced notifications: n=%d items=%d", n, len(s.ListOutbox(inc.ID)))
	}
}

func TestConcurrentAckResolveTimerConverges(t *testing.T) {
	// 零等待策略：每个定时器一扫描就到期，最大化计时器与确认/解决的竞争。
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
			_, _ = s.Acknowledge(AckRequest{
				RequestID: fmt.Sprintf("ack-%d", w), IncidentID: inc.ID, AcknowledgedBy: fmt.Sprintf("u%d", w),
			}, raceNow)
		}()
		go func() {
			defer wg.Done()
			_, _ = s.Resolve(ResolveRequest{
				RequestID: fmt.Sprintf("res-%d", w), IncidentID: inc.ID, ResolvedBy: fmt.Sprintf("u%d", w),
			}, raceNow)
		}()
	}
	wg.Wait()

	final, _ := s.GetIncident(inc.ID)
	switch final.Status {
	case StatusAcknowledged, StatusResolved:
	default:
		t.Fatalf("illegal final status: %s", final.Status)
	}
	if final.Status == StatusAcknowledged && !final.ResolvedAt.IsZero() {
		t.Fatalf("acknowledged incident must not carry resolve time")
	}

	// 任何 (步骤,目标) 最多一条 outbox；步骤推进数不超过一次/调用。
	counts := map[string]int{}
	for _, it := range s.ListOutbox(inc.ID) {
		counts[fmt.Sprintf("%d/%s", it.StepIndex, it.Target)]++
	}
	for k, c := range counts {
		if c > 1 {
			t.Fatalf("intent %s appeared %d times", k, c)
		}
	}
	// 已解决/已确认后绝不能再有“新”通知；解决之前已合法触发的通知允许存在，
	// 但触发的步骤必须是从 0 开始的连续前缀（没有跳步）。
	firedSteps := map[int]bool{}
	for k := range counts {
		var idx int
		fmt.Sscanf(k, "%d/", &idx)
		firedSteps[idx] = true
	}
	for i := 0; i < final.NextStepIndex; i++ {
		if !firedSteps[i] {
			t.Fatalf("step %d missing while progress reached %d (skip detected)", i, final.NextStepIndex)
		}
	}

	// 收敛后继续扫描，世界不再变化。
	before := len(s.ListOutbox(inc.ID))
	for i := 0; i < 5; i++ {
		if _, err := s.ProcessDue(raceNow.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	if after := len(s.ListOutbox(inc.ID)); after != before {
		t.Fatalf("state kept changing after convergence: %d -> %d", before, after)
	}
}

func TestConcurrentSameRequestIDCreateIsIdempotent(t *testing.T) {
	s := newTestStore(t)
	now := time.Unix(1000, 0)
	const n = 32
	var wg sync.WaitGroup
	ids := make([]string, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			inc, err := s.CreateIncident(CreateIncidentRequest{RequestID: "same-req", PolicyID: "p1", Detail: "x"}, now)
			if err == nil {
				ids[i] = inc.ID
			}
			errs[i] = err
		}(i)
	}
	wg.Wait()
	var first string
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent create %d failed: %v", i, err)
		}
		if first == "" {
			first = ids[i]
		} else if ids[i] != first {
			t.Fatalf("concurrent same request id created different incidents: %s vs %s", first, ids[i])
		}
	}
	if len(s.ListIncidents()) != 1 {
		t.Fatalf("want exactly 1 incident, got %d", len(s.ListIncidents()))
	}
}

func TestRestartResumesTimersAndOutbox(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	t0 := time.Unix(50_000, 0)

	s, err := OpenStore(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.CreatePolicy(testPolicy("p1"), t0); err != nil {
		t.Fatal(err)
	}
	inc, err := s.CreateIncident(CreateIncidentRequest{RequestID: "r1", PolicyID: "p1", Detail: "boom"}, t0)
	if err != nil {
		t.Fatal(err)
	}
	// step0 在重启前触发并落盘。
	if n, err := s.ProcessDue(t0.Add(time.Minute)); err != nil || n != 1 {
		t.Fatalf("step0: n=%d err=%v", n, err)
	}

	// 模拟进程重启：重新打开同一文件。
	s2, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	tmr, ok := s2.Timer(inc.ID)
	if !ok || tmr.StepIndex != 1 {
		t.Fatalf("timer not recovered: %+v ok=%v", tmr, ok)
	}
	if got := s2.ListOutbox(inc.ID); len(got) != 2 || got[0].Status != OutboxPending {
		t.Fatalf("outbox not recovered: %+v", got)
	}
	restored, err := s2.GetIncident(inc.ID)
	if err != nil || restored.NextStepIndex != 1 || len(restored.FrozenSteps) != 3 {
		t.Fatalf("incident not restored: %v %+v", err, restored)
	}
	// 重启后接着推进 step1。
	if n, err := s2.ProcessDue(t0.Add(3 * time.Minute)); err != nil || n != 1 {
		t.Fatalf("step1 after restart: n=%d err=%v", n, err)
	}
	if items := s2.ListOutbox(inc.ID); len(items) != 3 {
		t.Fatalf("after restart step1 outbox = %d items", len(items))
	}
	// 幂等请求表也恢复：同号异内容仍然冲突。
	if _, err := s2.CreateIncident(CreateIncidentRequest{RequestID: "r1", PolicyID: "p1", Detail: "other"}, t0); !errors.Is(err, ErrConflict) {
		t.Fatalf("idempotency table lost across restart: %v", err)
	}
}

func TestOutboxMarkDelivered(t *testing.T) {
	s := newTestStore(t)
	t0 := time.Unix(1000, 0)
	inc, _ := s.CreateIncident(CreateIncidentRequest{RequestID: "r1", PolicyID: "p1"}, t0)
	if _, err := s.ProcessDue(t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	items := s.PendingOutbox()
	if len(items) != 2 {
		t.Fatalf("pending = %d", len(items))
	}
	if err := s.MarkDelivered(items[0].ID, t0.Add(2*time.Minute)); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if len(s.PendingOutbox()) != 1 {
		t.Fatalf("pending after mark = %d", len(s.PendingOutbox()))
	}
	if err := s.MarkDelivered(999, t0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("mark missing: want ErrNotFound, got %v", err)
	}
	if incs := s.ListOutbox(""); len(incs) != 2 {
		t.Fatalf("list all outbox = %d", len(incs))
	}
	if _, err := s.History("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("history missing: want ErrNotFound, got %v", err)
	}
	_ = inc
}

func TestHistory(t *testing.T) {
	s := newTestStore(t)
	t0 := time.Unix(1000, 0)
	inc, _ := s.CreateIncident(CreateIncidentRequest{RequestID: "r1", PolicyID: "p1"}, t0)
	if _, err := s.ProcessDue(t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acknowledge(AckRequest{RequestID: "a1", IncidentID: inc.ID, AcknowledgedBy: "alice"}, t0.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	h, err := s.History(inc.ID)
	if err != nil {
		t.Fatal(err)
	}
	kinds := make([]string, len(h))
	for i, e := range h {
		kinds[i] = e.Kind
	}
	want := []string{"created", "step_fired", "acknowledged"}
	if fmt.Sprint(kinds) != fmt.Sprint(want) {
		t.Fatalf("history kinds = %v, want %v", kinds, want)
	}
	all, _ := s.History("")
	if len(all) != len(h) {
		t.Fatalf("global history length = %d, want %d", len(all), len(h))
	}
}

func TestMultipleIncidentsProcessInOneScan(t *testing.T) {
	s := newTestStore(t)
	t0 := time.Unix(1000, 0)
	i1, _ := s.CreateIncident(CreateIncidentRequest{RequestID: "r1", PolicyID: "p1", IncidentID: "i1"}, t0)
	i2, _ := s.CreateIncident(CreateIncidentRequest{RequestID: "r2", PolicyID: "p1", IncidentID: "i2"}, t0.Add(time.Second))
	n, err := s.ProcessDue(t0.Add(10 * time.Minute))
	if err != nil || n != 2 {
		t.Fatalf("scan n=%d err=%v, want 2 (one step per incident)", n, err)
	}
	for _, id := range []string{i1.ID, i2.ID} {
		inc, _ := s.GetIncident(id)
		if inc.NextStepIndex != 1 {
			t.Fatalf("%s advanced to %d, want 1", id, inc.NextStepIndex)
		}
	}
	// 顺序稳定：按 incident id 排序处理。
	items := s.PendingOutbox()
	targets := []string{}
	for _, it := range items {
		targets = append(targets, it.IncidentID+"/"+it.Target)
	}
	sorted := append([]string(nil), targets...)
	sort.Strings(sorted)
	if fmt.Sprint(targets) != fmt.Sprint(sorted) {
		t.Fatalf("outbox ordering not stable: %v", targets)
	}
}
