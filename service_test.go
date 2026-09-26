package incidentescalation

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeClock 可手动推进的测试时钟。
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 9, 27, 8, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// newTestService 返回使用临时目录存储与假时钟的服务。
func newTestService(t *testing.T) (*Service, *fakeClock, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	clock := newFakeClock()
	return NewService(store).WithClock(clock), clock, dir
}

// mustPolicy 创建三步策略：每步等待 1 分钟，目标分别为 p1/p2/p3。
func mustPolicy(t *testing.T, s *Service) Policy {
	t.Helper()
	p, err := s.CreatePolicy(CreatePolicyInput{
		ID:   "oncall",
		Name: "值班升级",
		Steps: []Step{
			{WaitDuration: time.Minute, Targets: []string{"p1"}},
			{WaitDuration: time.Minute, Targets: []string{"p2", "p2-backup"}},
			{WaitDuration: time.Minute, Targets: []string{"p3"}},
		},
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}
	return p
}

func mustIncident(t *testing.T, s *Service, reqID string) *Incident {
	t.Helper()
	inc, err := s.CreateIncident(CreateIncidentInput{
		RequestID: reqID, PolicyID: "oncall", Title: "数据库主库失联",
	})
	if err != nil {
		t.Fatalf("create incident: %v", err)
	}
	return inc
}

func TestCreatePolicyValidation(t *testing.T) {
	s, _, _ := newTestService(t)

	if _, err := s.CreatePolicy(CreatePolicyInput{ID: ""}); !errors.Is(err, ErrValidation) {
		t.Fatalf("empty id: want ErrValidation, got %v", err)
	}
	if _, err := s.CreatePolicy(CreatePolicyInput{ID: "p"}); !errors.Is(err, ErrValidation) {
		t.Fatalf("no steps: want ErrValidation, got %v", err)
	}
	if _, err := s.CreatePolicy(CreatePolicyInput{
		ID: "p", Steps: []Step{{WaitDuration: 0, Targets: []string{"a"}}},
	}); !errors.Is(err, ErrValidation) {
		t.Fatalf("zero wait: want ErrValidation, got %v", err)
	}
	if _, err := s.CreatePolicy(CreatePolicyInput{
		ID: "p", Steps: []Step{{WaitDuration: time.Minute}},
	}); !errors.Is(err, ErrValidation) {
		t.Fatalf("no targets: want ErrValidation, got %v", err)
	}

	// 同一步骤内重复目标被去重。
	p, err := s.CreatePolicy(CreatePolicyInput{
		ID: "p", Steps: []Step{{WaitDuration: time.Minute, Targets: []string{"a", "a", "b"}}},
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}
	if len(p.Steps[0].Targets) != 2 {
		t.Fatalf("duplicate targets not deduped: %v", p.Steps[0].Targets)
	}
}

func TestCreateIncidentFreezesPolicy(t *testing.T) {
	s, _, _ := newTestService(t)
	mustPolicy(t, s)
	inc := mustIncident(t, s, "req-1")

	// 创建后修改策略，事件内冻结的快照不应变化。
	if _, err := s.CreatePolicy(CreatePolicyInput{
		ID: "oncall", Steps: []Step{{WaitDuration: time.Hour, Targets: []string{"someone-else"}}},
	}); err != nil {
		t.Fatalf("replace policy: %v", err)
	}

	got, err := s.GetIncident(inc.ID)
	if err != nil {
		t.Fatalf("get incident: %v", err)
	}
	if len(got.Policy.Steps) != 3 || got.Policy.Steps[0].Targets[0] != "p1" {
		t.Fatalf("policy snapshot not frozen: %+v", got.Policy)
	}
}

func TestCreateIncidentSchedulesStepZero(t *testing.T) {
	s, clock, _ := newTestService(t)
	mustPolicy(t, s)
	inc := mustIncident(t, s, "req-1")

	outbox := s.ListOutboxForIncident(inc.ID)
	if len(outbox) != 1 || outbox[0].Step != 0 || outbox[0].Target != "p1" {
		t.Fatalf("step-0 notification missing: %+v", outbox)
	}

	// 定时器未到期前扫描不应推进。
	if rs := s.TickDue(); len(rs) != 0 {
		t.Fatalf("tick before due should not advance: %+v", rs)
	}

	clock.Advance(time.Minute)
	rs := s.TickDue()
	if len(rs) != 1 || rs[0].ToStep != 1 {
		t.Fatalf("tick at due should advance to step 1: %+v", rs)
	}
	outbox = s.ListOutboxForIncident(inc.ID)
	if len(outbox) != 3 { // p1 + p2 + p2-backup
		t.Fatalf("step-1 notifications missing: %+v", outbox)
	}
}

func TestTickAdvancesAtMostOneStepPerScan(t *testing.T) {
	s, clock, _ := newTestService(t)
	mustPolicy(t, s)
	inc := mustIncident(t, s, "req-1")

	// 模拟进程停机 10 分钟：三步的定时器在重启后全部「过期」。
	clock.Advance(10 * time.Minute)

	// 第一次扫描只能推进一步，即使后续步骤也均已到期。
	rs := s.TickDue()
	if len(rs) != 1 || rs[0].FromStep != 0 || rs[0].ToStep != 1 {
		t.Fatalf("first tick must advance exactly one step: %+v", rs)
	}
	got, _ := s.GetIncident(inc.ID)
	if got.CurrentStep != 1 {
		t.Fatalf("incident jumped steps: current=%d", got.CurrentStep)
	}

	// 重复扫描逐步追赶：第二步、然后耗尽。
	rs = s.TickDue()
	if len(rs) != 1 || rs[0].ToStep != 2 {
		t.Fatalf("second tick should reach step 2: %+v", rs)
	}
	rs = s.TickDue()
	if len(rs) != 1 || !rs[0].Exhausted || rs[0].ToStep != -1 {
		t.Fatalf("third tick should exhaust the chain: %+v", rs)
	}
	got, _ = s.GetIncident(inc.ID)
	if !got.Exhausted {
		t.Fatal("incident should be exhausted after last step")
	}
	if rs := s.TickDue(); len(rs) != 0 {
		t.Fatalf("exhausted incident must not advance: %+v", rs)
	}

	// 全程 outbox 不应出现重复意图。
	outbox := s.ListOutboxForIncident(inc.ID)
	if len(outbox) != 4 { // p1, p2, p2-backup, p3
		t.Fatalf("duplicate outbox entries: %+v", outbox)
	}
}

func TestTickDoesNotDuplicateOutboxOnRescan(t *testing.T) {
	s, clock, _ := newTestService(t)
	mustPolicy(t, s)
	inc := mustIncident(t, s, "req-1")

	clock.Advance(time.Minute)
	for i := 0; i < 3; i++ {
		s.TickDue() // 重复扫描同一到期窗口
		clock.Advance(time.Minute)
	}
	outbox := s.ListOutboxForIncident(inc.ID)
	seen := map[string]int{}
	for _, e := range outbox {
		seen[e.Key]++
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("outbox entry %s duplicated %d times", k, n)
		}
	}
}

func TestAcknowledgeStopsEscalation(t *testing.T) {
	s, clock, _ := newTestService(t)
	mustPolicy(t, s)
	inc := mustIncident(t, s, "req-1")

	ack, err := s.Acknowledge("ack-1", inc.ID, "oncall-a")
	if err != nil {
		t.Fatalf("acknowledge: %v", err)
	}
	if ack.Status != StatusAcknowledged || ack.AckedBy != "oncall-a" {
		t.Fatalf("unexpected ack result: %+v", ack)
	}

	// 确认后即使时间越过所有步骤，也不再推进、不再产生通知。
	clock.Advance(time.Hour)
	if rs := s.TickDue(); len(rs) != 0 {
		t.Fatalf("acknowledged incident must not escalate: %+v", rs)
	}
	if n := len(s.ListOutboxForIncident(inc.ID)); n != 1 {
		t.Fatalf("acknowledged incident produced extra notifications: %d", n)
	}
}

func TestResolveTerminatesIncident(t *testing.T) {
	s, clock, _ := newTestService(t)
	mustPolicy(t, s)
	inc := mustIncident(t, s, "req-1")

	if _, err := s.Resolve("res-1", inc.ID, "oncall-a"); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	// 解决后不能再确认。
	if _, err := s.Acknowledge("ack-1", inc.ID, "oncall-b"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("ack after resolve: want ErrInvalidState, got %v", err)
	}
	// 解决后不再推进、不再通知。
	clock.Advance(time.Hour)
	if rs := s.TickDue(); len(rs) != 0 {
		t.Fatalf("resolved incident must not escalate: %+v", rs)
	}
	if n := len(s.ListOutboxForIncident(inc.ID)); n != 1 {
		t.Fatalf("resolved incident produced extra notifications: %d", n)
	}
}

func TestIdempotentCreate(t *testing.T) {
	s, _, _ := newTestService(t)
	mustPolicy(t, s)

	in := CreateIncidentInput{RequestID: "req-1", PolicyID: "oncall", Title: "t"}
	first, err := s.CreateIncident(in)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	second, err := s.CreateIncident(in)
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("replay returned different incident: %s vs %s", first.ID, second.ID)
	}
	if n := len(s.ListIncidents()); n != 1 {
		t.Fatalf("replay created extra incident: %d", n)
	}

	// 同号异内容报冲突。
	in.Title = "different"
	if _, err := s.CreateIncident(in); !errors.Is(err, ErrConflict) {
		t.Fatalf("same request id different content: want ErrConflict, got %v", err)
	}
}

func TestIdempotentAckAndResolve(t *testing.T) {
	s, _, _ := newTestService(t)
	mustPolicy(t, s)
	inc := mustIncident(t, s, "req-1")

	if _, err := s.Acknowledge("ack-1", inc.ID, "a"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	again, err := s.Acknowledge("ack-1", inc.ID, "a")
	if err != nil {
		t.Fatalf("ack replay: %v", err)
	}
	if again.Status != StatusAcknowledged {
		t.Fatalf("ack replay returned wrong state: %+v", again)
	}
	if _, err := s.Acknowledge("ack-1", inc.ID, "someone-else"); !errors.Is(err, ErrConflict) {
		t.Fatalf("ack conflict: want ErrConflict, got %v", err)
	}

	if _, err := s.Resolve("res-1", inc.ID, "a"); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if _, err := s.Resolve("res-1", inc.ID, "a"); err != nil {
		t.Fatalf("resolve replay: %v", err)
	}
	if _, err := s.Resolve("res-1", inc.ID, "b"); !errors.Is(err, ErrConflict) {
		t.Fatalf("resolve conflict: want ErrConflict, got %v", err)
	}

	// 历史里确认与解决各只出现一次。
	hist, _ := s.GetHistory(inc.ID)
	var acks, resolves int
	for _, h := range hist {
		switch h.Type {
		case HistoryAcknowledged:
			acks++
		case HistoryResolved:
			resolves++
		}
	}
	if acks != 1 || resolves != 1 {
		t.Fatalf("idempotent ops recorded multiple times: acks=%d resolves=%d", acks, resolves)
	}
}

func TestConcurrentAckResolveAndTick(t *testing.T) {
	// 并发压测：确认、解决、到期推进同时发生，最终只能形成一个合法状态。
	for trial := 0; trial < 20; trial++ {
		s, clock, _ := newTestService(t)
		mustPolicy(t, s)
		inc := mustIncident(t, s, fmt.Sprintf("req-%d", trial))
		clock.Advance(time.Minute) // 让定时器到期，制造真实竞态

		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(3)
			go func(i int) {
				defer wg.Done()
				_, _ = s.Acknowledge(fmt.Sprintf("ack-%d", i), inc.ID, "a")
			}(i)
			go func(i int) {
				defer wg.Done()
				_, _ = s.Resolve(fmt.Sprintf("res-%d", i), inc.ID, "r")
			}(i)
			go func() {
				defer wg.Done()
				s.TickDue()
			}()
		}
		wg.Wait()

		got, err := s.GetIncident(inc.ID)
		if err != nil {
			t.Fatalf("get incident: %v", err)
		}
		switch got.Status {
		case StatusAcknowledged, StatusResolved:
		default:
			t.Fatalf("illegal final status: %s", got.Status)
		}
		if got.Status == StatusResolved && got.ResolvedAt == nil {
			t.Fatal("resolved incident missing ResolvedAt")
		}
		if got.Status == StatusAcknowledged && got.AckedAt == nil {
			t.Fatal("acknowledged incident missing AckedAt")
		}
		// 终结后不再推进。
		clock.Advance(time.Hour)
		s.TickDue()
		after, _ := s.GetIncident(inc.ID)
		if after.CurrentStep != got.CurrentStep || after.Status != got.Status {
			t.Fatalf("state changed after termination: %+v -> %+v", got, after)
		}
	}
}

func TestRestartResumesTimers(t *testing.T) {
	s, clock, dir := newTestService(t)
	mustPolicy(t, s)
	inc := mustIncident(t, s, "req-1")

	clock.Advance(time.Minute) // 第 0 步定时器到期
	s.TickDue()                // 推进到第 1 步

	// 模拟进程重启：用同一目录重新打开存储。
	store2, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	s2 := NewService(store2).WithClock(clock)

	got, err := s2.GetIncident(inc.ID)
	if err != nil {
		t.Fatalf("incident lost after restart: %v", err)
	}
	if got.CurrentStep != 1 || got.Status != StatusOpen {
		t.Fatalf("state not persisted: %+v", got)
	}
	if n := len(s2.ListOutboxForIncident(inc.ID)); n != 3 {
		t.Fatalf("outbox not persisted: %d entries", n)
	}

	// 重启后继续按持久化的定时器推进。
	clock.Advance(time.Minute)
	rs := s2.TickDue()
	if len(rs) != 1 || rs[0].ToStep != 2 {
		t.Fatalf("escalation did not resume after restart: %+v", rs)
	}
}

func TestRestartIdempotencySurvives(t *testing.T) {
	s, _, dir := newTestService(t)
	mustPolicy(t, s)
	inc := mustIncident(t, s, "req-1")

	store2, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	s2 := NewService(store2)

	// 重启后同号重放仍返回同一事件，异内容仍冲突。
	replay, err := s2.CreateIncident(CreateIncidentInput{RequestID: "req-1", PolicyID: "oncall", Title: "数据库主库失联"})
	if err != nil {
		t.Fatalf("replay after restart: %v", err)
	}
	if replay.ID != inc.ID {
		t.Fatalf("replay returned different incident after restart")
	}
	if _, err := s2.CreateIncident(CreateIncidentInput{RequestID: "req-1", PolicyID: "oncall", Title: "别的"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict after restart: want ErrConflict, got %v", err)
	}
}

func TestHistoryRecordsLifecycle(t *testing.T) {
	s, clock, _ := newTestService(t)
	mustPolicy(t, s)
	inc := mustIncident(t, s, "req-1")

	clock.Advance(time.Minute)
	s.TickDue()
	if _, err := s.Acknowledge("ack-1", inc.ID, "oncall-a"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if _, err := s.Resolve("res-1", inc.ID, "oncall-a"); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	hist, err := s.GetHistory(inc.ID)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	var types []HistoryEventType
	for _, h := range hist {
		types = append(types, h.Type)
	}
	want := []HistoryEventType{
		HistoryNotified, HistoryCreated, // 创建时：通知意图 + 创建记录
		HistoryNotified, HistoryNotified, HistoryEscalated, // 推进到第 1 步：两条通知 + 升级
		HistoryAcknowledged, HistoryResolved,
	}
	if len(types) != len(want) {
		t.Fatalf("history length: got %v want %v", types, want)
	}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("history[%d]: got %v want %v", i, types, want)
		}
	}

	if _, err := s.GetHistory("inc-missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("history of missing incident: want ErrNotFound, got %v", err)
	}
}

func TestOutboxMarkSent(t *testing.T) {
	s, _, _ := newTestService(t)
	mustPolicy(t, s)
	inc := mustIncident(t, s, "req-1")

	outbox := s.ListOutbox()
	if len(outbox) != 1 {
		t.Fatalf("want 1 pending entry, got %d", len(outbox))
	}
	if err := s.MarkOutboxSent(outbox[0].Key); err != nil {
		t.Fatalf("mark sent: %v", err)
	}
	if n := len(s.ListOutbox()); n != 0 {
		t.Fatalf("entry not removed: %d", n)
	}
	if n := len(s.ListOutboxForIncident(inc.ID)); n != 0 {
		t.Fatalf("entry not removed for incident: %d", n)
	}
	if err := s.MarkOutboxSent(outbox[0].Key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("double mark: want ErrNotFound, got %v", err)
	}
}

func TestNotFoundErrors(t *testing.T) {
	s, _, _ := newTestService(t)
	if _, err := s.GetPolicy("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get policy: want ErrNotFound, got %v", err)
	}
	if _, err := s.GetIncident("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get incident: want ErrNotFound, got %v", err)
	}
	if _, err := s.CreateIncident(CreateIncidentInput{RequestID: "r", PolicyID: "nope", Title: "t"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("create with missing policy: want ErrNotFound, got %v", err)
	}
	if _, err := s.Acknowledge("a", "nope", "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ack missing: want ErrNotFound, got %v", err)
	}
	if _, err := s.Resolve("r", "nope", "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("resolve missing: want ErrNotFound, got %v", err)
	}
}

func TestMissingRequestIDRejected(t *testing.T) {
	s, _, _ := newTestService(t)
	mustPolicy(t, s)
	if _, err := s.CreateIncident(CreateIncidentInput{PolicyID: "oncall", Title: "t"}); !errors.Is(err, ErrValidation) {
		t.Fatalf("create without request id: want ErrValidation, got %v", err)
	}
	inc := mustIncident(t, s, "req-1")
	if _, err := s.Acknowledge("", inc.ID, "a"); !errors.Is(err, ErrValidation) {
		t.Fatalf("ack without request id: want ErrValidation, got %v", err)
	}
	if _, err := s.Resolve("", inc.ID, "a"); !errors.Is(err, ErrValidation) {
		t.Fatalf("resolve without request id: want ErrValidation, got %v", err)
	}
}
