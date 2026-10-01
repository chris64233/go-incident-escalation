package incidentescalation

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// recordingDispatcher 记录投递调用；failing 集合中的目标返回失败模拟下游故障。
type recordingDispatcher struct {
	mu      sync.Mutex
	got     []string
	failing map[string]bool
}

func (d *recordingDispatcher) Dispatch(item OutboxItem) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failing[item.Target] {
		return errDownstream
	}
	d.got = append(d.got, item.Target)
	return nil
}

func (d *recordingDispatcher) delivered() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.got...)
}

var errDownstream = errors.New("downstream timeout")

func TestServiceFiresTimersAndDispatches(t *testing.T) {
	s := NewMemoryStore()
	now := time.Unix(1000, 0)
	if _, err := s.CreatePolicy(PolicyInput{
		ID: "p", Name: "p",
		Steps: []Step{
			{WaitBefore: 20 * time.Millisecond, Targets: []string{"alice"}},
			{WaitBefore: 20 * time.Millisecond, Targets: []string{"bob"}},
		},
	}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateIncident(CreateIncidentRequest{RequestID: "r1", PolicyID: "p"}, now); err != nil {
		t.Fatal(err)
	}

	disp := &recordingDispatcher{}
	svc := NewService(s, disp, Config{TickInterval: 5 * time.Millisecond, DispatchInterval: 5 * time.Millisecond})
	svc.Start()
	svc.Start() // 重复 Start 不应另起循环
	defer svc.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(disp.delivered()) == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	got := disp.delivered()
	if len(got) != 2 || got[0] != "alice" || got[1] != "bob" {
		t.Fatalf("dispatched = %v, want [alice bob] in order", got)
	}
	if pending := s.PendingOutbox(); len(pending) != 0 {
		t.Fatalf("pending after dispatch = %d", len(pending))
	}
}

func TestServiceDispatcherRetryThenAckStops(t *testing.T) {
	s := NewMemoryStore()
	base := time.Unix(1000, 0)
	if _, err := s.CreatePolicy(PolicyInput{
		ID: "p", Name: "p",
		Steps: []Step{
			{WaitBefore: 10 * time.Millisecond, Targets: []string{"alice"}},
			{WaitBefore: 10 * time.Millisecond, Targets: []string{"bob"}},
		},
	}, base); err != nil {
		t.Fatal(err)
	}
	inc, _ := s.CreateIncident(CreateIncidentRequest{RequestID: "r1", PolicyID: "p"}, base)

	disp := &recordingDispatcher{failing: map[string]bool{"alice": true}}
	svc := NewService(s, disp, Config{TickInterval: 5 * time.Millisecond, DispatchInterval: 5 * time.Millisecond})
	svc.Start()

	// 等到 step0 意图产生并进入重试。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(s.ListOutbox(inc.ID)) == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	// 故障恢复后应能投递成功（at-least-once 重试）。
	disp.mu.Lock()
	disp.failing = map[string]bool{}
	disp.mu.Unlock()

	// 确认事件：step1 永不触发，只有 alice 一条意图。
	if _, err := s.Acknowledge(AckRequest{RequestID: "a1", IncidentID: inc.ID, AcknowledgedBy: "oncall"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	svc.Stop()

	got := disp.delivered()
	if len(got) != 1 || got[0] != "alice" {
		t.Fatalf("delivered = %v, want only [alice]", got)
	}
	if items := s.ListOutbox(inc.ID); len(items) != 1 {
		t.Fatalf("outbox items = %d, want 1 (step0 only)", len(items))
	}
}

func TestServiceAutoReleasesWindowAndDispatchesCatchUp(t *testing.T) {
	s := NewMemoryStore()
	base := time.Unix(1000, 0)
	if _, err := s.CreatePolicy(PolicyInput{
		ID: "p", Name: "p",
		Steps: []Step{
			{WaitBefore: 5 * time.Millisecond, Targets: []string{"alice"}},
			{WaitBefore: 5 * time.Millisecond, Targets: []string{"bob"}},
		},
	}, base); err != nil {
		t.Fatal(err)
	}
	inc, err := s.CreateIncident(CreateIncidentRequest{RequestID: "r1", PolicyID: "p"}, base)
	if err != nil {
		t.Fatal(err)
	}

	// 极短维护窗口覆盖升级链：两级在窗口内触发（被抑制）。
	if _, err := s.CreateSuppression(SuppressionRequest{
		RequestID: "create-sup",
		SuppressionInput: SuppressionInput{
			RuleID:    "sup",
			Scope:     SuppressionScope{IncidentIDs: []string{inc.ID}},
			Reason:    "maintenance",
			StartsAt:  base,
			EndsAt:    base.Add(50 * time.Millisecond),
			Condition: ReleaseAtWindowEnd,
		},
	}, base); err != nil {
		t.Fatal(err)
	}

	disp := &recordingDispatcher{}
	svc := NewService(s, disp, Config{TickInterval: 5 * time.Millisecond, DispatchInterval: 5 * time.Millisecond})
	svc.Start()
	defer svc.Stop()

	// 后台循环：窗口内两级照走但零投递；窗口结束后自动解除并补发两级。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(disp.delivered()) == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	got := disp.delivered()
	if len(got) != 2 || got[0] != "alice" || got[1] != "bob" {
		t.Fatalf("catch-up delivered = %v, want [alice bob]", got)
	}
	rule, _ := s.GetSuppression("sup")
	if rule.Status != SuppressionReleased || rule.ReleasedBy != "system" {
		t.Fatalf("rule not auto-released by service: %+v", rule)
	}
	if pending := s.PendingOutbox(); len(pending) != 0 {
		t.Fatalf("pending after catch-up dispatch = %d", len(pending))
	}
}
