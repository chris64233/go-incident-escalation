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

// newHandoffStore 建好策略，并创建三个归 g1 的事件：
// inc-firing 保持 firing，inc-ack 被确认，inc-resolved 被解决。
func newHandoffStore(t *testing.T) (*Store, time.Time) {
	t.Helper()
	s := NewMemoryStore()
	now := time.Unix(1000, 0)
	if _, err := s.CreatePolicy(testPolicy("p1"), now); err != nil {
		t.Fatalf("seed policy: %v", err)
	}
	for i, id := range []string{"inc-firing", "inc-ack", "inc-resolved"} {
		if _, err := s.CreateIncident(CreateIncidentRequest{
			RequestID:  fmt.Sprintf("create-%s", id),
			IncidentID: id,
			PolicyID:   "p1",
			Detail:     "detail-" + id,
			OwnerGroup: "g1",
		}, now.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	if _, err := s.Acknowledge(AckRequest{RequestID: "ack-1", IncidentID: "inc-ack", AcknowledgedBy: "oncall"}, now); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if _, err := s.Resolve(ResolveRequest{RequestID: "res-1", IncidentID: "inc-resolved", ResolvedBy: "oncall"}, now); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	return s, now
}

func TestHandoffTransfersOnlyFiring(t *testing.T) {
	s, now := newHandoffStore(t)

	// 触发第 0 步，让 firing 事件产生两条 pending 升级意图；
	// 其中一条标记为已投递，验证“已发通知保留原记录”。
	if _, err := s.ProcessDue(now.Add(time.Minute)); err != nil {
		t.Fatalf("process due: %v", err)
	}
	pending := s.PendingOutboxFor("g1")
	if len(pending) != 2 {
		t.Fatalf("pending for g1 = %d, want 2", len(pending))
	}
	if err := s.MarkDelivered(pending[0].ID, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("mark delivered: %v", err)
	}

	rec, err := s.Handoff(HandoffRequest{RequestID: "ho-req-1", FromGroup: "g1", ToGroup: "g2"}, now.Add(3*time.Minute))
	if err != nil {
		t.Fatalf("handoff: %v", err)
	}

	// 交接生效时冻结清单：只有仍 firing 的事件被移交。
	if len(rec.IncidentIDs) != 1 || rec.IncidentIDs[0] != "inc-firing" {
		t.Fatalf("handoff list = %v, want [inc-firing]", rec.IncidentIDs)
	}
	if rec.FromGroup != "g1" || rec.ToGroup != "g2" || rec.ID == "" {
		t.Fatalf("unexpected record: %+v", rec)
	}

	firing, _ := s.GetIncident("inc-firing")
	if firing.OwnerGroup != "g2" {
		t.Fatalf("inc-firing owner = %q, want g2", firing.OwnerGroup)
	}
	// 已确认/已解决事件不再移交。
	for _, id := range []string{"inc-ack", "inc-resolved"} {
		inc, _ := s.GetIncident(id)
		if inc.OwnerGroup != "g1" {
			t.Fatalf("%s owner = %q, want g1 (not transferred)", id, inc.OwnerGroup)
		}
	}

	// 未派发的升级意图改派给 g2，已投递的保留 g1 原记录。
	items := s.ListOutbox("inc-firing")
	var delivered, reassigned, notice int
	for _, it := range items {
		switch {
		case it.Kind == OutboxKindHandoff:
			notice++
			if it.Target != "g2" || it.OwnerGroup != "g2" || it.StepIndex != -1 {
				t.Fatalf("bad handoff notice: %+v", it)
			}
		case it.Status == OutboxDelivered:
			delivered++
			if it.OwnerGroup != "g1" {
				t.Fatalf("delivered item owner = %q, want g1 (original record kept)", it.OwnerGroup)
			}
		case it.Status == OutboxPending:
			reassigned++
			if it.OwnerGroup != "g2" {
				t.Fatalf("pending item owner = %q, want g2", it.OwnerGroup)
			}
		}
	}
	if delivered != 1 || reassigned != 1 || notice != 1 {
		t.Fatalf("outbox = delivered:%d reassigned:%d notice:%d, want 1/1/1", delivered, reassigned, notice)
	}

	// 旧班组不再看到任何待派发意图，新班组看到 2 条（改派的升级 + 交接通知）。
	if got := s.PendingOutboxFor("g1"); len(got) != 0 {
		t.Fatalf("pending for g1 after handoff = %d, want 0", len(got))
	}
	if got := s.PendingOutboxFor("g2"); len(got) != 2 {
		t.Fatalf("pending for g2 after handoff = %d, want 2", len(got))
	}

	// 历史中有交接记录。
	hist, err := s.History("inc-firing")
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	found := false
	for _, h := range hist {
		if h.Kind == "handoff" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no handoff entry in history: %+v", hist)
	}
}

func TestHandoffIdempotentAndNoDuplicate(t *testing.T) {
	s, now := newHandoffStore(t)

	rec, err := s.Handoff(HandoffRequest{RequestID: "ho-req-1", FromGroup: "g1", ToGroup: "g2"}, now)
	if err != nil {
		t.Fatalf("handoff: %v", err)
	}
	outboxBefore := len(s.ListOutbox(""))

	// 同 RequestID 重放：返回同一交接单，不重复移交、不重复通知。
	replay, err := s.Handoff(HandoffRequest{RequestID: "ho-req-1", FromGroup: "g1", ToGroup: "g2"}, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replay.ID != rec.ID || len(replay.IncidentIDs) != 1 {
		t.Fatalf("replay = %+v, want same record as %+v", replay, rec)
	}
	if got := len(s.ListOutbox("")); got != outboxBefore {
		t.Fatalf("outbox grew after replay: %d -> %d", outboxBefore, got)
	}

	// 同号异内容：冲突。
	if _, err := s.Handoff(HandoffRequest{RequestID: "ho-req-1", FromGroup: "g1", ToGroup: "g3"}, now); !errors.Is(err, ErrConflict) {
		t.Fatalf("reused request id: want ErrConflict, got %v", err)
	}

	// 用新请求号对同一 FromGroup 重复交接：清单为空，负责人不再变化，也不产生新通知。
	rec2, err := s.Handoff(HandoffRequest{RequestID: "ho-req-2", FromGroup: "g1", ToGroup: "g3"}, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("second handoff: %v", err)
	}
	if len(rec2.IncidentIDs) != 0 {
		t.Fatalf("second handoff list = %v, want empty", rec2.IncidentIDs)
	}
	inc, _ := s.GetIncident("inc-firing")
	if inc.OwnerGroup != "g2" {
		t.Fatalf("owner after duplicate handoff = %q, want g2", inc.OwnerGroup)
	}
	if got := len(s.ListOutbox("")); got != outboxBefore {
		t.Fatalf("outbox grew after duplicate handoff: %d -> %d", outboxBefore, got)
	}
}

func TestHandoffValidation(t *testing.T) {
	s, now := newHandoffStore(t)
	cases := []HandoffRequest{
		{RequestID: "v1", FromGroup: "", ToGroup: "g2"},
		{RequestID: "v2", FromGroup: "g1", ToGroup: ""},
		{RequestID: "v3", FromGroup: "g1", ToGroup: "g1"},
		{RequestID: "", FromGroup: "g1", ToGroup: "g2"},
	}
	for i, req := range cases {
		if _, err := s.Handoff(req, now); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("case %d: want ErrInvalidArgument, got %v", i, err)
		}
	}
	if _, err := s.GetHandoff("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get missing handoff: want ErrNotFound, got %v", err)
	}
}

// TestHandoffAckResolveRace 并发“确认 + 解决 + 交接 + 计时推进”，
// 验证最终只收敛出一个结果，且没有事件失去负责方、没有重复意图。
func TestHandoffAckResolveRace(t *testing.T) {
	s := NewMemoryStore()
	now := time.Unix(1000, 0)
	if _, err := s.CreatePolicy(testPolicy("p1"), now); err != nil {
		t.Fatal(err)
	}
	const n = 24
	for i := 0; i < n; i++ {
		if _, err := s.CreateIncident(CreateIncidentRequest{
			RequestID:  fmt.Sprintf("c-%d", i),
			IncidentID: fmt.Sprintf("inc-%d", i),
			PolicyID:   "p1",
			OwnerGroup: "g1",
		}, now); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(3)
		go func() {
			defer wg.Done()
			_, _ = s.Acknowledge(AckRequest{RequestID: fmt.Sprintf("a-%d", i), IncidentID: fmt.Sprintf("inc-%d", i), AcknowledgedBy: "u"}, now.Add(time.Second))
		}()
		go func() {
			defer wg.Done()
			_, _ = s.Resolve(ResolveRequest{RequestID: fmt.Sprintf("r-%d", i), IncidentID: fmt.Sprintf("inc-%d", i), ResolvedBy: "u"}, now.Add(time.Second))
		}()
		go func() {
			defer wg.Done()
			_, _ = s.ProcessDue(now.Add(time.Hour))
		}()
	}
	// 交接与上述操作并发进行。
	_, handoffErr := s.Handoff(HandoffRequest{RequestID: "ho-race-req", HandoffID: "ho-race", FromGroup: "g1", ToGroup: "g2"}, now.Add(2*time.Second))
	wg.Wait()
	if handoffErr != nil {
		t.Fatalf("handoff: %v", handoffErr)
	}

	rec, err := s.GetHandoff("ho-race")
	if err != nil {
		t.Fatalf("get handoff: %v", err)
	}
	transferred := map[string]bool{}
	for _, id := range rec.IncidentIDs {
		transferred[id] = true
	}

	// 每个事件恰好收敛为一个合法状态，且始终有负责方。
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("inc-%d", i)
		inc, err := s.GetIncident(id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if inc.OwnerGroup == "" {
			t.Fatalf("%s lost owner", id)
		}
		// 冻结清单之外的事件不得被移交。
		if !transferred[id] && inc.OwnerGroup != "g1" {
			t.Fatalf("%s owner = %q but not in handoff list", id, inc.OwnerGroup)
		}
		// 已确认/已解决的事件不得出现在移交清单里。
		if transferred[id] && inc.Status != StatusFiring {
			// 允许“先移交后确认/解决”：此时状态为终态但曾在清单中，
			// 历史里交接必须先于确认/解决。
			hist, _ := s.History(id)
			hoIdx, termIdx := -1, -1
			for j, h := range hist {
				if h.Kind == "handoff" && hoIdx < 0 {
					hoIdx = j
				}
				if (h.Kind == "acknowledged" || h.Kind == "resolved") && termIdx < 0 {
					termIdx = j
				}
			}
			if hoIdx < 0 || termIdx < 0 || hoIdx > termIdx {
				t.Fatalf("%s: inconsistent history order: %+v", id, hist)
			}
		}
	}

	// 意图去重：同一 (事件, 步骤, 目标) 至多一条；pending 意图的负责方
	// 必须与事件当前负责方一致（任何时候只有一方在处理）。
	seen := map[string]int{}
	incidents := map[string]*Incident{}
	for _, inc := range s.ListIncidents() {
		incidents[inc.ID] = inc
	}
	for _, it := range s.ListOutbox("") {
		seen[fmt.Sprintf("%s/%d/%s", it.IncidentID, it.StepIndex, it.Target)]++
		if it.Status == OutboxPending && it.OwnerGroup != incidents[it.IncidentID].OwnerGroup {
			t.Fatalf("pending item %+v owner mismatch with incident owner %q", it, incidents[it.IncidentID].OwnerGroup)
		}
	}
	for key, cnt := range seen {
		if cnt > 1 {
			t.Fatalf("duplicate intent %s x%d", key, cnt)
		}
	}
}

// TestHandoffRestartResume 交接落盘后重启：负责人、交接单、改派后的
// pending 意图全部恢复，后续升级步骤由新班组继续，投递失败重试不丢。
func TestHandoffRestartResume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	now := time.Unix(1000, 0)

	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreatePolicy(testPolicy("p1"), now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateIncident(CreateIncidentRequest{
		RequestID: "c1", IncidentID: "inc-1", PolicyID: "p1", OwnerGroup: "g1",
	}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessDue(now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Handoff(HandoffRequest{RequestID: "ho-1", FromGroup: "g1", ToGroup: "g2"}, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}

	// 模拟进程重启：重新打开同一状态文件。
	s2, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	inc, err := s2.GetIncident("inc-1")
	if err != nil {
		t.Fatal(err)
	}
	if inc.OwnerGroup != "g2" {
		t.Fatalf("owner after restart = %q, want g2", inc.OwnerGroup)
	}
	rec, err := s2.GetHandoff("ho-1")
	if err != nil {
		t.Fatalf("handoff record lost after restart: %v", err)
	}
	if len(rec.IncidentIDs) != 1 || rec.IncidentIDs[0] != "inc-1" {
		t.Fatalf("restored handoff list = %v", rec.IncidentIDs)
	}
	// 重启后交接请求幂等表仍在：重放不重复通知。
	outboxBefore := len(s2.ListOutbox(""))
	if _, err := s2.Handoff(HandoffRequest{RequestID: "ho-1", FromGroup: "g1", ToGroup: "g2"}, now.Add(3*time.Minute)); err != nil {
		t.Fatalf("replay after restart: %v", err)
	}
	if got := len(s2.ListOutbox("")); got != outboxBefore {
		t.Fatalf("outbox grew after restart replay: %d -> %d", outboxBefore, got)
	}

	// pending 意图归新班组：两条改派的升级意图（alice、bob）+ 一条交接通知。
	pending := s2.PendingOutboxFor("g2")
	if len(pending) != 3 {
		t.Fatalf("pending for g2 after restart = %d, want 3", len(pending))
	}
	if got := s2.PendingOutboxFor("g1"); len(got) != 0 {
		t.Fatalf("pending for g1 after restart = %d, want 0", len(got))
	}

	// 后续升级步骤由新班组接手。
	if _, err := s2.ProcessDue(now.Add(3 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	for _, it := range s2.PendingOutbox() {
		if it.OwnerGroup != "g2" {
			t.Fatalf("pending item after restart owned by %q, want g2: %+v", it.OwnerGroup, it)
		}
	}

	// 投递失败保持 pending 并可重试，成功后标记 delivered。
	if err := s2.MarkDelivered(pending[0].ID, now.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	s3, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(s3.PendingOutbox()); got != len(s2.PendingOutbox()) {
		t.Fatalf("pending count changed across restart: %d vs %d", got, len(s2.PendingOutbox()))
	}
}

// TestServiceGroupDispatch 两个班组的 Service 各只投递自己名下的意图；
// 交接后未派发的意图只由新班组的 Service 继续处理。
func TestServiceGroupDispatch(t *testing.T) {
	s := NewMemoryStore()
	now := time.Unix(1000, 0)
	if _, err := s.CreatePolicy(PolicyInput{
		ID: "p", Name: "p",
		Steps: []Step{
			{WaitBefore: 10 * time.Millisecond, Targets: []string{"alice"}},
			{WaitBefore: 10 * time.Millisecond, Targets: []string{"bob"}},
		},
	}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateIncident(CreateIncidentRequest{
		RequestID: "c1", IncidentID: "inc-1", PolicyID: "p", OwnerGroup: "g1",
	}, now); err != nil {
		t.Fatal(err)
	}

	d1 := &recordingDispatcher{}
	d2 := &recordingDispatcher{}
	svc1 := NewService(s, d1, Config{TickInterval: 5 * time.Millisecond, DispatchInterval: 5 * time.Millisecond, Group: "g1"})
	svc2 := NewService(s, d2, Config{TickInterval: 5 * time.Millisecond, DispatchInterval: 5 * time.Millisecond, Group: "g2"})
	svc1.Start()
	svc2.Start()
	defer svc1.Stop()
	defer svc2.Stop()

	waitFor := func(want int, what string, get func() int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if get() >= want {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("timeout waiting for %s: got %d, want >= %d", what, get(), want)
	}

	// g1 的 Service 投递第 0 步。
	waitFor(1, "g1 dispatch", func() int { return len(d1.delivered()) })
	if got := len(d2.delivered()); got != 0 {
		t.Fatalf("g2 dispatched %d items before handoff, want 0", got)
	}

	// 交接：剩余升级与交接通知都归 g2。
	if _, err := s.Handoff(HandoffRequest{RequestID: "ho-1", FromGroup: "g1", ToGroup: "g2"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	// g2 应收到交接通知以及后续升级步骤；g1 不再收到任何新意图。
	waitFor(2, "g2 dispatch", func() int { return len(d2.delivered()) })
	kinds := map[OutboxKind]int{}
	s.mu.Lock()
	for _, item := range s.snap.Outbox {
		if item.Status == OutboxDelivered && item.OwnerGroup == "g2" {
			kinds[item.Kind]++
		}
	}
	s.mu.Unlock()
	if kinds[OutboxKindHandoff] != 1 {
		t.Fatalf("g2 delivered handoff notices = %d, want 1", kinds[OutboxKindHandoff])
	}
	if got := len(d1.delivered()); got != 1 {
		t.Fatalf("g1 dispatched %d items total, want exactly 1 (pre-handoff)", got)
	}
}

func TestListHandoffsSorted(t *testing.T) {
	s, now := newHandoffStore(t)
	if _, err := s.Handoff(HandoffRequest{RequestID: "h1", FromGroup: "g1", ToGroup: "g2"}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Handoff(HandoffRequest{RequestID: "h2", FromGroup: "g2", ToGroup: "g3"}, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	recs := s.ListHandoffs()
	if len(recs) != 2 || recs[0].RequestID != "h1" || recs[1].RequestID != "h2" {
		t.Fatalf("handoffs = %+v", recs)
	}
	// 链式交接：事件最终归 g3。
	inc, _ := s.GetIncident("inc-firing")
	if inc.OwnerGroup != "g3" {
		t.Fatalf("owner after chained handoffs = %q, want g3", inc.OwnerGroup)
	}
	// 每个交接单各产生一条交接通知，目标分别是 g2、g3。
	var targets []string
	for _, it := range s.ListOutbox("inc-firing") {
		if it.Kind == OutboxKindHandoff {
			targets = append(targets, it.Target)
		}
	}
	sort.Strings(targets)
	if len(targets) != 2 || targets[0] != "g2" || targets[1] != "g3" {
		t.Fatalf("handoff notice targets = %v, want [g2 g3]", targets)
	}
}
