package incidentescalation

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// firingIncident 创建一个归属 team、处于 firing 的事件（使用测试策略 p1）。
func firingIncident(t *testing.T, s *Store, reqID, id, team string, at time.Time) *Incident {
	t.Helper()
	inc, err := s.CreateIncident(CreateIncidentRequest{
		RequestID: reqID, IncidentID: id, PolicyID: "p1", AssignedTeam: team,
	}, at)
	if err != nil {
		t.Fatalf("create incident %s: %v", id, err)
	}
	return inc
}

func TestHandoffFreezesListAndReassigns(t *testing.T) {
	s := newTestStore(t)
	t0 := time.Unix(10_000, 0)

	firing := firingIncident(t, s, "r-fire", "i-fire", "team-a", t0)
	acked := firingIncident(t, s, "r-ack", "i-ack", "team-a", t0.Add(time.Second))
	resolved := firingIncident(t, s, "r-res", "i-res", "team-a", t0.Add(2*time.Second))
	other := firingIncident(t, s, "r-other", "i-other", "team-c", t0.Add(3*time.Second))

	if _, err := s.Acknowledge(AckRequest{RequestID: "a1", IncidentID: acked.ID, AcknowledgedBy: "bob"}, t0.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Resolve(ResolveRequest{RequestID: "x1", IncidentID: resolved.ID, ResolvedBy: "bob"}, t0.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}

	// firing 事件的 step0 先触发：alice、bob 两条升级通知，归属 team-a。
	if n, err := s.ProcessDue(t0.Add(2 * time.Minute)); err != nil || n != 2 {
		t.Fatalf("process due: n=%d err=%v, want 2", n, err)
	}
	// alice 已成功投递；bob 仍 pending。
	items := s.ListOutbox(firing.ID)
	if len(items) != 2 {
		t.Fatalf("step0 outbox = %d items", len(items))
	}
	var aliceID, bobID int64
	for _, it := range items {
		if it.Target == "alice" {
			aliceID = it.ID
		}
		if it.Target == "bob" {
			bobID = it.ID
			if it.OwnerTeam != "team-a" {
				t.Fatalf("pending escalation owner = %q, want team-a", it.OwnerTeam)
			}
		}
	}
	if err := s.MarkDelivered(aliceID, t0.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}

	// 交接生效。
	handoffAt := t0.Add(4 * time.Minute)
	h, err := s.Handoff(HandoffRequest{
		RequestID: "h1", FromTeam: "team-a", ToTeam: "team-b",
	}, handoffAt)
	if err != nil {
		t.Fatalf("handoff: %v", err)
	}

	// 1) 冻结清单只含当时 firing 且属于 team-a 的事件。
	if len(h.IncidentIDs) != 1 || h.IncidentIDs[0] != firing.ID {
		t.Fatalf("frozen list = %v, want only [%s]", h.IncidentIDs, firing.ID)
	}

	got, _ := s.GetIncident(firing.ID)
	if got.AssignedTeam != "team-b" {
		t.Fatalf("firing incident assigned = %q, want team-b", got.AssignedTeam)
	}
	if len(got.Handoffs) != 1 || got.Handoffs[0].HandoffID != h.ID ||
		got.Handoffs[0].FromTeam != "team-a" || got.Handoffs[0].ToTeam != "team-b" {
		t.Fatalf("handoff trail wrong: %+v", got.Handoffs)
	}
	// 已确认 / 已解决 / 其它班组事件的负责人不变，也没有交接轨迹。
	for id, wantTeam := range map[string]string{acked.ID: "team-a", resolved.ID: "team-a", other.ID: "team-c"} {
		inc, _ := s.GetIncident(id)
		if inc.AssignedTeam != wantTeam || len(inc.Handoffs) != 0 {
			t.Fatalf("%s must not be handed over: team=%s handoffs=%d", id, inc.AssignedTeam, len(inc.Handoffs))
		}
	}

	// 2) 已投递记录保留原责任方；pending 的 bob 原子转交给 team-b。
	var aliceAfter, bobAfter *OutboxItem
	for _, it := range s.ListOutbox("") {
		switch it.ID {
		case aliceID:
			aliceAfter = it
		case bobID:
			bobAfter = it
		}
	}
	if aliceAfter.OwnerTeam != "team-a" || aliceAfter.Status != OutboxDelivered {
		t.Fatalf("delivered record rewritten: %+v", aliceAfter)
	}
	if bobAfter.OwnerTeam != "team-b" || bobAfter.Status != OutboxPending {
		t.Fatalf("pending intent not transferred: %+v", bobAfter)
	}

	// 3) 产生一条发给 team-b 的交接通知，归属 team-b。
	hNotes := []*OutboxItem{}
	for _, it := range s.ListOutbox(firing.ID) {
		if it.Kind == KindHandoff {
			hNotes = append(hNotes, it)
		}
	}
	if len(hNotes) != 1 || hNotes[0].Target != "team-b" || hNotes[0].OwnerTeam != "team-b" {
		t.Fatalf("handoff notification wrong: %+v", hNotes)
	}

	// 定时器原样保留（下一步仍是 step1）。
	tmr, ok := s.Timer(firing.ID)
	if !ok || tmr.StepIndex != 1 {
		t.Fatalf("timer lost or reset by handoff: %+v ok=%v", tmr, ok)
	}

	// 交接后升级继续：step1 触发的新通知归属新班组 team-b。
	// 注意同一次扫描里其它仍 firing 的事件也可能推进，因此不校验推进数，
	// 只看本事件是否推进到 step1。
	if _, err := s.ProcessDue(handoffAt.Add(3 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	advanced, _ := s.GetIncident(firing.ID)
	if advanced.NextStepIndex != 2 {
		t.Fatalf("post-handoff step index = %d, want 2", advanced.NextStepIndex)
	}
	for _, it := range s.ListOutbox(firing.ID) {
		if it.Kind == KindEscalation && it.StepIndex == 1 && it.OwnerTeam != "team-b" {
			t.Fatalf("post-handoff escalation owner = %q, want team-b", it.OwnerTeam)
		}
	}
}

func TestHandoffIdempotentAndNoDoubleNotification(t *testing.T) {
	s := newTestStore(t)
	t0 := time.Unix(10_000, 0)
	inc := firingIncident(t, s, "r1", "i1", "team-a", t0)

	req := HandoffRequest{RequestID: "h1", HandoffID: "ho1", FromTeam: "team-a", ToTeam: "team-b"}
	first, err := s.Handoff(req, t0.Add(time.Minute))
	if err != nil {
		t.Fatalf("first handoff: %v", err)
	}
	// 同号同内容重放：返回同一交接记录。
	replay, err := s.Handoff(req, t0.Add(2*time.Minute))
	if err != nil || replay.ID != first.ID {
		t.Fatalf("handoff replay: %v %+v", err, replay)
	}

	got, _ := s.GetIncident(inc.ID)
	if len(got.Handoffs) != 1 || got.AssignedTeam != "team-b" {
		t.Fatalf("replay changed state: handoffs=%d team=%s", len(got.Handoffs), got.AssignedTeam)
	}
	if notes := len(s.ListOutbox(inc.ID)); notes != 1 {
		t.Fatalf("replay produced %d outbox items, want exactly 1 handoff note", notes)
	}

	// 新请求号再来一次相同方向的交接：事件已不属于 team-a，冻结清单为空，
	// 不换人、不通知。
	second, err := s.Handoff(HandoffRequest{RequestID: "h2", FromTeam: "team-a", ToTeam: "team-b"}, t0.Add(3*time.Minute))
	if err != nil {
		t.Fatalf("second handoff: %v", err)
	}
	if len(second.IncidentIDs) != 0 {
		t.Fatalf("second handoff must freeze empty list, got %v", second.IncidentIDs)
	}
	got, _ = s.GetIncident(inc.ID)
	if len(got.Handoffs) != 1 || got.AssignedTeam != "team-b" {
		t.Fatalf("duplicate handoff changed owner/trail: %+v", got)
	}
	if notes := len(s.ListOutbox(inc.ID)); notes != 1 {
		t.Fatalf("duplicate handoff produced extra notification: %d", notes)
	}

	// 请求号跨操作类型复用 → 冲突。
	if _, err := s.Resolve(ResolveRequest{RequestID: "h1", IncidentID: inc.ID, ResolvedBy: "u"}, t0); !errors.Is(err, ErrConflict) {
		t.Fatalf("request id reused across operation: want ErrConflict, got %v", err)
	}
	// 入参校验。
	if _, err := s.Handoff(HandoffRequest{RequestID: "h3", FromTeam: " ", ToTeam: "b"}, t0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("blank team: want ErrInvalidArgument, got %v", err)
	}
	if _, err := s.Handoff(HandoffRequest{RequestID: "h4", FromTeam: "a", ToTeam: "a"}, t0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("same team: want ErrInvalidArgument, got %v", err)
	}
	// 显式交接 ID 冲突。
	if _, err := s.Handoff(HandoffRequest{RequestID: "h5", HandoffID: "ho1", FromTeam: "a", ToTeam: "c"}, t0); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("dup handoff id: want ErrAlreadyExists, got %v", err)
	}
	if _, err := s.GetHandoff("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get missing handoff: want ErrNotFound, got %v", err)
	}
}

func TestHandoffChainFollowsCurrentOwner(t *testing.T) {
	s := newTestStore(t)
	t0 := time.Unix(10_000, 0)
	inc := firingIncident(t, s, "r1", "i1", "team-a", t0)

	// step0 在 team-a 任期触发。
	if n, _ := s.ProcessDue(t0.Add(time.Minute)); n != 1 {
		t.Fatal("step0")
	}
	if _, err := s.Handoff(HandoffRequest{RequestID: "h1", FromTeam: "team-a", ToTeam: "team-b"}, t0.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	// step1 在 team-b 任期触发：新通知 + 此前 pending 均归 team-b。
	if n, _ := s.ProcessDue(t0.Add(4 * time.Minute)); n != 1 {
		t.Fatal("step1")
	}
	if _, err := s.Handoff(HandoffRequest{RequestID: "h2", FromTeam: "team-b", ToTeam: "team-c"}, t0.Add(5*time.Minute)); err != nil {
		t.Fatal(err)
	}

	got, _ := s.GetIncident(inc.ID)
	if got.AssignedTeam != "team-c" || len(got.Handoffs) != 2 {
		t.Fatalf("chain end state wrong: team=%s trail=%d", got.AssignedTeam, len(got.Handoffs))
	}
	// 所有 pending（含交接通知）都应归当前班组 team-c；无一条遗留旧班组。
	for _, it := range s.PendingOutbox() {
		if it.OwnerTeam != "team-c" {
			t.Fatalf("pending item %d still owned by %q", it.ID, it.OwnerTeam)
		}
	}
	// team-a 与 team-b 都捞不到任何待投递意图。
	if len(s.PendingOutboxForTeam("team-a")) != 0 || len(s.PendingOutboxForTeam("team-b")) != 0 {
		t.Fatal("old teams must not see pending intents after chain handoff")
	}
	pendingC := s.PendingOutboxForTeam("team-c")
	if len(pendingC) == 0 {
		t.Fatal("team-c must inherit all pending intents")
	}
	// 每次交接各一条交接通知，目标分别为继任班组。
	noteTargets := map[string]int{}
	for _, it := range s.ListOutbox(inc.ID) {
		if it.Kind == KindHandoff {
			noteTargets[it.Target]++
		}
	}
	if noteTargets["team-b"] != 1 || noteTargets["team-c"] != 1 {
		t.Fatalf("handoff notes = %v, want one for team-b and one for team-c", noteTargets)
	}
}

func TestConcurrentHandoffAckResolveTimerConverges(t *testing.T) {
	s := NewMemoryStore()
	now := time.Unix(1000, 0)
	if _, err := s.CreatePolicy(PolicyInput{
		ID: "p", Name: "p",
		Steps: []Step{{Targets: []string{"a"}}, {Targets: []string{"b"}}, {Targets: []string{"c"}}},
	}, now); err != nil {
		t.Fatal(err)
	}
	const incidentCount = 8
	ids := make([]string, incidentCount)
	for i := range ids {
		ids[i] = fmt.Sprintf("inc-%d", i)
		inc, err := s.CreateIncident(CreateIncidentRequest{
			RequestID: fmt.Sprintf("create-%d", i), IncidentID: ids[i],
			PolicyID: "p", AssignedTeam: "team-a",
		}, now)
		if err != nil {
			t.Fatal(err)
		}
		_ = inc
	}

	const workers = 16
	var wg sync.WaitGroup
	raceNow := now.Add(time.Second)
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(4)
		go func() {
			defer wg.Done()
			_, _ = s.ProcessDue(raceNow.Add(time.Duration(w) * time.Microsecond))
		}()
		go func() {
			defer wg.Done()
			id := ids[w%incidentCount]
			_, _ = s.Acknowledge(AckRequest{
				RequestID: fmt.Sprintf("ack-%d", w), IncidentID: id, AcknowledgedBy: fmt.Sprintf("u%d", w),
			}, raceNow)
		}()
		go func() {
			defer wg.Done()
			id := ids[(w+3)%incidentCount]
			_, _ = s.Resolve(ResolveRequest{
				RequestID: fmt.Sprintf("res-%d", w), IncidentID: id, ResolvedBy: fmt.Sprintf("u%d", w),
			}, raceNow)
		}()
		go func() {
			defer wg.Done()
			// 同一交接请求号并发提交：只能形成一个交接结果。
			_, _ = s.Handoff(HandoffRequest{
				RequestID: "handoff-shared", FromTeam: "team-a", ToTeam: "team-b",
			}, raceNow)
		}()
	}
	wg.Wait()

	handoffs := s.ListHandoffs()
	if len(handoffs) != 1 {
		t.Fatalf("concurrent same handoff request produced %d handoffs, want 1", len(handoffs))
	}
	h := handoffs[0]

	frozenSet := map[string]bool{}
	for _, fid := range h.IncidentIDs {
		frozenSet[fid] = true
	}

	for _, id := range ids {
		inc, _ := s.GetIncident(id)
		// 并发下两种串行顺序都合法，但必须恰好落定其中一种：
		//  A) 交接先赢：事件进冻结清单、归属 team-b、带一条交接轨迹；
		//     随后到达的确认/解决只是“由新班组负责的事件”状态推进，
		//     其轨迹仍保留（已确认/已解决但归属 team-b）。
		//  B) 确认/解决先赢：事件不进冻结清单、归属保持 team-a、无交接轨迹。
		wasFrozen := frozenSet[id]
		trailed := len(inc.Handoffs) == 1
		switch {
		case inc.AssignedTeam == "team-b" && wasFrozen && trailed:
			// 顺序 A：交接先落盘。状态可以是 firing/acknowledged/resolved 任一，
			// 但状态字段必须自洽。
			if inc.Status == StatusAcknowledged && inc.AcknowledgedAt.IsZero() {
				t.Fatalf("%s acknowledged without timestamp", id)
			}
			if inc.Status == StatusResolved && inc.ResolvedAt.IsZero() {
				t.Fatalf("%s resolved without timestamp", id)
			}
		case inc.AssignedTeam == "team-a" && !wasFrozen && !trailed:
			// 顺序 B：确认/解决先落盘，交接跳过了它；此时不能仍是 firing
			//（唯一的 firing→保留 team-a 路径是交接未发生，而交接确实发生了）。
			if inc.Status == StatusFiring {
				t.Fatalf("%s firing and unhanded although handoff ran", id)
			}
		default:
			t.Fatalf("%s inconsistent convergence: status=%s team=%s frozen=%v trail=%d",
				id, inc.Status, inc.AssignedTeam, wasFrozen, len(inc.Handoffs))
		}
	}

	// 任何 (事件,类型,步骤,目标) 意图最多一条。
	counts := map[string]int{}
	for _, it := range s.ListOutbox("") {
		counts[fmt.Sprintf("%s/%s/%d/%s", it.IncidentID, it.Kind, it.StepIndex, it.Target)]++
	}
	for k, c := range counts {
		if c > 1 {
			t.Fatalf("intent %s appeared %d times", k, c)
		}
	}
}

func TestHandoffRestartResumesPersistedState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	t0 := time.Unix(50_000, 0)

	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreatePolicy(testPolicy("p1"), t0); err != nil {
		t.Fatal(err)
	}
	inc, err := s.CreateIncident(CreateIncidentRequest{
		RequestID: "r1", PolicyID: "p1", AssignedTeam: "team-a",
	}, t0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessDue(t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Handoff(HandoffRequest{RequestID: "h1", FromTeam: "team-a", ToTeam: "team-b"}, t0.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}

	// 重启：交接结果、冻结清单、pending 归属、交接通知全部恢复。
	s2, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, err := s2.GetIncident(inc.ID)
	if err != nil || got.AssignedTeam != "team-b" || len(got.Handoffs) != 1 {
		t.Fatalf("incident ownership not recovered: %v %+v", err, got)
	}
	hs := s2.ListHandoffs()
	if len(hs) != 1 || len(hs[0].IncidentIDs) != 1 || hs[0].IncidentIDs[0] != inc.ID {
		t.Fatalf("handoff record not recovered: %+v", hs)
	}
	if len(s2.PendingOutboxForTeam("team-a")) != 0 {
		t.Fatal("old team must not see pending intents after restart")
	}
	pendingB := s2.PendingOutboxForTeam("team-b")
	if len(pendingB) < 2 { // step0 的两条未投递升级意图 + 交接通知
		t.Fatalf("team-b pending not recovered: %d items", len(pendingB))
	}
	hasNote := false
	for _, it := range pendingB {
		if it.Kind == KindHandoff && it.Target == "team-b" {
			hasNote = true
		}
	}
	if !hasNote {
		t.Fatal("handoff notification not recovered for team-b")
	}

	// 重启后升级定时器继续，新通知仍归 team-b；交接请求号幂等表同样恢复。
	if n, err := s2.ProcessDue(t0.Add(5 * time.Minute)); err != nil || n != 1 {
		t.Fatalf("step1 after restart: n=%d err=%v", n, err)
	}
	for _, it := range s2.ListOutbox(inc.ID) {
		if it.Kind == KindEscalation && it.StepIndex == 1 && it.OwnerTeam != "team-b" {
			t.Fatalf("post-restart escalation owner = %q", it.OwnerTeam)
		}
	}
	if _, err := s2.Handoff(HandoffRequest{RequestID: "h1", FromTeam: "team-a", ToTeam: "team-b"}, t0); err != nil {
		t.Fatalf("handoff idempotency table lost across restart: %v", err)
	}
	if len(s2.ListHandoffs()) != 1 {
		t.Fatal("replayed handoff after restart created a second record")
	}
}

func TestServiceDispatchOwnershipSwitchesAtHandoff(t *testing.T) {
	s := newTestStore(t)
	t0 := time.Unix(10_000, 0)
	inc := firingIncident(t, s, "r1", "i1", "team-a", t0)

	// team-a 的派发器始终故障：升级意图保持 pending 直到交接。
	dispA := &recordingDispatcher{failing: map[string]bool{"alice": true, "bob": true}}
	svcA := NewService(s, dispA, Config{
		Team: "team-a", TickInterval: 5 * time.Millisecond, DispatchInterval: 5 * time.Millisecond,
	})
	dispB := &recordingDispatcher{}
	svcB := NewService(s, dispB, Config{
		Team: "team-b", TickInterval: 5 * time.Millisecond, DispatchInterval: 5 * time.Millisecond,
	})
	svcA.Start()

	// 等 step0 意图产生并被 team-a 反复重试。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(s.ListOutbox(inc.ID)) == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if len(s.ListOutbox(inc.ID)) != 2 {
		t.Fatalf("step0 intents = %d", len(s.ListOutbox(inc.ID)))
	}

	// 交接：pending 意图转投 team-b，同时产生交接通知。
	if _, err := s.Handoff(HandoffRequest{RequestID: "h1", FromTeam: "team-a", ToTeam: "team-b"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	svcB.Start()
	defer svcA.Stop()
	defer svcB.Stop()

	// team-b 接手投递：两条升级意图 + 一条交接通知，且都被标记 delivered。
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(s.PendingOutboxForTeam("team-b")) > 0 {
		time.Sleep(5 * time.Millisecond)
	}
	gotB := dispB.delivered()
	if len(gotB) != 3 {
		t.Fatalf("team-b delivered %v, want 2 escalations + 1 handoff note", gotB)
	}
	last := gotB[len(gotB)-1]
	// 升级意图按 outbox ID 在前，交接通知在交接时才追加。
	if last != "team-b" {
		t.Fatalf("handoff note not dispatched to new team: delivered=%v", gotB)
	}
	// team-a 从未成功投递任何一条（故障 + 交接后无权再取件）。
	if gotA := dispA.delivered(); len(gotA) != 0 {
		t.Fatalf("old team dispatched after handoff: %v", gotA)
	}
	if len(s.PendingOutbox()) != 0 {
		t.Fatalf("pending remains: %d", len(s.PendingOutbox()))
	}
}
