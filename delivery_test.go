package incidentescalation

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// setupDeliveryIncident 创建单步单目标策略与事件，并触发第 0 步产生一条意图。
func setupDeliveryIncident(t *testing.T, s *Store, t0 time.Time) (*Incident, *OutboxItem) {
	t.Helper()
	if _, err := s.CreatePolicy(PolicyInput{
		ID: "dp", Name: "dp",
		Steps: []Step{{WaitBefore: time.Minute, Targets: []string{"alice"}}},
	}, t0); err != nil {
		t.Fatal(err)
	}
	inc, err := s.CreateIncident(CreateIncidentRequest{RequestID: "r1", PolicyID: "dp"}, t0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProcessDue(t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	items := s.ListOutbox(inc.ID)
	if len(items) != 1 {
		t.Fatalf("outbox = %d, want 1", len(items))
	}
	return inc, items[0]
}

func TestReceiptOutOfOrderAndStaleVersion(t *testing.T) {
	s := NewMemoryStore()
	t0 := time.Unix(10_000, 0)
	inc, item := setupDeliveryIncident(t, s, t0)

	// 第一次尝试：v1 交给渠道，渠道明确失败 → 回到 pending 等待重试。
	if err := s.MarkDispatched(item.ID, t0); err != nil {
		t.Fatal(err)
	}
	res, err := s.ReportReceipt(ReceiptRequest{
		ReceiptID: "rc-1", IncidentID: inc.ID, StepIndex: 0, Target: "alice",
		IntentVersion: 1, Outcome: ReceiptFailed, Error: "sms gateway 503",
	}, t0.Add(time.Second))
	if err != nil || !res.Applied || res.Status != DeliveryPending {
		t.Fatalf("failure receipt: res=%+v err=%v", res, err)
	}

	// 第二次尝试：版本升到 v2。
	if err := s.MarkDispatched(item.ID, t0.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}

	// 乱序：针对 v1 的迟到成功回执不能改回 v2 的状态。
	res, err = s.ReportReceipt(ReceiptRequest{
		ReceiptID: "rc-late", IncidentID: inc.ID, StepIndex: 0, Target: "alice",
		IntentVersion: 1, Outcome: ReceiptAccepted,
	}, t0.Add(3*time.Second))
	if !errors.Is(err, ErrStaleReceipt) {
		t.Fatalf("stale receipt: want ErrStaleReceipt, got %v", err)
	}
	if res.Applied || res.Status != DeliveryDispatched {
		t.Fatalf("stale receipt changed state: %+v", res)
	}

	// v2 的成功回执生效 → acknowledged。
	res, err = s.ReportReceipt(ReceiptRequest{
		ReceiptID: "rc-2", IncidentID: inc.ID, StepIndex: 0, Target: "alice",
		IntentVersion: 2, Outcome: ReceiptAccepted,
	}, t0.Add(4*time.Second))
	if err != nil || !res.Applied || res.Status != DeliveryAcknowledged {
		t.Fatalf("accept receipt: res=%+v err=%v", res, err)
	}

	// 成功之后到达的失败回执不能降回失败。
	res, err = s.ReportReceipt(ReceiptRequest{
		ReceiptID: "rc-3", IncidentID: inc.ID, StepIndex: 0, Target: "alice",
		IntentVersion: 2, Outcome: ReceiptFailed, Error: "late failure",
	}, t0.Add(5*time.Second))
	if err != nil || res.Applied || res.Status != DeliveryAcknowledged {
		t.Fatalf("late failure must not downgrade: res=%+v err=%v", res, err)
	}

	got := s.ListOutbox(inc.ID)[0]
	if got.Status != DeliveryAcknowledged || got.Attempts != 2 || got.AckedAt.IsZero() {
		t.Fatalf("final item = %+v", got)
	}
}

func TestRetryLimitReachesTerminalFailure(t *testing.T) {
	s := NewMemoryStore()
	if err := s.SetRetryPolicy(RetryPolicy{MaxAttempts: 2}); err != nil {
		t.Fatal(err)
	}
	t0 := time.Unix(10_000, 0)
	inc, item := setupDeliveryIncident(t, s, t0)

	// 第 1 次失败：回到 pending，等待重试，不会生成第二条意图。
	if err := s.MarkAttemptFailed(item.ID, "dial timeout", t0); err != nil {
		t.Fatal(err)
	}
	if got := s.ListOutbox(inc.ID); len(got) != 1 || got[0].Status != DeliveryPending {
		t.Fatalf("after 1st failure: %+v", got)
	}
	// 第 2 次失败：达到上限 → failed 终态。
	if err := s.MarkAttemptFailed(item.ID, "dial timeout", t0.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	got := s.ListOutbox(inc.ID)[0]
	if got.Status != DeliveryFailed || got.Attempts != 2 || got.LastError != "dial timeout" {
		t.Fatalf("terminal failure: %+v", got)
	}
	// 终态后不能再投递或重试。
	if err := s.MarkDispatched(item.ID, t0.Add(2*time.Second)); !errors.Is(err, ErrDeliveryClosed) {
		t.Fatalf("dispatch terminal: want ErrDeliveryClosed, got %v", err)
	}
	if err := s.MarkAttemptFailed(item.ID, "x", t0.Add(2*time.Second)); !errors.Is(err, ErrDeliveryClosed) {
		t.Fatalf("fail terminal: want ErrDeliveryClosed, got %v", err)
	}
	// 历史保留了每次状态变化的原因。
	var kinds []string
	for _, h := range mustHistory(t, s, inc.ID) {
		kinds = append(kinds, h.Kind)
	}
	want := []string{"created", "step_fired", "delivery_retry_scheduled", "delivery_failed"}
	for i, k := range want {
		if i >= len(kinds) || kinds[i] != k {
			t.Fatalf("history kinds = %v, want prefix %v", kinds, want)
		}
	}
}

func TestAckStopsRetryAndLateReceiptIgnored(t *testing.T) {
	s := NewMemoryStore()
	t0 := time.Unix(10_000, 0)
	inc, item := setupDeliveryIncident(t, s, t0)

	// 一次失败尝试后处于待重试状态。
	if err := s.MarkAttemptFailed(item.ID, "busy", t0); err != nil {
		t.Fatal(err)
	}
	// 确认事件：与回执/重试竞态时以事件状态为准，待重试投递被停止并留下原因。
	if _, err := s.Acknowledge(AckRequest{RequestID: "a1", IncidentID: inc.ID, AcknowledgedBy: "oncall"}, t0.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	got := s.ListOutbox(inc.ID)[0]
	if got.Status != DeliveryStopped || got.StopReason != "incident_acknowledged" {
		t.Fatalf("stopped item = %+v", got)
	}
	// 确认后到达的成功回执不再生效。
	res, err := s.ReportReceipt(ReceiptRequest{
		ReceiptID: "rc-after-ack", IncidentID: inc.ID, StepIndex: 0, Target: "alice",
		IntentVersion: got.Version, Outcome: ReceiptAccepted,
	}, t0.Add(2*time.Second))
	if err != nil || res.Applied || res.Status != DeliveryStopped {
		t.Fatalf("receipt after ack: res=%+v err=%v", res, err)
	}
	// 待重试集合为空：重启扫描也不会再捞起它。
	if due := s.DueOutbox(t0.Add(time.Hour)); len(due) != 0 {
		t.Fatalf("due after ack = %d", len(due))
	}
}

func TestRestartRecoversPendingRetries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	t0 := time.Unix(10_000, 0)

	s1, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	inc, item := setupDeliveryIncident(t, s1, t0)
	if err := s1.SetRetryPolicy(RetryPolicy{MaxAttempts: 5, Backoff: time.Minute}); err != nil {
		t.Fatal(err)
	}
	// 失败一次并安排 1 分钟后的重试。
	if err := s1.MarkAttemptFailed(item.ID, "connection reset", t0); err != nil {
		t.Fatal(err)
	}

	// 模拟进程重启：重新打开同一文件。
	s2, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	// 退避期内不重试，到期后待重试意图恢复可见。
	if due := s2.DueOutbox(t0.Add(30 * time.Second)); len(due) != 0 {
		t.Fatalf("due during backoff = %d", len(due))
	}
	due := s2.DueOutbox(t0.Add(2 * time.Minute))
	if len(due) != 1 || due[0].ID != item.ID || due[0].Attempts != 1 {
		t.Fatalf("due after restart = %+v", due)
	}
	// 重启后继续投递并收到回执。
	if err := s2.MarkDispatched(item.ID, t0.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	res, err := s2.ReportReceipt(ReceiptRequest{
		ReceiptID: "rc-restart", IncidentID: inc.ID, StepIndex: 0, Target: "alice",
		IntentVersion: 1, Outcome: ReceiptAccepted,
	}, t0.Add(3*time.Minute))
	if err != nil || !res.Applied || res.Status != DeliveryAcknowledged {
		t.Fatalf("receipt after restart: res=%+v err=%v", res, err)
	}

	// 再次重启：终态与回执记录都还在。
	s3, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	got := s3.ListOutbox(inc.ID)[0]
	if got.Status != DeliveryAcknowledged || got.AckedAt.IsZero() {
		t.Fatalf("state lost across restart: %+v", got)
	}
	// 回执重放仍返回第一次的结果。
	res, err = s3.ReportReceipt(ReceiptRequest{
		ReceiptID: "rc-restart", IncidentID: inc.ID, StepIndex: 0, Target: "alice",
		IntentVersion: 1, Outcome: ReceiptAccepted,
	}, t0.Add(4*time.Minute))
	if err != nil || !res.Applied || res.Status != DeliveryAcknowledged {
		t.Fatalf("replay after restart: res=%+v err=%v", res, err)
	}
}

func TestDuplicateReceiptReplayAndConflict(t *testing.T) {
	s := NewMemoryStore()
	t0 := time.Unix(10_000, 0)
	inc, item := setupDeliveryIncident(t, s, t0)
	if err := s.MarkDispatched(item.ID, t0); err != nil {
		t.Fatal(err)
	}
	req := ReceiptRequest{
		ReceiptID: "rc-dup", IncidentID: inc.ID, StepIndex: 0, Target: "alice",
		IntentVersion: 1, Outcome: ReceiptAccepted,
	}
	first, err := s.ReportReceipt(req, t0.Add(time.Second))
	if err != nil || !first.Applied {
		t.Fatalf("first: %+v err=%v", first, err)
	}
	// 相同回执重放：返回第一次处理结果，不重复追加状态变化。
	again, err := s.ReportReceipt(req, t0.Add(time.Hour))
	if err != nil || again != first {
		t.Fatalf("replay = %+v err=%v, want %+v", again, err, first)
	}
	// 同号异内容：冲突。
	bad := req
	bad.Outcome = ReceiptFailed
	if _, err := s.ReportReceipt(bad, t0.Add(2*time.Hour)); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting receipt: want ErrConflict, got %v", err)
	}
	// 重放没有产生额外的历史条目。
	var acked int
	for _, h := range mustHistory(t, s, inc.ID) {
		if h.Kind == "delivery_acknowledged" {
			acked++
		}
	}
	if acked != 1 {
		t.Fatalf("delivery_acknowledged history entries = %d, want 1", acked)
	}
}

func TestReceiptTimeoutRequeuesDispatch(t *testing.T) {
	s := NewMemoryStore()
	if err := s.SetRetryPolicy(RetryPolicy{MaxAttempts: 2, ReceiptTimeout: time.Minute}); err != nil {
		t.Fatal(err)
	}
	t0 := time.Unix(10_000, 0)
	inc, item := setupDeliveryIncident(t, s, t0)
	if err := s.MarkDispatched(item.ID, t0); err != nil {
		t.Fatal(err)
	}
	// 回执超时前不重投。
	if n, _ := s.RequeueTimedOutDispatches(t0.Add(30 * time.Second)); n != 0 {
		t.Fatalf("requeue before timeout = %d", n)
	}
	// 超时后重新入队，可再次尝试。
	if n, _ := s.RequeueTimedOutDispatches(t0.Add(2 * time.Minute)); n != 1 {
		t.Fatalf("requeue after timeout = %d", n)
	}
	got := s.ListOutbox(inc.ID)[0]
	if got.Status != DeliveryPending || got.LastError != "receipt timeout" {
		t.Fatalf("requeued item = %+v", got)
	}
}

func TestIncidentDeliveryView(t *testing.T) {
	s := NewMemoryStore()
	t0 := time.Unix(10_000, 0)
	if _, err := s.CreatePolicy(PolicyInput{
		ID: "vp", Name: "vp",
		Steps: []Step{
			{WaitBefore: time.Minute, Targets: []string{"alice", "bob"}},
			{WaitBefore: time.Minute, Targets: []string{"carol"}},
		},
	}, t0); err != nil {
		t.Fatal(err)
	}
	inc, _ := s.CreateIncident(CreateIncidentRequest{RequestID: "r1", PolicyID: "vp"}, t0)
	if _, err := s.ProcessDue(t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	items := s.ListOutbox(inc.ID)
	if err := s.MarkDispatched(items[0].ID, t0); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkDelivered(items[0].ID, t0.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	view, err := s.IncidentDelivery(inc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if view.EscalationLevel != 1 || len(view.Deliveries) != 2 {
		t.Fatalf("view = %+v", view)
	}
	if view.Deliveries[0].Status != DeliveryAcknowledged || view.Deliveries[0].AckedAt.IsZero() {
		t.Fatalf("acked delivery = %+v", view.Deliveries[0])
	}
	if view.Deliveries[1].Status != DeliveryPending {
		t.Fatalf("pending delivery = %+v", view.Deliveries[1])
	}
	if _, err := s.IncidentDelivery("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing incident: want ErrNotFound, got %v", err)
	}
}

func mustHistory(t *testing.T, s *Store, incidentID string) []HistoryEntry {
	t.Helper()
	h, err := s.History(incidentID)
	if err != nil {
		t.Fatal(err)
	}
	return h
}
