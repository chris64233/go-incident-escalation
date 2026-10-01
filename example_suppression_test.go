package incidentescalation_test

import (
	"fmt"
	"time"

	incidentescalation "github.com/chris64233/go-incident-escalation"
)

// ExampleSuppressionWorkflow 展示维护窗口抑制的完整流程：
// 交接后开启抑制窗口 → 抑制期间升级照走但不通知、级别变化可见 →
// 窗口结束自动解除 → 按事件最新状态补发错过的级别（每个级别一次）。
func Example_suppressionWorkflow() {
	store := incidentescalation.NewMemoryStore()
	t0 := time.Unix(1_700_000_000, 0)

	_, _ = store.CreatePolicy(incidentescalation.PolicyInput{
		ID:   "oncall-standard",
		Name: "标准值班升级",
		Steps: []incidentescalation.Step{
			{WaitBefore: 0, Targets: []string{"l1"}},
			{WaitBefore: 5 * time.Minute, Targets: []string{"l2"}},
		},
	}, t0)

	inc, _ := store.CreateIncident(incidentescalation.CreateIncidentRequest{
		RequestID: "inc-1", PolicyID: "oncall-standard", Severity: "critical",
		Detail: "核心交换机丢包",
	}, t0)

	// 1) 值班交接：alice 把事件移交给 bob；每次交接都会保留。
	_, _ = store.Handover(incidentescalation.HandoverRequest{
		RequestID: "h1", IncidentID: inc.ID, From: "alice", To: "bob",
	}, t0)

	// 2) 开启 30 分钟维护窗口（同内容规则不能重复创建，即使解除时间不同也会报错）。
	_, err := store.CreateSuppression(incidentescalation.SuppressionRequest{
		RequestID: "sup-1",
		SuppressionInput: incidentescalation.SuppressionInput{
			Scope:     incidentescalation.SuppressionScope{IncidentIDs: []string{inc.ID}},
			Reason:    "核心交换机固件升级",
			StartsAt:  t0,
			EndsAt:    t0.Add(30 * time.Minute),
			Condition: incidentescalation.ReleaseAtWindowEnd,
			CreatedBy: "bob",
		},
	}, t0)
	fmt.Println("创建抑制规则:", err == nil)

	// 3) 窗口内两次到期：两级都推进，但零通知。
	_, _ = store.ProcessDue(t0.Add(10 * time.Minute))
	_, _ = store.ProcessDue(t0.Add(20 * time.Minute))
	fmt.Println("窗口内通知数:", len(store.ListOutbox(inc.ID)))

	// 抑制期间事件级别变化持续可见（这里记录一条变化历史）。
	_, _ = store.ChangeSeverity(incidentescalation.ChangeSeverityRequest{
		RequestID: "sev-1", IncidentID: inc.ID, Severity: "blocker",
		Reason: "丢包加剧", ChangedBy: "bob",
	}, t0.Add(15*time.Minute))

	// 4) 窗口结束后的第一次扫描自动解除，并按最新状态补发每一级（每级一次）。
	_, _ = store.ProcessDue(t0.Add(31 * time.Minute))
	items := store.ListOutbox(inc.ID)
	for _, item := range items {
		fmt.Printf("补发通知: step=%d target=%s handler=%s catch-up=%t\n",
			item.StepIndex, item.Target, item.Handler, item.CatchUp)
	}

	// 5) 综合视图：升级状态、交接链、级别变化与抑制原因一次性查全。
	view, _ := store.IncidentViewAt(inc.ID, t0.Add(32*time.Minute))
	for _, esc := range view.Escalations {
		fmt.Printf("级别视图: step=%d state=%s\n", esc.StepIndex, esc.State)
	}
	fmt.Println("当前接手人:", view.Incident.CurrentHandler(), "最新级别:", view.Incident.Severity)

	// Output:
	// 创建抑制规则: true
	// 窗口内通知数: 0
	// 补发通知: step=0 target=l1 handler=bob catch-up=true
	// 补发通知: step=1 target=l2 handler=bob catch-up=true
	// 级别视图: step=0 state=caught_up
	// 级别视图: step=1 state=caught_up
	// 当前接手人: bob 最新级别: blocker
}
