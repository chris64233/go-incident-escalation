package incidentescalation_test

import (
	"fmt"
	"time"

	incidentescalation "github.com/chris64233/go-incident-escalation"
)

// Example 展示一次完整的升级流程：配置策略 → 创建事件（冻结策略、登记定时器）
// → 到期推进（产生 outbox 意图）→ 确认停止升级 → 查询历史。
func Example() {
	store := incidentescalation.NewMemoryStore()
	now := time.Unix(1_700_000_000, 0)

	// 1) 配置策略：第 0 步立即通知一线；5 分钟未确认升级到二线；15 分钟通知主管。
	_, err := store.CreatePolicy(incidentescalation.PolicyInput{
		ID:   "oncall-standard",
		Name: "标准值班升级",
		Steps: []incidentescalation.Step{
			{WaitBefore: 0, Targets: []string{"oncall-l1"}},
			{WaitBefore: 5 * time.Minute, Targets: []string{"oncall-l2"}},
			{WaitBefore: 10 * time.Minute, Targets: []string{"manager"}},
		},
	}, now)
	if err != nil {
		panic(err)
	}

	// 2) 创建事件：策略被冻结进事件，第 0 步定时器同步持久化。
	inc, err := store.CreateIncident(incidentescalation.CreateIncidentRequest{
		RequestID: "ext-req-001",
		PolicyID:  "oncall-standard",
		Severity:  "critical",
		Detail:    "数据库主库连接耗尽",
	}, now)
	if err != nil {
		panic(err)
	}

	// 3) 同一外部请求号重试：返回同一个事件，不会重复创建。
	again, err := store.CreateIncident(incidentescalation.CreateIncidentRequest{
		RequestID: "ext-req-001", PolicyID: "oncall-standard", Severity: "critical", Detail: "数据库主库连接耗尽",
	}, now.Add(time.Hour))
	if err != nil {
		panic(err)
	}
	fmt.Println("幂等创建得到同一事件:", again.ID == inc.ID)

	// 4) 定时器到期（实际服务由后台循环周期调用；此处手工演示）。
	t := now
	// 第 0 步等待为 0，立即触发；无论时间推进多远，每次只跨一步。
	for i := 0; i < 3; i++ {
		if _, err := store.ProcessDue(t); err != nil {
			panic(err)
		}
		if i == 0 {
			// 一线在第 0 步触发后确认，升级立刻停止，后续步骤不再通知。
			if _, err := store.Acknowledge(incidentescalation.AckRequest{
				RequestID: "ack-001", IncidentID: inc.ID, AcknowledgedBy: "oncall-l1",
			}, t); err != nil {
				panic(err)
			}
		}
		t = t.Add(10 * time.Minute)
	}
	// 再往后扫描也不会有新通知。
	if _, err := store.ProcessDue(t.Add(100 * time.Hour)); err != nil {
		panic(err)
	}

	for _, item := range store.ListOutbox(inc.ID) {
		fmt.Printf("通知意图: step=%d target=%s\n", item.StepIndex, item.Target)
	}

	// 5) 查询历史。
	history, err := store.History(inc.ID)
	if err != nil {
		panic(err)
	}
	for _, h := range history {
		fmt.Println("历史:", h.Kind)
	}

	// Output:
	// 幂等创建得到同一事件: true
	// 通知意图: step=0 target=oncall-l1
	// 历史: created
	// 历史: step_fired
	// 历史: acknowledged
}
