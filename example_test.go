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

// ExampleSuppression 展示维护窗口抑制：窗口内升级被暂缓（原因可查），
// 期间级别变化持续记录，窗口结束后按最新级别补发未处理级别，
// 已解决事件不会重新通知。
func Example_suppressionWindow() {
	store := incidentescalation.NewMemoryStore()
	now := time.Unix(1_700_000_000, 0)

	_, _ = store.CreatePolicy(incidentescalation.PolicyInput{
		ID:   "p",
		Name: "标准升级",
		Steps: []incidentescalation.Step{
			{WaitBefore: 0, Targets: []string{"oncall-l1"}},
			{WaitBefore: 30 * time.Minute, Targets: []string{"oncall-l2"}},
		},
	}, now)

	inc, err := store.CreateIncident(incidentescalation.CreateIncidentRequest{
		RequestID: "inc-1", IncidentID: "inc-1", PolicyID: "p",
		Severity: "warning", Detail: "复制延迟过高", OwnerGroup: "night-shift",
	}, now)
	if err != nil {
		panic(err)
	}

	// 交班后开一个 2 小时维护窗口，只抑制这起事件的升级。
	rule, err := store.CreateSuppression(incidentescalation.CreateSuppressionRequest{
		RequestID:  "mw-req-1",
		RuleID:     "mw-db-reindex",
		Scope:      incidentescalation.SuppressionScope{IncidentIDs: []string{inc.ID}},
		Reason:     "凌晨数据库索引重建",
		ValidFrom:  now,
		ValidUntil: now.Add(2 * time.Hour),
	}, now)
	if err != nil {
		panic(err)
	}

	// 窗口内两步升级都到期：步骤照常触发，但通知意图全部 held，不派发。
	_, _ = store.ProcessDue(now.Add(time.Hour))
	_, _ = store.ProcessDue(now.Add(90 * time.Minute))
	if pending := store.PendingOutbox(); len(pending) != 0 {
		panic("suppressed escalations must not be dispatched")
	}

	// 内容相同（范围 + 原因）但解除时间不同：明确拒绝，不静默延长或覆盖原窗口。
	_, dupErr := store.CreateSuppression(incidentescalation.CreateSuppressionRequest{
		RequestID: "mw-req-dup", RuleID: "mw-db-reindex-longer",
		Scope:     incidentescalation.SuppressionScope{IncidentIDs: []string{inc.ID}},
		Reason:    "凌晨数据库索引重建",
		ValidFrom: now, ValidUntil: now.Add(4 * time.Hour),
	}, now.Add(time.Minute))
	fmt.Println("重复规则被拒绝:", dupErr != nil)

	// 抑制期间事件级别变化：持续记录、可查询。
	_, _ = store.UpdateSeverity(incidentescalation.UpdateSeverityRequest{
		RequestID: "sev-1", IncidentID: inc.ID, Severity: "critical",
		Reason: "业务高峰期延迟影响扩大", UpdatedBy: "night-shift",
	}, now.Add(75*time.Minute))

	// 维护窗口结束后的扫描：自动解除，按最新级别补发暂缓的升级。
	_, _ = store.ProcessDue(now.Add(3 * time.Hour))
	for _, item := range store.PendingOutbox() {
		fmt.Printf("补发通知: step=%d target=%s kind=%s\n",
			item.StepIndex, item.Target, item.Kind)
	}

	// 查询视图：事件、每一级升级与抑制原因、级别轨迹、当时是否还在窗口内。
	view, err := store.GetIncidentView(inc.ID, now.Add(3*time.Hour))
	if err != nil {
		panic(err)
	}
	fmt.Println("最新级别:", view.Incident.Severity, "有效抑制规则数:", len(view.ActiveSuppressions))
	_ = rule

	// Output:
	// 重复规则被拒绝: true
	// 补发通知: step=0 target=oncall-l1 kind=suppression_resume
	// 补发通知: step=1 target=oncall-l2 kind=suppression_resume
	// 最新级别: critical 有效抑制规则数: 0
}
