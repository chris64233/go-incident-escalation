package incidentescalation

import "time"

// Step 是升级策略中的一个响应步骤。
type Step struct {
	// WaitBefore 是该步骤相对上一动作触发前的等待时长：
	// 第 0 步相对事件开始时间，之后的步骤相对前一步触发时间。
	WaitBefore time.Duration `json:"wait_before"`
	// Targets 是本步需要通知的目标，一步可通知多个目标。
	// 同一事件、同一步骤、同一目标只会形成一次通知意图。
	Targets []string `json:"targets"`
}

// Policy 是一份升级策略。策略自身可被管理（创建/更新/查询），
// 但事件创建时会把当时的步骤序列“冻结”进事件，后续修改策略不影响存量事件。
type Policy struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Steps     []Step    `json:"steps"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	// Version 在策略配置变更时递增，供乐观判断使用。
	Version int `json:"version"`
}

// IncidentStatus 是事件的生命周期状态。
type IncidentStatus string

const (
	// StatusFiring 事件进行中，等待确认或下一步升级。
	StatusFiring IncidentStatus = "firing"
	// StatusAcknowledged 已被值班人员确认，升级停止（不再推进、不再通知）。
	StatusAcknowledged IncidentStatus = "acknowledged"
	// StatusResolved 事件已解决，不能再确认，也不会再通知。
	StatusResolved IncidentStatus = "resolved"
)

// Incident 是一个值班事件及其冻结的升级策略与当前进度。
type Incident struct {
	ID       string         `json:"id"`
	PolicyID string         `json:"policy_id"`
	Severity string         `json:"severity"`
	Detail   string         `json:"detail"`
	Status   IncidentStatus `json:"status"`
	// FrozenSteps 是事件创建时冻结下来的步骤快照。
	FrozenSteps []Step    `json:"frozen_steps"`
	StartedAt   time.Time `json:"started_at"`
	// NextStepIndex 下一个待触发的步骤下标（0 表示还未触发任何步骤）。
	NextStepIndex int `json:"next_step_index"`
	// FiredAt 每个已触发步骤的触发时间，下标与 FrozenSteps 对齐。
	FiredAt        []time.Time `json:"fired_at"`
	AcknowledgedAt time.Time   `json:"acknowledged_at,omitempty"`
	AcknowledgedBy string      `json:"acknowledged_by,omitempty"`
	ResolvedAt     time.Time   `json:"resolved_at,omitempty"`
	ResolvedBy     string      `json:"resolved_by,omitempty"`
	CreatedAt      time.Time   `json:"created_at"`
	UpdatedAt      time.Time   `json:"updated_at"`
}

// OutboxStatus 通知意图的投递状态。
type OutboxStatus string

const (
	// OutboxPending 待投递。
	OutboxPending OutboxStatus = "pending"
	// OutboxDelivered 已成功投递（由投递方标记）。
	OutboxDelivered OutboxStatus = "delivered"
)

// OutboxItem 是事务性 outbox 中的一条通知意图。
// 它与事件状态、定时器在同一次原子写入中落盘，
// 保证“状态推进了就一定有通知意图”，且绝不重复。
type OutboxItem struct {
	ID          int64        `json:"id"`
	IncidentID  string       `json:"incident_id"`
	StepIndex   int          `json:"step_index"`
	Target      string       `json:"target"`
	Channel     string       `json:"channel"`
	Message     string       `json:"message"`
	Status      OutboxStatus `json:"status"`
	CreatedAt   time.Time    `json:"created_at"`
	DeliveredAt time.Time    `json:"delivered_at,omitempty"`
}

// HistoryEntry 是事件历史中的一条记录。
type HistoryEntry struct {
	Time       time.Time `json:"time"`
	IncidentID string    `json:"incident_id"`
	Kind       string    `json:"kind"`
	Detail     string    `json:"detail"`
}
