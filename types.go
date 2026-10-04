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

// DeliveryStatus 是一条通知意图的投递状态。
type DeliveryStatus string

const (
	// DeliveryPending 意图已生成，等待（首次或重试）交给渠道。
	DeliveryPending DeliveryStatus = "pending"
	// DeliveryDispatched 已交给渠道，等待渠道回执。
	DeliveryDispatched DeliveryStatus = "dispatched"
	// DeliveryAcknowledged 渠道明确接收（终态）。
	DeliveryAcknowledged DeliveryStatus = "acknowledged"
	// DeliveryFailed 最终投递失败：尝试次数达到重试上限（终态）。
	DeliveryFailed DeliveryStatus = "failed"
	// DeliveryStopped 尚未投递完成，但事件已确认/解决/被抑制，停止重试（终态）。
	DeliveryStopped DeliveryStatus = "stopped"
)

// OutboxStatus 是 DeliveryStatus 的旧名，保留以兼容既有代码。
type OutboxStatus = DeliveryStatus

const (
	// OutboxPending 等同 DeliveryPending。
	OutboxPending = DeliveryPending
	// OutboxDelivered 等同 DeliveryAcknowledged。
	OutboxDelivered = DeliveryAcknowledged
)

// RetryPolicy 控制投递失败后的重试行为。零值字段取默认值。
type RetryPolicy struct {
	// MaxAttempts 单条意图的最大尝试次数（含首次），默认 3。
	// 达到上限后意图进入 DeliveryFailed 终态。
	MaxAttempts int `json:"max_attempts"`
	// Backoff 是失败后到下次可重试的等待时长，默认 0（下一轮扫描即可重试）。
	Backoff time.Duration `json:"backoff"`
	// ReceiptTimeout 是交给渠道后等待回执的超时，默认 1 分钟；
	// 超时未收到回执视为一次失败尝试，可继续重试。
	ReceiptTimeout time.Duration `json:"receipt_timeout"`
}

// normalized 填充默认值。
func (rp RetryPolicy) normalized() RetryPolicy {
	if rp.MaxAttempts <= 0 {
		rp.MaxAttempts = 3
	}
	if rp.Backoff < 0 {
		rp.Backoff = 0
	}
	if rp.ReceiptTimeout <= 0 {
		rp.ReceiptTimeout = time.Minute
	}
	return rp
}

// OutboxItem 是事务性 outbox 中的一条通知意图及其投递状态。
// 它与事件状态、定时器在同一次原子写入中落盘，
// 保证“状态推进了就一定有通知意图”，且绝不重复。
// 重试只更新本条意图的投递状态，绝不会生成第二条意图。
type OutboxItem struct {
	ID         int64          `json:"id"`
	IncidentID string         `json:"incident_id"`
	StepIndex  int            `json:"step_index"`
	Target     string         `json:"target"`
	Channel    string         `json:"channel"`
	Message    string         `json:"message"`
	Status     DeliveryStatus `json:"status"`
	// Version 是通知意图版本：每次交给渠道（一次尝试）递增。
	// 渠道回执必须携带该版本，迟到回执不能把更高版本的状态改回去。
	Version  int `json:"version"`
	Attempts int `json:"attempts"`
	// LastError 是最近一次尝试/回执失败的原因。
	LastError string `json:"last_error,omitempty"`
	// LastAttemptAt 是最后一次交给渠道（或尝试失败）的时间。
	LastAttemptAt time.Time `json:"last_attempt_at,omitempty"`
	// NextAttemptAt 是下次可重试时间（持久化，重启后继续按此重试）。
	NextAttemptAt time.Time `json:"next_attempt_at,omitempty"`
	// AckedAt 是渠道明确接收（确认）的时间。
	AckedAt time.Time `json:"acked_at,omitempty"`
	// StopReason 是进入 DeliveryStopped 的原因（如 incident_acknowledged）。
	StopReason string    `json:"stop_reason,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

// ReceiptOutcome 是渠道回执的结论。
type ReceiptOutcome string

const (
	// ReceiptAccepted 渠道明确接收。
	ReceiptAccepted ReceiptOutcome = "accepted"
	// ReceiptFailed 渠道明确失败。
	ReceiptFailed ReceiptOutcome = "failed"
)

// ReceiptRequest 上报一条渠道回执。
// 回执关联事件、步骤、目标与通知意图版本；ReceiptID 用于幂等：
// 相同回执重放返回第一次处理结果，同号异内容返回 ErrConflict。
type ReceiptRequest struct {
	ReceiptID     string
	IncidentID    string
	StepIndex     int
	Target        string
	IntentVersion int
	Outcome       ReceiptOutcome
	// Error 是 outcome=failed 时渠道给出的失败原因。
	Error string
}

// ReceiptResult 是回执的处理结果（重放时返回第一次的结果）。
type ReceiptResult struct {
	// Applied 为 true 表示回执改变了投递状态。
	Applied bool
	// Status 是处理后的投递状态。
	Status DeliveryStatus
	// Reason 说明处理结论（如 applied、stale_intent_version、already_acknowledged）。
	Reason string
}

// TargetDelivery 是事件级投递视图中一个通知目标的最终投递结果。
type TargetDelivery struct {
	StepIndex     int            `json:"step_index"`
	Target        string         `json:"target"`
	Channel       string         `json:"channel"`
	Status        DeliveryStatus `json:"status"`
	Version       int            `json:"version"`
	Attempts      int            `json:"attempts"`
	LastError     string         `json:"last_error,omitempty"`
	LastAttemptAt time.Time      `json:"last_attempt_at,omitempty"`
	AckedAt       time.Time      `json:"acked_at,omitempty"`
	StopReason    string         `json:"stop_reason,omitempty"`
}

// IncidentDeliveryView 是事件级投递视图：
// 升级进度（已触发步骤数）加上每个通知目标的最终投递结果。
type IncidentDeliveryView struct {
	Incident *Incident `json:"incident"`
	// EscalationLevel 是已触发的升级步骤数（0 表示尚未触发任何步骤）。
	EscalationLevel int              `json:"escalation_level"`
	Deliveries      []TargetDelivery `json:"deliveries"`
}

// HistoryEntry 是事件历史中的一条记录。
type HistoryEntry struct {
	Time       time.Time `json:"time"`
	IncidentID string    `json:"incident_id"`
	Kind       string    `json:"kind"`
	Detail     string    `json:"detail"`
}
