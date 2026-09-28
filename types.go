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
	// AssignedTeam 是当前对该事件负责的值班班组；值班交接时整体替换为新班组。
	AssignedTeam string `json:"assigned_team"`
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
	// Handoffs 是本事件经历过的值班交接轨迹（按发生顺序），用于判断是否已被某次交接移交。
	Handoffs  []HandoffRecord `json:"handoffs,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// HandoffRecord 是事件交接轨迹中的一条：某次交接把本事件从 FromTeam 移交给 ToTeam。
type HandoffRecord struct {
	HandoffID string    `json:"handoff_id"`
	FromTeam  string    `json:"from_team"`
	ToTeam    string    `json:"to_team"`
	At        time.Time `json:"at"`
}

// Handoff 是一次值班交接：交接生效瞬间冻结的待移交事件清单及其新旧班组。
type Handoff struct {
	ID string `json:"id"`
	// FromTeam / ToTeam 是交接双方的值班班组标识。
	FromTeam string `json:"from_team"`
	ToTeam   string `json:"to_team"`
	// IncidentIDs 是交接生效时冻结下来的清单：仅包含当时仍在 firing
	// 且责任班组为 FromTeam 的事件；已确认/已解决的事件不进入清单。
	IncidentIDs []string  `json:"incident_ids"`
	CreatedAt   time.Time `json:"created_at"`
}

// OutboxStatus 通知意图的投递状态。
type OutboxStatus string

const (
	// OutboxPending 待投递。
	OutboxPending OutboxStatus = "pending"
	// OutboxDelivered 已成功投递（由投递方标记）。
	OutboxDelivered OutboxStatus = "delivered"
)

// NotificationKind 区分 outbox 中通知意图的类型。
type NotificationKind string

const (
	// KindEscalation 升级策略推进产生的常规升级通知。
	KindEscalation NotificationKind = "escalation"
	// KindHandoff 值班交接时发给新值班班组的交接通知。
	KindHandoff NotificationKind = "handoff"
)

// OutboxItem 是事务性 outbox 中的一条通知意图。
// 它与事件状态、定时器在同一次原子写入中落盘，
// 保证“状态推进了就一定有通知意图”，且绝不重复。
type OutboxItem struct {
	ID         int64  `json:"id"`
	IncidentID string `json:"incident_id"`
	// Kind 标识这是一条升级通知还是交接通知。
	Kind NotificationKind `json:"kind"`
	// StepIndex 仅对升级通知有意义（交接通知为 -1）。
	StepIndex int          `json:"step_index"`
	Target    string       `json:"target"`
	Channel   string       `json:"channel"`
	Message   string       `json:"message"`
	Status    OutboxStatus `json:"status"`
	// OwnerTeam 是“当前负责投递该意图的值班班组”。
	// 交接时 pending 的意图原子转交给新班组（始终只有一方负责投递）；
	// 已投递的意图保持原记录与原责任方不变。空字符串表示尚未引入班组概念的旧数据。
	OwnerTeam   string    `json:"owner_team,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	DeliveredAt time.Time `json:"delivered_at,omitempty"`
}

// HistoryEntry 是事件历史中的一条记录。
type HistoryEntry struct {
	Time       time.Time `json:"time"`
	IncidentID string    `json:"incident_id"`
	Kind       string    `json:"kind"`
	Detail     string    `json:"detail"`
}
