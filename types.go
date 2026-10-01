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

// DefaultGroup 是未显式指定时事件与通知意图所属的值班组。
const DefaultGroup = "default"

// Incident 是一个值班事件及其冻结的升级策略与当前进度。
type Incident struct {
	ID       string         `json:"id"`
	PolicyID string         `json:"policy_id"`
	Severity string         `json:"severity"`
	Detail   string         `json:"detail"`
	Status   IncidentStatus `json:"status"`
	// OwnerGroup 是当前负责处理本事件的值班组，随交接原子变更。
	OwnerGroup string `json:"owner_group"`
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
	// SeverityChanges 是事件级别（severity）的完整变更轨迹，按时间排序。
	// 第 0 条是创建时的初始级别；抑制期间级别变化也会持续记录，
	// 解除抑制时以最后一条的级别判断是否需要恢复通知。
	SeverityChanges []SeverityChange `json:"severity_changes,omitempty"`
	// Handoffs 是该事件经历的每次值班交接，按时间排序，
	// 即使同一事件被多次交接也保留每一次的接手人和时间。
	Handoffs  []HandoffEntry `json:"handoffs,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
}

// SeverityChange 是一次事件级别变更的记录。
type SeverityChange struct {
	Time   time.Time `json:"time"`
	From   string    `json:"from"`
	To     string    `json:"to"`
	Reason string    `json:"reason"`
	// By 是发起级别变更的操作人（为空表示由系统/外部告警更新）。
	By string `json:"by,omitempty"`
	// Suppressed 表示变更发生时事件正处于抑制窗口内：
	// 变更持续可见，但当时不触发升级通知，解除抑制时再按最新级别判断。
	Suppressed bool `json:"suppressed,omitempty"`
}

// HandoffEntry 记录一次交接中本事件的接手信息（挂在事件上，保留完整轨迹）。
type HandoffEntry struct {
	HandoffID string    `json:"handoff_id"`
	FromGroup string    `json:"from_group"`
	ToGroup   string    `json:"to_group"`
	Time      time.Time `json:"time"`
}

// OutboxStatus 通知意图的投递状态。
type OutboxStatus string

const (
	// OutboxPending 待投递。
	OutboxPending OutboxStatus = "pending"
	// OutboxDelivered 已成功投递（由投递方标记）。
	OutboxDelivered OutboxStatus = "delivered"
	// OutboxHeld 升级已触发，但因维护窗口抑制而暂缓投递；
	// 解除抑制后若事件仍需升级则转回 pending，否则转为 canceled。
	OutboxHeld OutboxStatus = "held"
	// OutboxCanceled 抑制期间事件被确认/解决，或抑制解除后最新级别不再需要，
	// 该条升级意图被永久撤销（保留记录用于审计，不再投递）。
	OutboxCanceled OutboxStatus = "canceled"
)

// OutboxKind 通知意图的类型。
type OutboxKind string

const (
	// OutboxKindEscalation 升级步骤触发的通知。
	OutboxKindEscalation OutboxKind = "escalation"
	// OutboxKindHandoff 交接生效时发给新值班组的通知。
	OutboxKindHandoff OutboxKind = "handoff"
	// OutboxKindSuppressionResume 抑制解除后补发的升级通知。
	OutboxKindSuppressionResume OutboxKind = "suppression_resume"
)

// OutboxItem 是事务性 outbox 中的一条通知意图。
// 它与事件状态、定时器在同一次原子写入中落盘，
// 保证“状态推进了就一定有通知意图”，且绝不重复。
type OutboxItem struct {
	ID         int64        `json:"id"`
	IncidentID string       `json:"incident_id"`
	StepIndex  int          `json:"step_index"` // 交接通知为 -1
	Target     string       `json:"target"`
	Channel    string       `json:"channel"`
	Kind       OutboxKind   `json:"kind"`
	Message    string       `json:"message"`
	Status     OutboxStatus `json:"status"`
	// OwnerGroup 是当前负责派发本条意图的值班组。
	// 交接时未派发的意图随事件原子改派给新班组，已投递的保留原记录。
	OwnerGroup string `json:"owner_group"`
	// SuppressionID 非空表示本意图（曾）被某条抑制规则暂缓，
	// SuppressionReason 是当时记录的抑制原因；补发或撤销后仍然保留，
	// 供查询展示“这条升级为什么没发 / 后来怎么处理的”。
	SuppressionID     string    `json:"suppression_id,omitempty"`
	SuppressionReason string    `json:"suppression_reason,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	DeliveredAt       time.Time `json:"delivered_at,omitempty"`
	HeldAt            time.Time `json:"held_at,omitempty"`
	// ResumedAt 是 held -> pending（解除抑制补发）的时间。
	ResumedAt time.Time `json:"resumed_at,omitempty"`
	// CanceledAt / CanceledReason 是 held -> canceled 的时间与原因。
	CanceledAt     time.Time `json:"canceled_at,omitempty"`
	CanceledReason string    `json:"canceled_reason,omitempty"`
}

// HandoffRecord 是一张已生效的交接单：生效瞬间冻结的移交事件清单。
// 交接单持久化且按 RequestID 幂等，重放不会重复移交或重复通知。
type HandoffRecord struct {
	ID        string `json:"id"`
	RequestID string `json:"request_id"`
	FromGroup string `json:"from_group"`
	ToGroup   string `json:"to_group"`
	// IncidentIDs 是交接生效时冻结的移交清单（仅含当时仍 firing 的事件）。
	IncidentIDs []string  `json:"incident_ids"`
	CreatedAt   time.Time `json:"created_at"`
}

// ReleaseCondition 是抑制规则的解除条件类型。
type ReleaseCondition string

const (
	// ReleaseManual 允许通过 ReleaseSuppression 随时提前手工解除；
	// 到达 ValidUntil 时仍会由到期扫描兜底解除，保证 held 意图得到裁决。
	ReleaseManual ReleaseCondition = "manual"
	// ReleaseWindowEnd 到达 ValidUntil 时由到期扫描自动解除（默认）。
	ReleaseWindowEnd ReleaseCondition = "window_end"
	// ReleaseOnResolve 关联事件解决时自动解除；到 ValidUntil 仍未解决也会解除。
	ReleaseOnResolve ReleaseCondition = "on_resolve"
)

// SuppressionStatus 是抑制规则的生命周期状态。
type SuppressionStatus string

const (
	// SuppressionActive 抑制规则生效中（窗口内且未解除）。
	SuppressionActive SuppressionStatus = "active"
	// SuppressionReleased 抑制规则已解除（手工/窗口到期/事件解决）。
	SuppressionReleased SuppressionStatus = "released"
)

// SuppressionScope 界定一条抑制规则覆盖哪些事件：各条件之间为“与”，
// 零值（全部为空）表示覆盖全部事件。
type SuppressionScope struct {
	// IncidentIDs 非空时只抑制列出的事件。
	IncidentIDs []string `json:"incident_ids,omitempty"`
	// PolicyID 非空时只抑制使用该策略的事件。
	PolicyID string `json:"policy_id,omitempty"`
	// Severities 非空时只抑制当前级别在列表内的事件；
	// 抑制期间事件级别变化到列表之外会立即“逃出”抑制，不能被吞掉。
	Severities []string `json:"severities,omitempty"`
}

// SuppressionRule 是一条维护窗口升级抑制规则：范围 + 有效时间 + 解除条件。
type SuppressionRule struct {
	ID         string           `json:"id"`
	Scope      SuppressionScope `json:"scope"`
	Reason     string           `json:"reason"`
	ValidFrom  time.Time        `json:"valid_from"`
	ValidUntil time.Time        `json:"valid_until"`
	// ReleaseCondition 解除条件：manual / window_end（默认）/ on_resolve。
	ReleaseCondition ReleaseCondition  `json:"release_condition"`
	Status           SuppressionStatus `json:"status"`
	// CreatedBy 是规则创建人（如交班值班长）。
	CreatedBy string    `json:"created_by,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	// ReleasedAt / ReleasedBy 记录实际解除时间与操作人
	//（手工解除为操作人，自动解除由系统标记为 "system"）。
	ReleasedAt   time.Time `json:"released_at,omitempty"`
	ReleasedBy   string    `json:"released_by,omitempty"`
	ReleaseCause string    `json:"release_cause,omitempty"` // manual | window_end | on_resolve
}

// EscalationView 是查询视图中一条升级级别的展示信息：
// 合并 outbox 状态与抑制原因，说明这一级“是否通知、是否被抑制、后来是否补发/撤销”。
type EscalationView struct {
	StepIndex  int          `json:"step_index"`
	Target     string       `json:"target"`
	Kind       OutboxKind   `json:"kind"`
	Status     OutboxStatus `json:"status"`
	OwnerGroup string       `json:"owner_group"`
	CreatedAt  time.Time    `json:"created_at"`
	// Fired 表示该升级步骤在升级链上是否已经触发（可能触发后被 held）。
	Fired             bool      `json:"fired"`
	SuppressionID     string    `json:"suppression_id,omitempty"`
	SuppressionReason string    `json:"suppression_reason,omitempty"`
	ResumedAt         time.Time `json:"resumed_at,omitempty"`
	CanceledAt        time.Time `json:"canceled_at,omitempty"`
	CanceledReason    string    `json:"canceled_reason,omitempty"`
}

// IncidentView 是事件的综合查询视图：事件本体 + 级别轨迹 + 交接轨迹 +
// 每一级升级（含抑制原因与最终去向）+ 当前生效的抑制规则。
type IncidentView struct {
	Incident *Incident
	// Escalations 只包含升级类意图（escalation / suppression_resume），按 outbox ID 排序。
	Escalations []EscalationView
	// ActiveSuppressions 是此刻覆盖本事件且仍 active 的规则。
	ActiveSuppressions []*SuppressionRule
}

// HistoryEntry 是事件历史中的一条记录。
type HistoryEntry struct {
	Time       time.Time `json:"time"`
	IncidentID string    `json:"incident_id"`
	Kind       string    `json:"kind"`
	Detail     string    `json:"detail"`
}
