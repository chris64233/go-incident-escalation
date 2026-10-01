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
	// Handovers 保留该事件的每一次值班交接（移交人、接手人与时间），按发生顺序排列。
	Handovers []HandoverRecord `json:"handovers,omitempty"`
	// SeverityHistory 记录事件级别的每次变化；抑制期间级别照常变化且持续可见。
	SeverityHistory []SeverityChange `json:"severity_history,omitempty"`
	// SuppressedSteps 记录每个在抑制窗口内触发的升级步骤；解除抑制后据此补发，
	// 已解决/已确认事件的记录只保留用于展示，不会触发补发。
	SuppressedSteps []SuppressedStep `json:"suppressed_steps,omitempty"`
	CreatedAt       time.Time        `json:"created_at"`
	UpdatedAt       time.Time        `json:"updated_at"`
}

// CurrentHandler 返回事件当前的值班接手人（最近一次交接的接手方）；
// 从未交接过时返回空字符串。
func (inc *Incident) CurrentHandler() string {
	if len(inc.Handovers) > 0 {
		return inc.Handovers[len(inc.Handovers)-1].To
	}
	return ""
}

// hasSuppressedFromRuleLocked 报告事件是否有在指定规则下触发的抑制级别
// （即使该级别后来已补发也计入，用于解除时定位历史覆盖范围）。
func (inc *Incident) hasSuppressedFromRuleLocked(ruleID string) bool {
	for _, rec := range inc.SuppressedSteps {
		if rec.RuleID == ruleID {
			return true
		}
	}
	return false
}

// HandoverRecord 是一次值班交接记录。同一事件多次交接全部保留。
type HandoverRecord struct {
	Time time.Time `json:"time"`
	From string    `json:"from"`
	To   string    `json:"to"`
	Note string    `json:"note,omitempty"`
}

// SeverityChange 记录一次事件级别（severity）调整。
type SeverityChange struct {
	Time      time.Time `json:"time"`
	From      string    `json:"from"`
	To        string    `json:"to"`
	Reason    string    `json:"reason,omitempty"`
	ChangedBy string    `json:"changed_by,omitempty"`
}

// SuppressedStep 记录一个在抑制窗口内触发的升级步骤：步骤照常推进，
// 但不产生通知，等待解除抑制时按事件最新状态决定是否补发。
type SuppressedStep struct {
	StepIndex  int       `json:"step_index"`
	FiredAt    time.Time `json:"fired_at"`
	RuleID     string    `json:"rule_id"`
	Reason     string    `json:"reason"`
	CaughtUpAt time.Time `json:"caught_up_at,omitempty"`
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
	ID         int64        `json:"id"`
	IncidentID string       `json:"incident_id"`
	StepIndex  int          `json:"step_index"`
	Target     string       `json:"target"`
	Channel    string       `json:"channel"`
	Message    string       `json:"message"`
	Status     OutboxStatus `json:"status"`
	// Handler 是意图形成时该事件的当前值班接手人（可能为空）。
	Handler string `json:"handler,omitempty"`
	// CatchUp 为 true 表示这是抑制解除后按事件最新状态补发的通知。
	CatchUp     bool      `json:"catch_up,omitempty"`
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

// SuppressionReleaseCondition 是抑制规则的解除条件。
type SuppressionReleaseCondition string

const (
	// ReleaseAtWindowEnd 窗口结束时自动解除（默认）。
	ReleaseAtWindowEnd SuppressionReleaseCondition = "window_end"
	// ReleaseManual 只能手工解除；窗口结束后规则仍然有效。
	ReleaseManual SuppressionReleaseCondition = "manual"
)

// SuppressionScope 定义抑制规则覆盖的事件范围；三类条件为“或”关系，
// 全部为空表示覆盖所有事件。
type SuppressionScope struct {
	IncidentIDs []string `json:"incident_ids,omitempty"`
	Severities  []string `json:"severities,omitempty"`
	PolicyIDs   []string `json:"policy_ids,omitempty"`
}

// SuppressionStatus 是抑制规则的生命周期状态。
type SuppressionStatus string

const (
	// SuppressionActive 规则有效（是否在窗口内还需结合当前时间判断）。
	SuppressionActive SuppressionStatus = "active"
	// SuppressionReleased 规则已解除（窗口到期自动解除或手工解除）。
	SuppressionReleased SuppressionStatus = "released"
)

// SuppressionRule 是一条维护窗口抑制规则：范围 + 有效时间 + 解除条件。
type SuppressionRule struct {
	ID        string                      `json:"id"`
	Scope     SuppressionScope            `json:"scope"`
	Reason    string                      `json:"reason"`
	StartsAt  time.Time                   `json:"starts_at"`
	EndsAt    time.Time                   `json:"ends_at"`
	Condition SuppressionReleaseCondition `json:"condition"`
	Status    SuppressionStatus           `json:"status"`
	CreatedBy string                      `json:"created_by,omitempty"`
	CreatedAt time.Time                   `json:"created_at"`
	// ReleasedAt/ReleasedBy 记录解除时刻与操作者；自动解除时 ReleasedBy 为 "system"。
	ReleasedAt time.Time `json:"released_at,omitempty"`
	ReleasedBy string    `json:"released_by,omitempty"`
}

// EscalationState 是视图中单个升级步骤的展示状态。
type EscalationState string

const (
	// EscalationPending 步骤尚未到触发时间。
	EscalationPending EscalationState = "pending"
	// EscalationNotified 步骤正常触发并已形成通知。
	EscalationNotified EscalationState = "notified"
	// EscalationSuppressed 步骤在抑制窗口内触发，通知仍被抑制。
	EscalationSuppressed EscalationState = "suppressed"
	// EscalationCaughtUp 曾被抑制，解除后已补发通知。
	EscalationCaughtUp EscalationState = "caught_up"
)

// EscalationInfo 是视图中的单个升级步骤信息。
type EscalationInfo struct {
	StepIndex  int             `json:"step_index"`
	Targets    []string        `json:"targets"`
	FiredAt    time.Time       `json:"fired_at,omitempty"`
	State      EscalationState `json:"state"`
	RuleID     string          `json:"rule_id,omitempty"`
	Reason     string          `json:"reason,omitempty"`
	CaughtUpAt time.Time       `json:"caught_up_at,omitempty"`
}

// SuppressionView 是视图中与某事件相关的抑制规则及其当前是否生效。
type SuppressionView struct {
	RuleID   string            `json:"rule_id"`
	Reason   string            `json:"reason"`
	Active   bool              `json:"active"`
	Status   SuppressionStatus `json:"status"`
	StartsAt time.Time         `json:"starts_at"`
	EndsAt   time.Time         `json:"ends_at"`
}

// IncidentView 是事件的综合查询视图：事件本体、升级、交接、级别变化与抑制原因。
type IncidentView struct {
	Incident        *Incident
	Handovers       []HandoverRecord
	SeverityHistory []SeverityChange
	Escalations     []EscalationInfo
	Suppressions    []SuppressionView
}
