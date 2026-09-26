package incidentescalation

import (
	"errors"
	"fmt"
	"time"
)

// IncidentStatus 表示事件生命周期状态。
type IncidentStatus string

const (
	StatusOpen         IncidentStatus = "open"
	StatusAcknowledged IncidentStatus = "acknowledged"
	StatusResolved     IncidentStatus = "resolved"
)

// 领域错误，调用方可用 errors.Is 判定。
var (
	ErrNotFound     = errors.New("not found")
	ErrConflict     = errors.New("idempotency conflict")
	ErrInvalidState = errors.New("invalid state transition")
	ErrValidation   = errors.New("validation error")
)

func validationErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrValidation, fmt.Sprintf(format, args...))
}

func notFoundErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrNotFound, fmt.Sprintf(format, args...))
}

func invalidStateErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidState, fmt.Sprintf(format, args...))
}

// Step 是升级策略中的一步：等待 WaitDuration 后若事件仍未确认则推进到下一步；
// 进入某一步时会向该步所有 Targets 生成通知意图（outbox）。
type Step struct {
	WaitDuration time.Duration `json:"waitDuration"`
	Targets      []string      `json:"targets"`
}

// Policy 是升级策略。事件创建时会冻结其快照，之后对策略的修改不影响已创建事件。
type Policy struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Steps     []Step    `json:"steps"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// Incident 是值班事件。Policy 字段为创建时冻结的策略快照。
type Incident struct {
	ID          string         `json:"id"`
	PolicyID    string         `json:"policyId"`
	Policy      Policy         `json:"policy"` // 冻结快照
	Title       string         `json:"title"`
	Description string         `json:"description"`
	Status      IncidentStatus `json:"status"`
	CurrentStep int            `json:"currentStep"` // 当前正在等待确认的步骤下标
	Exhausted   bool           `json:"exhausted"`   // 所有步骤均已执行仍未确认

	AckedBy    string     `json:"ackedBy,omitempty"`
	AckedAt    *time.Time `json:"ackedAt,omitempty"`
	ResolvedBy string     `json:"resolvedBy,omitempty"`
	ResolvedAt *time.Time `json:"resolvedAt,omitempty"`

	CreatedAt time.Time `json:"createdAt"`
}

// Timer 是事件的持久化定时器；每个事件至多一个活跃定时器。
type Timer struct {
	IncidentID string    `json:"incidentId"`
	Step       int       `json:"step"`
	DueAt      time.Time `json:"dueAt"`
}

// OutboxEntry 是一条通知意图。Key = incidentID|step|target，天然去重：
// 同一事件、同一步骤、同一目标只会形成一次通知意图。
type OutboxEntry struct {
	Key        string    `json:"key"`
	IncidentID string    `json:"incidentId"`
	Step       int       `json:"step"`
	Target     string    `json:"target"`
	CreatedAt  time.Time `json:"createdAt"`
}

// HistoryEventType 历史事件类型。
type HistoryEventType string

const (
	HistoryCreated      HistoryEventType = "created"
	HistoryNotified     HistoryEventType = "notified"
	HistoryEscalated    HistoryEventType = "escalated"
	HistoryAcknowledged HistoryEventType = "acknowledged"
	HistoryResolved     HistoryEventType = "resolved"
	HistoryExhausted    HistoryEventType = "exhausted"
)

// HistoryEvent 是事件上发生的一次状态变化记录。
type HistoryEvent struct {
	IncidentID string           `json:"incidentId"`
	Type       HistoryEventType `json:"type"`
	Step       int              `json:"step"`
	Detail     string           `json:"detail,omitempty"`
	At         time.Time        `json:"at"`
}

// idempotencyRecord 记录外部请求号的处理结果，用于幂等重放与冲突检测。
type idempotencyRecord struct {
	Key         string `json:"key"` // op + ":" + requestID
	RequestHash string `json:"requestHash"`
	Result      string `json:"result"` // create 操作返回事件 ID，其余为空串
}

// Clock 抽象时间来源，便于测试。
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
