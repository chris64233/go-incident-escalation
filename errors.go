package incidentescalation

import "errors"

// 业务错误均为哨兵错误，可用 errors.Is 判断；具体错误信息会用 %w 包裹补充上下文。
var (
	// ErrNotFound 策略或事件不存在。
	ErrNotFound = errors.New("incident-escalation: resource not found")
	// ErrConflict 同一外部请求号被用于不同内容，或资源状态与请求不兼容。
	ErrConflict = errors.New("incident-escalation: conflict")
	// ErrAlreadyExists 同名资源（如策略 ID）已存在。
	ErrAlreadyExists = errors.New("incident-escalation: resource already exists")
	// ErrInvalidArgument 入参非法。
	ErrInvalidArgument = errors.New("incident-escalation: invalid argument")
	// ErrAlreadyAcknowledged 事件已被确认（且不是本次幂等重试）。
	ErrAlreadyAcknowledged = errors.New("incident-escalation: incident already acknowledged")
	// ErrAlreadyResolved 事件已被解决（且不是本次幂等重试）。
	ErrAlreadyResolved = errors.New("incident-escalation: incident already resolved")
	// ErrIncidentResolved 事件已解决，不能再确认；解决后也不会再产生通知。
	ErrIncidentResolved = errors.New("incident-escalation: incident already resolved, ack and notifications are rejected")
	// ErrStaleReceipt 回执携带的意图版本落后于当前版本（迟到回执），不能改回状态。
	ErrStaleReceipt = errors.New("incident-escalation: stale receipt for older intent version")
	// ErrDeliveryClosed 通知意图已进入终态（已确认/最终失败/已停止），不能再投递或重试。
	ErrDeliveryClosed = errors.New("incident-escalation: delivery is in a terminal state")
)
