// Package incidentescalation 提供值班事件升级服务：按冻结策略分步定时通知、
// 确认/解决状态机、值班交接（未派发意图随事件改派）、维护窗口升级抑制
// （窗口内 held、解除时按事件最新级别补发，每级最多一次有效通知），
// 全部状态一次性原子落盘，进程重启后从持久化状态继续执行。
package incidentescalation
