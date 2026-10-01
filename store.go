package incidentescalation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Timer 是一个持久化的升级定时器：到达 FireAt 时，若事件仍未确认/解决，
// 则触发 IncidentID 的 StepIndex 步骤。
// 每个事件至多存在一个定时器（指向“下一步”），随状态在同一原子写入中落盘，
// 因此重启后可以原样恢复并继续执行。
type Timer struct {
	IncidentID string    `json:"incident_id"`
	StepIndex  int       `json:"step_index"`
	FireAt     time.Time `json:"fire_at"`
}

// PolicyInput 是创建/更新策略时由调用方提供的内容。
type PolicyInput struct {
	ID    string
	Name  string
	Steps []Step
}

// CreateIncidentRequest 创建事件。RequestID 是外部请求号，用于幂等。
type CreateIncidentRequest struct {
	RequestID  string
	IncidentID string // 可选，为空则自动生成
	PolicyID   string
	Severity   string
	Detail     string
	// OwnerGroup 初始负责的值班组，为空则归入 DefaultGroup。
	OwnerGroup string
}

// AckRequest 确认事件。RequestID 是外部请求号，用于幂等。
type AckRequest struct {
	RequestID      string
	IncidentID     string
	AcknowledgedBy string
}

// ResolveRequest 解决事件。RequestID 是外部请求号，用于幂等。
type ResolveRequest struct {
	RequestID  string
	IncidentID string
	ResolvedBy string
}

// UpdateSeverityRequest 更新事件级别（severity）。级别变化始终记录并可见；
// 若事件正处于抑制窗口内，变化只记录不通知，解除抑制时按最新级别判断。
// RequestID 是外部请求号，用于幂等。
type UpdateSeverityRequest struct {
	RequestID  string
	IncidentID string
	Severity   string
	Reason     string
	UpdatedBy  string
}

// HandoffRequest 执行一次值班交接：把 FromGroup 名下仍未确认/未解决的事件
// 及其未派发的通知整体移交给 ToGroup。RequestID 是外部请求号，用于幂等。
type HandoffRequest struct {
	RequestID string
	HandoffID string // 可选，为空则自动生成
	FromGroup string
	ToGroup   string
}

// CreateSuppressionRequest 创建一条维护窗口抑制规则。
// RequestID 是外部请求号，用于幂等。
type CreateSuppressionRequest struct {
	RequestID        string
	RuleID           string // 可选，为空则自动生成
	Scope            SuppressionScope
	Reason           string
	ValidFrom        time.Time
	ValidUntil       time.Time
	ReleaseCondition ReleaseCondition // 为空按 window_end 处理
	CreatedBy        string
}

// ReleaseSuppressionRequest 手工解除一条抑制规则。RequestID 用于幂等。
type ReleaseSuppressionRequest struct {
	RequestID  string
	RuleID     string
	ReleasedBy string
}

// requestRecord 记录一个外部请求号对应的请求内容指纹与结果，用于幂等与冲突检测。
type requestRecord struct {
	Kind        string `json:"kind"`        // create_incident | acknowledge | resolve | update_severity | handoff | create_suppression | release_suppression
	Fingerprint string `json:"fingerprint"` // 请求内容指纹
	IncidentID  string `json:"incident_id"` // 幂等重放时定位结果
}

// timerRecord 是 Timer 的持久化形态（当前同构，独立命名以便演进）。
type timerRecord = Timer

// snapshot 是一次性原子落盘的完整状态：事件状态、定时器、outbox 同生共死。
type snapshot struct {
	Policies     map[string]*Policy          `json:"policies"`
	Incidents    map[string]*Incident        `json:"incidents"`
	Outbox       []*OutboxItem               `json:"outbox"`
	NextOutboxID int64                       `json:"next_outbox_id"`
	Timers       map[string]*timerRecord     `json:"timers"`       // incidentID -> 唯一定时器
	SentIntents  map[string]struct{}         `json:"sent_intents"` // "incidentID\x00step\x00target" 去重
	Requests     map[string]requestRecord    `json:"requests"`
	Handoffs     map[string]*HandoffRecord   `json:"handoffs"`     // handoffID -> 交接单
	Suppressions map[string]*SuppressionRule `json:"suppressions"` // ruleID -> 抑制规则
	History      []HistoryEntry              `json:"history"`
	NextSeq      int64                       `json:"next_seq"`
}

func newSnapshot() *snapshot {
	return &snapshot{
		Policies:     map[string]*Policy{},
		Incidents:    map[string]*Incident{},
		Timers:       map[string]*timerRecord{},
		SentIntents:  map[string]struct{}{},
		Requests:     map[string]requestRecord{},
		Handoffs:     map[string]*HandoffRecord{},
		Suppressions: map[string]*SuppressionRule{},
	}
}

// Store 是事件升级服务的持久化核心。
// 所有变更都在互斥锁内完成并以“临时文件 + fsync + 原子 rename”整体落盘，
// 保证事件状态、定时器与 outbox 要么全部生效、要么全部不生效。
type Store struct {
	mu   sync.Mutex
	path string // 为空表示纯内存存储
	snap *snapshot
}

// OpenStore 打开（不存在则创建）路径上的持久化存储，并在重启后恢复全部状态。
func OpenStore(path string) (*Store, error) {
	s := &Store{path: path, snap: newSnapshot()}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if len(data) > 0 {
			if err := json.Unmarshal(data, s.snap); err != nil {
				return nil, fmt.Errorf("incident-escalation: corrupt state file %q: %w", path, err)
			}
			s.snap.afterLoad()
		}
	case errors.Is(err, os.ErrNotExist):
		if err := s.persistLocked(); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("incident-escalation: open store %q: %w", path, err)
	}
	return s, nil
}

// NewMemoryStore 返回不落盘的存储（主要用于测试）。
func NewMemoryStore() *Store {
	return &Store{snap: newSnapshot()}
}

// afterLoad 修复旧版本快照中可能为 nil 的 map。
func (snap *snapshot) afterLoad() {
	if snap.Policies == nil {
		snap.Policies = map[string]*Policy{}
	}
	if snap.Incidents == nil {
		snap.Incidents = map[string]*Incident{}
	}
	if snap.Timers == nil {
		snap.Timers = map[string]*timerRecord{}
	}
	if snap.SentIntents == nil {
		snap.SentIntents = map[string]struct{}{}
	}
	if snap.Requests == nil {
		snap.Requests = map[string]requestRecord{}
	}
	if snap.Handoffs == nil {
		snap.Handoffs = map[string]*HandoffRecord{}
	}
	if snap.Suppressions == nil {
		snap.Suppressions = map[string]*SuppressionRule{}
	}
	// 兼容旧版本快照：升级意图与归属字段是后续版本新增的。
	for _, item := range snap.Outbox {
		if item.Kind == "" {
			item.Kind = OutboxKindEscalation
		}
		if item.OwnerGroup == "" {
			item.OwnerGroup = DefaultGroup
		}
	}
	// 旧事件没有级别轨迹时补一条初始轨迹，保证查询视图完整。
	for _, inc := range snap.Incidents {
		if len(inc.SeverityChanges) == 0 {
			inc.SeverityChanges = []SeverityChange{{
				Time: inc.CreatedAt, To: inc.Severity, Reason: "created",
			}}
		}
		if inc.OwnerGroup == "" {
			inc.OwnerGroup = DefaultGroup
		}
	}
}

// persistLocked 将当前快照原子写入磁盘；调用方必须持有 mu。
func (s *Store) persistLocked() error {
	if s.path == "" {
		return nil
	}
	// 先在锁内完成序列化，避免落盘期间状态被改写导致写一半。
	data, err := json.MarshalIndent(s.snap, "", "  ")
	if err != nil {
		return fmt.Errorf("incident-escalation: marshal state: %w", err)
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("incident-escalation: create temp state: %w", err)
	}
	tmpName := tmp.Name()
	rollback := func(cause error) error {
		tmp.Close()
		os.Remove(tmpName)
		return cause
	}
	if _, err := tmp.Write(data); err != nil {
		return rollback(fmt.Errorf("incident-escalation: write state: %w", err))
	}
	if err := tmp.Sync(); err != nil {
		return rollback(fmt.Errorf("incident-escalation: fsync state: %w", err))
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("incident-escalation: close state: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("incident-escalation: rename state: %w", err)
	}
	// fsync 目录，保证 rename 也持久化。
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// commitLocked 统一处理“改完即落盘”。
func (s *Store) commitLocked() error { return s.persistLocked() }

// ---------- 工具 ----------

func fingerprint(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// checkRequest 实现“按外部请求号幂等；同号异内容冲突”。
// 命中同内容请求时返回已记录的 incidentID（重放）；同号异内容返回 ErrConflict。
func (s *Store) checkRequestLocked(kind, requestID string, content any) (string, bool, error) {
	if strings.TrimSpace(requestID) == "" {
		return "", false, fmt.Errorf("%w: request id is required", ErrInvalidArgument)
	}
	fp := fingerprint(content)
	rec, ok := s.snap.Requests[requestID]
	if !ok {
		return "", false, nil
	}
	if rec.Kind != kind || rec.Fingerprint != fp {
		return "", false, fmt.Errorf("%w: request id %q reused with different payload", ErrConflict, requestID)
	}
	return rec.IncidentID, true, nil
}

func (s *Store) rememberRequestLocked(kind, requestID, incidentID string, content any) {
	s.snap.Requests[requestID] = requestRecord{
		Kind:        kind,
		Fingerprint: fingerprint(content),
		IncidentID:  incidentID,
	}
}

func (s *Store) appendHistoryLocked(incidentID, kind, detail string, now time.Time) {
	s.snap.History = append(s.snap.History, HistoryEntry{
		Time:       now,
		IncidentID: incidentID,
		Kind:       kind,
		Detail:     detail,
	})
}

func cloneSteps(in []Step) []Step {
	out := make([]Step, len(in))
	for i, st := range in {
		out[i] = Step{WaitBefore: st.WaitBefore, Targets: append([]string(nil), st.Targets...)}
	}
	return out
}

func intentKey(incidentID string, stepIndex int, target string) string {
	return fmt.Sprintf("%s\x00%d\x00%s", incidentID, stepIndex, target)
}

// ---------- 策略配置 ----------

func validatePolicy(in PolicyInput) error {
	if strings.TrimSpace(in.ID) == "" {
		return fmt.Errorf("%w: policy id is required", ErrInvalidArgument)
	}
	if strings.TrimSpace(in.Name) == "" {
		return fmt.Errorf("%w: policy name is required", ErrInvalidArgument)
	}
	if len(in.Steps) == 0 {
		return fmt.Errorf("%w: policy %q must contain at least one step", ErrInvalidArgument, in.ID)
	}
	seen := map[string]struct{}{}
	for i, step := range in.Steps {
		if step.WaitBefore < 0 {
			return fmt.Errorf("%w: step %d wait_before must be >= 0", ErrInvalidArgument, i)
		}
		if len(step.Targets) == 0 {
			return fmt.Errorf("%w: step %d must notify at least one target", ErrInvalidArgument, i)
		}
		for _, t := range step.Targets {
			if strings.TrimSpace(t) == "" {
				return fmt.Errorf("%w: step %d contains an empty target", ErrInvalidArgument, i)
			}
			if _, dup := seen[t]; dup {
				return fmt.Errorf("%w: target %q appears more than once in step %d", ErrInvalidArgument, t, i)
			}
			seen[t] = struct{}{}
		}
	}
	return nil
}

// CreatePolicy 创建一份升级策略。
func (s *Store) CreatePolicy(in PolicyInput, now time.Time) (*Policy, error) {
	if err := validatePolicy(in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.snap.Policies[in.ID]; exists {
		return nil, fmt.Errorf("%w: policy %q", ErrAlreadyExists, in.ID)
	}
	p := &Policy{
		ID:        in.ID,
		Name:      in.Name,
		Steps:     cloneSteps(in.Steps),
		CreatedAt: now,
		UpdatedAt: now,
		Version:   1,
	}
	s.snap.Policies[in.ID] = p
	if err := s.commitLocked(); err != nil {
		return nil, err
	}
	return clonePolicy(p), nil
}

// UpdatePolicy 更新策略。已创建的事件持有冻结快照，不会受到影响。
func (s *Store) UpdatePolicy(in PolicyInput, now time.Time) (*Policy, error) {
	if err := validatePolicy(in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.snap.Policies[in.ID]
	if !ok {
		return nil, fmt.Errorf("%w: policy %q", ErrNotFound, in.ID)
	}
	p.Name = in.Name
	p.Steps = cloneSteps(in.Steps)
	p.UpdatedAt = now
	p.Version++
	if err := s.commitLocked(); err != nil {
		return nil, err
	}
	return clonePolicy(p), nil
}

// GetPolicy 读取策略。
func (s *Store) GetPolicy(id string) (*Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.snap.Policies[id]
	if !ok {
		return nil, fmt.Errorf("%w: policy %q", ErrNotFound, id)
	}
	return clonePolicy(p), nil
}

// ListPolicies 列出全部策略，按 ID 排序。
func (s *Store) ListPolicies() []*Policy {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.snap.Policies))
	for id := range s.snap.Policies {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*Policy, 0, len(ids))
	for _, id := range ids {
		out = append(out, clonePolicy(s.snap.Policies[id]))
	}
	return out
}

func clonePolicy(p *Policy) *Policy {
	cp := *p
	cp.Steps = cloneSteps(p.Steps)
	return &cp
}

// ---------- 事件生命周期 ----------

// CreateIncident 创建事件：冻结当时策略、登记第 0 步的持久化定时器。
// 同 RequestID 的重复请求：内容相同则幂等重放原事件；内容不同返回 ErrConflict。
func (s *Store) CreateIncident(req CreateIncidentRequest, now time.Time) (*Incident, error) {
	if strings.TrimSpace(req.PolicyID) == "" {
		return nil, fmt.Errorf("%w: policy_id is required", ErrInvalidArgument)
	}
	content := struct {
		IncidentID string `json:"incident_id"`
		PolicyID   string `json:"policy_id"`
		Severity   string `json:"severity"`
		Detail     string `json:"detail"`
		OwnerGroup string `json:"owner_group"`
	}{req.IncidentID, req.PolicyID, req.Severity, req.Detail, req.OwnerGroup}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existingID, replay, err := s.checkRequestLocked("create_incident", req.RequestID, content); err != nil {
		return nil, err
	} else if replay {
		return s.getIncidentLocked(existingID)
	}

	p, ok := s.snap.Policies[req.PolicyID]
	if !ok {
		return nil, fmt.Errorf("%w: policy %q", ErrNotFound, req.PolicyID)
	}

	id := req.IncidentID
	if id == "" {
		s.snap.NextSeq++
		id = fmt.Sprintf("inc-%d", s.snap.NextSeq)
	}
	if _, exists := s.snap.Incidents[id]; exists {
		return nil, fmt.Errorf("%w: incident %q", ErrAlreadyExists, id)
	}

	owner := req.OwnerGroup
	if owner == "" {
		owner = DefaultGroup
	}
	inc := &Incident{
		ID:          id,
		PolicyID:    req.PolicyID,
		Severity:    req.Severity,
		Detail:      req.Detail,
		Status:      StatusFiring,
		OwnerGroup:  owner,
		FrozenSteps: cloneSteps(p.Steps), // 冻结：之后策略如何改都与本事件无关
		StartedAt:   now,
		FiredAt:     make([]time.Time, len(p.Steps)),
		SeverityChanges: []SeverityChange{{
			Time: now, From: "", To: req.Severity, Reason: "created",
		}},
		CreatedAt: now,
		UpdatedAt: now,
	}
	s.snap.Incidents[id] = inc
	// 第 0 步定时器与事件在同一次原子写入中持久化。
	s.snap.Timers[id] = &timerRecord{
		IncidentID: id,
		StepIndex:  0,
		FireAt:     now.Add(p.Steps[0].WaitBefore),
	}
	s.appendHistoryLocked(id, "created", fmt.Sprintf("policy=%s severity=%s", req.PolicyID, req.Severity), now)
	s.rememberRequestLocked("create_incident", req.RequestID, id, content)
	if err := s.commitLocked(); err != nil {
		return nil, err
	}
	return s.getIncidentLocked(id)
}

// Acknowledge 确认事件并停止升级：删除未触发的定时器，后续到期扫描不再产生通知。
func (s *Store) Acknowledge(req AckRequest, now time.Time) (*Incident, error) {
	content := struct {
		IncidentID     string `json:"incident_id"`
		AcknowledgedBy string `json:"acknowledged_by"`
	}{req.IncidentID, req.AcknowledgedBy}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existingID, replay, err := s.checkRequestLocked("acknowledge", req.RequestID, content); err != nil {
		return nil, err
	} else if replay {
		return s.getIncidentLocked(existingID)
	}

	inc, ok := s.snap.Incidents[req.IncidentID]
	if !ok {
		return nil, fmt.Errorf("%w: incident %q", ErrNotFound, req.IncidentID)
	}
	switch inc.Status {
	case StatusResolved:
		return nil, fmt.Errorf("%w: incident %q", ErrIncidentResolved, inc.ID)
	case StatusAcknowledged:
		return nil, fmt.Errorf("%w: incident %q", ErrAlreadyAcknowledged, inc.ID)
	}
	inc.Status = StatusAcknowledged
	inc.AcknowledgedAt = now
	inc.AcknowledgedBy = req.AcknowledgedBy
	inc.UpdatedAt = now
	delete(s.snap.Timers, inc.ID) // 状态与定时器删除原子落盘
	// 抑制期间暂缓的升级随确认永久撤销：确认即代表本事件不再需要升级。
	s.cancelHeldLocked(inc.ID, now, "acknowledged by "+req.AcknowledgedBy)
	s.appendHistoryLocked(inc.ID, "acknowledged", "by="+req.AcknowledgedBy, now)
	s.rememberRequestLocked("acknowledge", req.RequestID, inc.ID, content)
	if err := s.commitLocked(); err != nil {
		return nil, err
	}
	return s.getIncidentLocked(inc.ID)
}

// Resolve 解决事件。解决后不能再确认（返回 ErrIncidentResolved），
// 定时器被删除，到期扫描也不会再产生任何 outbox 通知。
func (s *Store) Resolve(req ResolveRequest, now time.Time) (*Incident, error) {
	content := struct {
		IncidentID string `json:"incident_id"`
		ResolvedBy string `json:"resolved_by"`
	}{req.IncidentID, req.ResolvedBy}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existingID, replay, err := s.checkRequestLocked("resolve", req.RequestID, content); err != nil {
		return nil, err
	} else if replay {
		return s.getIncidentLocked(existingID)
	}

	inc, ok := s.snap.Incidents[req.IncidentID]
	if !ok {
		return nil, fmt.Errorf("%w: incident %q", ErrNotFound, req.IncidentID)
	}
	if inc.Status == StatusResolved {
		return nil, fmt.Errorf("%w: incident %q", ErrAlreadyResolved, inc.ID)
	}
	inc.Status = StatusResolved
	inc.ResolvedAt = now
	inc.ResolvedBy = req.ResolvedBy
	inc.UpdatedAt = now
	delete(s.snap.Timers, inc.ID)
	// 已解决事件不再因为解除抑制而重新发通知：held 意图直接撤销。
	s.cancelHeldLocked(inc.ID, now, "resolved by "+req.ResolvedBy)
	// 解除条件为 on_resolve、且覆盖本事件的生效规则随解决自动解除。
	s.releaseOnResolveLocked(inc, now)
	s.appendHistoryLocked(inc.ID, "resolved", "by="+req.ResolvedBy, now)
	s.rememberRequestLocked("resolve", req.RequestID, inc.ID, content)
	if err := s.commitLocked(); err != nil {
		return nil, err
	}
	return s.getIncidentLocked(inc.ID)
}

// GetIncident 读取事件。
func (s *Store) GetIncident(id string) (*Incident, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getIncidentLocked(id)
}

func (s *Store) getIncidentLocked(id string) (*Incident, error) {
	inc, ok := s.snap.Incidents[id]
	if !ok {
		return nil, fmt.Errorf("%w: incident %q", ErrNotFound, id)
	}
	return cloneIncident(inc), nil
}

// ListIncidents 列出全部事件，按创建时间（即 ID 写入顺序）排序。
func (s *Store) ListIncidents() []*Incident {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Incident, 0, len(s.snap.Incidents))
	for _, inc := range s.snap.Incidents {
		out = append(out, cloneIncident(inc))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func cloneIncident(in *Incident) *Incident {
	cp := *in
	cp.FrozenSteps = cloneSteps(in.FrozenSteps)
	cp.FiredAt = append([]time.Time(nil), in.FiredAt...)
	cp.SeverityChanges = append([]SeverityChange(nil), in.SeverityChanges...)
	cp.Handoffs = append([]HandoffEntry(nil), in.Handoffs...)
	return &cp
}

// ---------- 定时器与到期推进 ----------

// NextDue 返回最近一个待触发定时器的时间；没有定时器时 ok 为 false。
// 重启后该信息完全来自持久化状态。
func (s *Store) NextDue() (fireAt time.Time, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.snap.Timers {
		if !ok || t.FireAt.Before(fireAt) {
			fireAt, ok = t.FireAt, true
		}
	}
	// 抑制窗口到期解除也是一个待处理时点。
	for _, rule := range s.snap.Suppressions {
		if rule.Status != SuppressionActive {
			continue
		}
		if !ok || rule.ValidUntil.Before(fireAt) {
			fireAt, ok = rule.ValidUntil, true
		}
	}
	return fireAt, ok
}

// Timer 返回某事件当前持久化的定时器（调试/测试用）。
func (s *Store) Timer(incidentID string) (Timer, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.snap.Timers[incidentID]
	if !ok {
		return Timer{}, false
	}
	return *t, true
}

// ProcessDue 扫描到期事项并推进，顺序为：先解除到期抑制窗口并补发/撤销其
// held 通知，再推进到期的升级定时器。所有动作在同一把锁内原子落盘。
// 关键并发/重放保证：
//   - 每个事件每次调用至多推进一步（到期多步也不会一次跨过多个步骤）；
//   - 仅对仍处于 firing 的事件推进，确认/解决/交接/抑制解除与触发竞态时，
//     最终只有一个合法状态；
//   - 通知意图按 (事件, 步骤, 目标) 去重，重复扫描不会产生重复 outbox；
//   - 抑制窗口内升级仍正常触发，但意图记为 held（带抑制原因），绝不派发；
//   - held 意图在解除抑制时按事件“最新级别”重新判断：仍在范围且仍 firing
//     才补发（每一级仍只发一次），否则撤销；已确认/解决事件绝不补发。
//
// 返回本次实际推进的升级步骤数（解除窗口/补发不计入）。
func (s *Store) ProcessDue(now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	changed := false
	// 阶段 1：窗口到期自动解除。manual 允许随时提前手工解除，但 ValidUntil
	// 仍是硬结束点（否则窗口结束后 held 意图会无人裁决）；on_resolve 若一直
	// 没解决也以 ValidUntil 兜底解除。
	ruleIDs := make([]string, 0, len(s.snap.Suppressions))
	for id, rule := range s.snap.Suppressions {
		if rule.Status == SuppressionActive &&
			!rule.ValidUntil.After(now) &&
			!now.Before(rule.ValidFrom) {
			ruleIDs = append(ruleIDs, id)
		}
	}
	sort.Strings(ruleIDs)
	for _, id := range ruleIDs {
		s.releaseRuleLocked(s.snap.Suppressions[id], now, "system", string(ReleaseWindowEnd))
		changed = true
	}

	// 固定处理顺序，使行为与历史可预测。
	ids := make([]string, 0, len(s.snap.Timers))
	for id := range s.snap.Timers {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	advanced := 0
	for _, id := range ids {
		t := s.snap.Timers[id]
		if t == nil || t.FireAt.After(now) {
			continue // 尚未到期
		}
		inc := s.snap.Incidents[id]
		if inc == nil {
			delete(s.snap.Timers, id)
			continue
		}
		// 与确认/解决竞态：以事件状态为准，非 firing 绝不通知。
		if inc.Status != StatusFiring {
			delete(s.snap.Timers, id)
			continue
		}
		idx := inc.NextStepIndex
		if idx != t.StepIndex || idx >= len(inc.FrozenSteps) {
			// 定时器与进度不一致（理论上不应出现）：丢弃过期定时器，
			// 不做任何推进，交由状态自身的下一步定时器驱动。
			delete(s.snap.Timers, id)
			continue
		}

		step := inc.FrozenSteps[idx]
		// 维护窗口抑制：升级照常触发并推进，但通知意图先记为 held，
		// 带上命中的抑制规则与原因，解除时再统一裁决。
		sup := s.coveringSuppressionLocked(inc, now)
		var targets []string
		for _, target := range step.Targets {
			key := intentKey(id, idx, target)
			if _, dup := s.snap.SentIntents[key]; dup {
				continue // 同一事件、同一步骤、同一目标只有一次通知意图
			}
			s.snap.SentIntents[key] = struct{}{}
			s.snap.NextOutboxID++
			item := &OutboxItem{
				ID:         s.snap.NextOutboxID,
				IncidentID: id,
				StepIndex:  idx,
				Target:     target,
				Channel:    "default",
				Kind:       OutboxKindEscalation,
				OwnerGroup: inc.OwnerGroup,
				Message: fmt.Sprintf("[incident %s] escalation step %d: %s",
					id, idx, inc.Detail),
				Status:    OutboxPending,
				CreatedAt: now,
			}
			// 抑制窗口内：触发但暂缓，记录规则与原因，解除时再裁决。
			if sup != nil {
				item.Status = OutboxHeld
				item.HeldAt = now
				item.SuppressionID = sup.ID
				item.SuppressionReason = sup.Reason
			}
			s.snap.Outbox = append(s.snap.Outbox, item)
			targets = append(targets, target)
		}

		inc.FiredAt[idx] = now
		inc.NextStepIndex = idx + 1
		inc.UpdatedAt = now
		if sup != nil {
			s.appendHistoryLocked(id, "step_suppressed",
				fmt.Sprintf("step=%d targets=%s suppression=%s reason=%s",
					idx, strings.Join(targets, ","), sup.ID, sup.Reason), now)
		} else {
			s.appendHistoryLocked(id, "step_fired",
				fmt.Sprintf("step=%d targets=%s", idx, strings.Join(targets, ",")), now)
		}

		// 推进一步后立即重新登记下一步定时器（基于实际触发时间等待），
		// 或在最后一步后清除定时器。
		if next := idx + 1; next < len(inc.FrozenSteps) {
			s.snap.Timers[id] = &timerRecord{
				IncidentID: id,
				StepIndex:  next,
				FireAt:     now.Add(inc.FrozenSteps[next].WaitBefore),
			}
		} else {
			delete(s.snap.Timers, id)
		}
		advanced++
		changed = true

		// 级别在抑制期间变化到抑制范围之外时，真正需要升级的通知不能被吞：
		// 立即重新裁决该事件所有 held 意图（无覆盖规则即补发）。
		// （UpdateSeverity 也会同步触发一次，这里覆盖“规则随时间失效”的情形。）
		if s.reassessHeldLocked(inc, now) {
			changed = true
		}
	}
	if changed {
		if err := s.commitLocked(); err != nil {
			return 0, err
		}
	}
	return advanced, nil
}

// ---------- Outbox ----------

// PendingOutbox 返回所有尚未投递的通知意图，按 ID 排序。
func (s *Store) PendingOutbox() []*OutboxItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingOutboxLocked("")
}

// PendingOutboxFor 返回指定值班组负责派发的未投递意图，按 ID 排序。
// 交接后旧班组在这里不再看到已改派的意图，新班组接着处理，
// 同一条 pending 意图任何时候只有一方可见。held/canceled 意图不可见。
func (s *Store) PendingOutboxFor(group string) []*OutboxItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingOutboxLocked(group)
}

func (s *Store) pendingOutboxLocked(group string) []*OutboxItem {
	var out []*OutboxItem
	for _, item := range s.snap.Outbox {
		if item.Status != OutboxPending {
			continue
		}
		if group != "" && item.OwnerGroup != group {
			continue
		}
		cp := *item
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ListOutbox 返回某事件（空字符串表示全部）的通知意图。
func (s *Store) ListOutbox(incidentID string) []*OutboxItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*OutboxItem
	for _, item := range s.snap.Outbox {
		if incidentID == "" || item.IncidentID == incidentID {
			cp := *item
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// MarkDelivered 将一条 outbox 意图标记为已投递。
// 投递与状态落盘是分离的：崩溃会导致重投（at-least-once），
// 通知渠道应以 item.ID / (事件,步骤,目标) 做幂等。
func (s *Store) MarkDelivered(id int64, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, item := range s.snap.Outbox {
		if item.ID == id {
			if item.Status == OutboxPending {
				item.Status = OutboxDelivered
				item.DeliveredAt = now
				return s.commitLocked()
			}
			return nil
		}
	}
	return fmt.Errorf("%w: outbox item %d", ErrNotFound, id)
}

// ---------- 事件级别变更 ----------

// UpdateSeverity 更新事件级别。级别变化始终记录到 SeverityChanges 并立即可见：
//   - 抑制期间变化同样持续可见，但不发通知；若新级别已逃出全部抑制规则
//     的范围，真正需要升级的通知立即恢复，绝不被窗口吞掉；
//   - 解除抑制时一律以最后一条 SeverityChange 的级别为准裁决。
//
// 同 RequestID 幂等；已解决事件不允许再改级别。
func (s *Store) UpdateSeverity(req UpdateSeverityRequest, now time.Time) (*Incident, error) {
	if strings.TrimSpace(req.IncidentID) == "" {
		return nil, fmt.Errorf("%w: incident_id is required", ErrInvalidArgument)
	}
	if strings.TrimSpace(req.Severity) == "" {
		return nil, fmt.Errorf("%w: severity is required", ErrInvalidArgument)
	}
	content := struct {
		IncidentID string `json:"incident_id"`
		Severity   string `json:"severity"`
		Reason     string `json:"reason"`
		UpdatedBy  string `json:"updated_by"`
	}{req.IncidentID, req.Severity, req.Reason, req.UpdatedBy}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existingID, replay, err := s.checkRequestLocked("update_severity", req.RequestID, content); err != nil {
		return nil, err
	} else if replay {
		return s.getIncidentLocked(existingID)
	}

	inc, ok := s.snap.Incidents[req.IncidentID]
	if !ok {
		return nil, fmt.Errorf("%w: incident %q", ErrNotFound, req.IncidentID)
	}
	if inc.Status == StatusResolved {
		return nil, fmt.Errorf("%w: incident %q", ErrIncidentResolved, inc.ID)
	}

	levelChanged := false
	if inc.Severity != req.Severity {
		from := inc.Severity
		// “变更发生时是否在抑制中”按变更前的级别/状态判定，
		// 这样“逃出抑制范围”的那次变化也会被正确标记为发生在窗口内。
		suppressed := s.coveringSuppressionLocked(inc, now) != nil
		inc.Severity = req.Severity
		inc.SeverityChanges = append(inc.SeverityChanges, SeverityChange{
			Time:       now,
			From:       from,
			To:         req.Severity,
			Reason:     req.Reason,
			By:         req.UpdatedBy,
			Suppressed: suppressed,
		})
		inc.UpdatedAt = now
		s.appendHistoryLocked(inc.ID, "severity_changed",
			fmt.Sprintf("from=%s to=%s reason=%s", from, req.Severity, req.Reason), now)
		// 新级别逃出抑制范围（或窗口已过期）时，held 的升级立即恢复。
		levelChanged = true
		s.reassessHeldLocked(inc, now)
	}
	s.rememberRequestLocked("update_severity", req.RequestID, inc.ID, content)
	if levelChanged {
		if err := s.commitLocked(); err != nil {
			return nil, err
		}
	}
	return s.getIncidentLocked(inc.ID)
}

// ---------- 值班交接 ----------

// Handoff 执行值班交接：在生效瞬间冻结“FromGroup 名下仍处于 firing 的事件”清单，
// 并在同一次原子写入中完成：
//   - 移交事件的负责人改为 ToGroup，并在事件上追加一条 HandoffEntry
//     （同一事件多次交接时每次接手人与时间都保留）；
//   - 这些事件尚未派发的意图（pending 与抑制 held）随事件改派给 ToGroup，
//     已投递的保留原记录，旧班次不能再派发；
//   - 为每个移交事件生成一条发给 ToGroup 的交接通知（按交接单号去重）。
//
// 已确认/已解决的事件不移交；与确认/解决/升级触发/抑制解除并发时由同一把
// 互斥锁串行化，最终只收敛出一个结果。同 RequestID 重放返回原交接单，
// 不会再次更换负责人或重复通知。
func (s *Store) Handoff(req HandoffRequest, now time.Time) (*HandoffRecord, error) {
	if strings.TrimSpace(req.FromGroup) == "" || strings.TrimSpace(req.ToGroup) == "" {
		return nil, fmt.Errorf("%w: from_group and to_group are required", ErrInvalidArgument)
	}
	if req.FromGroup == req.ToGroup {
		return nil, fmt.Errorf("%w: from_group and to_group must differ", ErrInvalidArgument)
	}
	content := struct {
		HandoffID string `json:"handoff_id"`
		FromGroup string `json:"from_group"`
		ToGroup   string `json:"to_group"`
	}{req.HandoffID, req.FromGroup, req.ToGroup}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existingID, replay, err := s.checkRequestLocked("handoff", req.RequestID, content); err != nil {
		return nil, err
	} else if replay {
		return s.getHandoffLocked(existingID)
	}

	id := req.HandoffID
	if id == "" {
		s.snap.NextSeq++
		id = fmt.Sprintf("ho-%d", s.snap.NextSeq)
	}
	if _, exists := s.snap.Handoffs[id]; exists {
		return nil, fmt.Errorf("%w: handoff %q", ErrAlreadyExists, id)
	}

	// 冻结待移交清单：仅当前归 FromGroup 且仍 firing 的事件。
	var ids []string
	for _, inc := range s.snap.Incidents {
		if inc.OwnerGroup == req.FromGroup && inc.Status == StatusFiring {
			ids = append(ids, inc.ID)
		}
	}
	sort.Strings(ids)

	for _, incID := range ids {
		inc := s.snap.Incidents[incID]
		inc.OwnerGroup = req.ToGroup
		inc.UpdatedAt = now
		inc.Handoffs = append(inc.Handoffs, HandoffEntry{
			HandoffID: id, FromGroup: req.FromGroup, ToGroup: req.ToGroup, Time: now,
		})
		// 尚未派发的意图（含抑制 held）只能由一方继续处理：随事件原子改派；
		// 已投递/已撤销的意图是历史事实，保留原记录不动。
		for _, item := range s.snap.Outbox {
			if item.IncidentID != incID {
				continue
			}
			if item.Status == OutboxPending || item.Status == OutboxHeld {
				item.OwnerGroup = req.ToGroup
			}
		}
		// 交接通知：同一事件、同一交接单只产生一条意图。
		key := intentKey(incID, -1, "handoff:"+id)
		if _, dup := s.snap.SentIntents[key]; !dup {
			s.snap.SentIntents[key] = struct{}{}
			s.snap.NextOutboxID++
			s.snap.Outbox = append(s.snap.Outbox, &OutboxItem{
				ID:         s.snap.NextOutboxID,
				IncidentID: incID,
				StepIndex:  -1,
				Target:     req.ToGroup,
				Channel:    "default",
				Kind:       OutboxKindHandoff,
				OwnerGroup: req.ToGroup,
				Message: fmt.Sprintf("[incident %s] handed off from %s to %s: %s",
					incID, req.FromGroup, req.ToGroup, inc.Detail),
				Status:    OutboxPending,
				CreatedAt: now,
			})
		}
		s.appendHistoryLocked(incID, "handoff",
			fmt.Sprintf("handoff=%s from=%s to=%s", id, req.FromGroup, req.ToGroup), now)
	}

	rec := &HandoffRecord{
		ID:          id,
		RequestID:   req.RequestID,
		FromGroup:   req.FromGroup,
		ToGroup:     req.ToGroup,
		IncidentIDs: ids,
		CreatedAt:   now,
	}
	s.snap.Handoffs[id] = rec
	s.rememberRequestLocked("handoff", req.RequestID, id, content)
	if err := s.commitLocked(); err != nil {
		return nil, err
	}
	return cloneHandoff(rec), nil
}

// GetHandoff 读取交接单。
func (s *Store) GetHandoff(id string) (*HandoffRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getHandoffLocked(id)
}

func (s *Store) getHandoffLocked(id string) (*HandoffRecord, error) {
	rec, ok := s.snap.Handoffs[id]
	if !ok {
		return nil, fmt.Errorf("%w: handoff %q", ErrNotFound, id)
	}
	return cloneHandoff(rec), nil
}

// ListHandoffs 列出全部交接单，按生效时间排序。
func (s *Store) ListHandoffs() []*HandoffRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*HandoffRecord, 0, len(s.snap.Handoffs))
	for _, rec := range s.snap.Handoffs {
		out = append(out, cloneHandoff(rec))
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func cloneHandoff(rec *HandoffRecord) *HandoffRecord {
	cp := *rec
	cp.IncidentIDs = append([]string(nil), rec.IncidentIDs...)
	return &cp
}

// ---------- 维护窗口抑制 ----------

func validateSuppression(in CreateSuppressionRequest) error {
	if strings.TrimSpace(in.Reason) == "" {
		return fmt.Errorf("%w: suppression reason is required", ErrInvalidArgument)
	}
	if !in.ValidUntil.After(in.ValidFrom) {
		return fmt.Errorf("%w: valid_until must be after valid_from", ErrInvalidArgument)
	}
	switch in.ReleaseCondition {
	case "", ReleaseManual, ReleaseWindowEnd, ReleaseOnResolve:
	default:
		return fmt.Errorf("%w: unknown release_condition %q", ErrInvalidArgument, in.ReleaseCondition)
	}
	for _, id := range in.Scope.IncidentIDs {
		if strings.TrimSpace(id) == "" {
			return fmt.Errorf("%w: scope contains an empty incident id", ErrInvalidArgument)
		}
	}
	for _, sev := range in.Scope.Severities {
		if strings.TrimSpace(sev) == "" {
			return fmt.Errorf("%w: scope contains an empty severity", ErrInvalidArgument)
		}
	}
	return nil
}

// normalizeScope 复制并排序范围字段，保证指纹稳定、规则比较确定。
func normalizeScope(sc SuppressionScope) SuppressionScope {
	out := SuppressionScope{PolicyID: sc.PolicyID}
	out.IncidentIDs = append([]string(nil), sc.IncidentIDs...)
	out.Severities = append([]string(nil), sc.Severities...)
	sort.Strings(out.IncidentIDs)
	sort.Strings(out.Severities)
	return out
}

// ruleActiveAt 判断规则在 t 时刻是否处于抑制状态：未解除且在有效窗口内
// （半开区间 [ValidFrom, ValidUntil)：from 时刻已生效，until 时刻已失效）。
func ruleActiveAt(r *SuppressionRule, t time.Time) bool {
	return r.Status == SuppressionActive &&
		!t.Before(r.ValidFrom) && t.Before(r.ValidUntil)
}

// activeRulesAtLocked 返回 t 时刻生效的规则，按 (ValidFrom, ID) 稳定排序。
func (s *Store) activeRulesAtLocked(t time.Time) []*SuppressionRule {
	out := make([]*SuppressionRule, 0, len(s.snap.Suppressions))
	for _, r := range s.snap.Suppressions {
		if ruleActiveAt(r, t) {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].ValidFrom.Equal(out[j].ValidFrom) {
			return out[i].ValidFrom.Before(out[j].ValidFrom)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// coveringSuppressionLocked 返回 t 时刻覆盖某事件的生效规则（多条时取最早生效的），
// 没有则返回 nil。是否覆盖一律基于事件“当前最新级别”。
func (s *Store) coveringSuppressionLocked(inc *Incident, t time.Time) *SuppressionRule {
	for _, r := range s.activeRulesAtLocked(t) {
		if r.Scope.covers(inc) {
			return r
		}
	}
	return nil
}

// covers 判断范围是否覆盖某事件：各条件为“与”，零值范围覆盖全部。
func (sc SuppressionScope) covers(inc *Incident) bool {
	if sc.PolicyID != "" && inc.PolicyID != sc.PolicyID {
		return false
	}
	if len(sc.Severities) > 0 && !containsString(sc.Severities, inc.Severity) {
		return false
	}
	if len(sc.IncidentIDs) > 0 && !containsString(sc.IncidentIDs, inc.ID) {
		return false
	}
	return true
}

func containsString(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// equal 判断两个已归一化的范围是否相同。
func (sc SuppressionScope) equal(other SuppressionScope) bool {
	return sc.PolicyID == other.PolicyID &&
		stringSliceEqual(sc.IncidentIDs, other.IncidentIDs) &&
		stringSliceEqual(sc.Severities, other.Severities)
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func cloneSuppression(r *SuppressionRule) *SuppressionRule {
	cp := *r
	cp.Scope = normalizeScope(r.Scope)
	return &cp
}

// CreateSuppression 创建一条维护窗口抑制规则。
// 同一时间只允许存在一条“内容相同”的生效规则：内容身份为
// （事件范围 + 抑制原因）。即使解除时间（ValidUntil）或解除条件不同，
// 也不会静默延长或覆盖已有窗口，而是返回 ErrAlreadyExists，并在错误信息中
// 明确指出已有规则 ID 及其解除时间。同 RequestID 同内容幂等重放。
func (s *Store) CreateSuppression(req CreateSuppressionRequest, now time.Time) (*SuppressionRule, error) {
	if err := validateSuppression(req); err != nil {
		return nil, err
	}
	scope := normalizeScope(req.Scope)
	cond := req.ReleaseCondition
	if cond == "" {
		cond = ReleaseWindowEnd
	}
	content := struct {
		RuleID  string           `json:"rule_id"`
		Scope   SuppressionScope `json:"scope"`
		Reason  string           `json:"reason"`
		From    time.Time        `json:"valid_from"`
		Until   time.Time        `json:"valid_until"`
		Cond    ReleaseCondition `json:"release_condition"`
		Creator string           `json:"created_by"`
	}{req.RuleID, scope, req.Reason, req.ValidFrom, req.ValidUntil, cond, req.CreatedBy}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existingID, replay, err := s.checkRequestLocked("create_suppression", req.RequestID, content); err != nil {
		return nil, err
	} else if replay {
		return s.getSuppressionLocked(existingID)
	}

	// 范围里显式指定的策略/事件必须存在；级别是自由字符串，不做外键校验。
	if scope.PolicyID != "" {
		if _, ok := s.snap.Policies[scope.PolicyID]; !ok {
			return nil, fmt.Errorf("%w: policy %q", ErrNotFound, scope.PolicyID)
		}
	}
	for _, id := range scope.IncidentIDs {
		if _, ok := s.snap.Incidents[id]; !ok {
			return nil, fmt.Errorf("%w: incident %q", ErrNotFound, id)
		}
	}

	// 内容相同（范围 + 原因）的生效规则：即使解除时间/解除条件不同，
	// 也明确拒绝，绝不静默延长或覆盖原窗口。
	for _, ex := range s.snap.Suppressions {
		if ex.Status == SuppressionActive && ex.Reason == req.Reason && ex.Scope.equal(scope) {
			return nil, fmt.Errorf(
				"%w: active suppression %q already covers the same scope with reason %q (valid_until=%s)",
				ErrAlreadyExists, ex.ID, ex.Reason, ex.ValidUntil.Format(time.RFC3339))
		}
	}

	id := req.RuleID
	if id == "" {
		s.snap.NextSeq++
		id = fmt.Sprintf("sup-%d", s.snap.NextSeq)
	}
	if _, exists := s.snap.Suppressions[id]; exists {
		return nil, fmt.Errorf("%w: suppression %q", ErrAlreadyExists, id)
	}

	rule := &SuppressionRule{
		ID:               id,
		Scope:            scope,
		Reason:           req.Reason,
		ValidFrom:        req.ValidFrom,
		ValidUntil:       req.ValidUntil,
		ReleaseCondition: cond,
		Status:           SuppressionActive,
		CreatedBy:        req.CreatedBy,
		CreatedAt:        now,
	}
	s.snap.Suppressions[id] = rule
	s.rememberRequestLocked("create_suppression", req.RequestID, id, content)
	// 规则不溯及既往：已经 pending 的意图照常投递；创建规则本身不改变任何
	// 已存在 outbox 的状态，只有之后触发的升级才会按当时规则被 held。
	s.appendHistoryLocked("", "suppression_created",
		fmt.Sprintf("suppression=%s reason=%s from=%s until=%s",
			id, req.Reason, req.ValidFrom.Format(time.RFC3339), req.ValidUntil.Format(time.RFC3339)), now)
	if err := s.commitLocked(); err != nil {
		return nil, err
	}
	return cloneSuppression(rule), nil
}

func (s *Store) getSuppressionLocked(id string) (*SuppressionRule, error) {
	rule, ok := s.snap.Suppressions[id]
	if !ok {
		return nil, fmt.Errorf("%w: suppression %q", ErrNotFound, id)
	}
	return cloneSuppression(rule), nil
}

// ReleaseSuppression 手工解除一条抑制规则，并立即裁决暂缓的升级：
// 事件仍 firing 且按最新级别不再被任何生效规则覆盖 → held 转 pending 补发
// （每级仍最多一次有效通知）；已确认/解决的事件绝不补发。
// 已解除的规则重复解除返回 ErrConflict；同 RequestID 幂等。
func (s *Store) ReleaseSuppression(req ReleaseSuppressionRequest, now time.Time) (*SuppressionRule, error) {
	if strings.TrimSpace(req.RuleID) == "" {
		return nil, fmt.Errorf("%w: rule_id is required", ErrInvalidArgument)
	}
	content := struct {
		RuleID     string `json:"rule_id"`
		ReleasedBy string `json:"released_by"`
	}{req.RuleID, req.ReleasedBy}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existingID, replay, err := s.checkRequestLocked("release_suppression", req.RequestID, content); err != nil {
		return nil, err
	} else if replay {
		return s.getSuppressionLocked(existingID)
	}

	rule, ok := s.snap.Suppressions[req.RuleID]
	if !ok {
		return nil, fmt.Errorf("%w: suppression %q", ErrNotFound, req.RuleID)
	}
	if rule.Status != SuppressionActive {
		return nil, fmt.Errorf("%w: suppression %q already released at %s",
			ErrConflict, rule.ID, rule.ReleasedAt.Format(time.RFC3339))
	}
	s.releaseRuleLocked(rule, now, req.ReleasedBy, "manual")
	s.rememberRequestLocked("release_suppression", req.RequestID, rule.ID, content)
	if err := s.commitLocked(); err != nil {
		return nil, err
	}
	return cloneSuppression(rule), nil
}

// releaseRuleLocked 把规则标记为已解除，并对全部 held 意图按最新状态重新裁决。
// 调用方负责加锁与落盘。
func (s *Store) releaseRuleLocked(rule *SuppressionRule, now time.Time, by, cause string) {
	rule.Status = SuppressionReleased
	rule.ReleasedAt = now
	rule.ReleasedBy = by
	rule.ReleaseCause = cause
	s.appendHistoryLocked("", "suppression_released",
		fmt.Sprintf("suppression=%s by=%s cause=%s", rule.ID, by, cause), now)

	incidents := map[string]*Incident{}
	for _, item := range s.snap.Outbox {
		if item.Status != OutboxHeld {
			continue
		}
		inc, ok := incidents[item.IncidentID]
		if !ok {
			inc = s.snap.Incidents[item.IncidentID]
			incidents[item.IncidentID] = inc
		}
		if inc == nil {
			item.Status = OutboxCanceled
			item.CanceledAt = now
			item.CanceledReason = "incident missing"
			continue
		}
		s.reassessOneHeldLocked(inc, item, now)
	}
}

// releaseOnResolveLocked 处理事件解决：覆盖该事件且解除条件为 on_resolve 的
// 生效规则自动解除（其 held 意图已在 Resolve 中先行撤销，不会补发）。
func (s *Store) releaseOnResolveLocked(inc *Incident, now time.Time) {
	var toRelease []*SuppressionRule
	for _, r := range s.activeRulesAtLocked(now) {
		if r.ReleaseCondition == ReleaseOnResolve && r.Scope.covers(inc) {
			toRelease = append(toRelease, r)
		}
	}
	for _, r := range toRelease {
		s.releaseRuleLocked(r, now, "system", string(ReleaseOnResolve))
	}
}

// cancelHeldLocked 撤销某事件全部 held 意图（确认/解决时调用）。
func (s *Store) cancelHeldLocked(incidentID string, now time.Time, reason string) {
	for _, item := range s.snap.Outbox {
		if item.IncidentID == incidentID && item.Status == OutboxHeld {
			item.Status = OutboxCanceled
			item.CanceledAt = now
			item.CanceledReason = reason
		}
	}
}

// reassessHeldLocked 重新裁决某事件的全部 held 意图；有状态变化返回 true。
// 触发时机：级别变更、规则解除、到期扫描发现窗口已随时间失效。
func (s *Store) reassessHeldLocked(inc *Incident, now time.Time) bool {
	changed := false
	for _, item := range s.snap.Outbox {
		if item.IncidentID == inc.ID && item.Status == OutboxHeld {
			if s.reassessOneHeldLocked(inc, item, now) {
				changed = true
			}
		}
	}
	return changed
}

// reassessOneHeldLocked 按事件“最新级别/状态”裁决单条 held 意图：
//   - 已确认/解决：撤销（已解决事件绝不能因解除抑制重新通知）；
//   - 仍被某条生效规则覆盖（最新级别仍在范围内）：保持 held，并把归属
//     更新到当前覆盖规则，抑制原因始终展示为最新；
//   - 不再被任何规则覆盖（窗口解除 / 级别逃出范围 / 窗口到期）：
//     恢复为 pending 立即补发。每一级从头到尾最多形成一次有效通知。
func (s *Store) reassessOneHeldLocked(inc *Incident, item *OutboxItem, now time.Time) bool {
	if inc.Status != StatusFiring {
		item.Status = OutboxCanceled
		item.CanceledAt = now
		item.CanceledReason = "incident " + string(inc.Status)
		s.appendHistoryLocked(inc.ID, "suppressed_canceled",
			fmt.Sprintf("step=%d target=%s status=%s", item.StepIndex, item.Target, inc.Status), now)
		return true
	}
	if sup := s.coveringSuppressionLocked(inc, now); sup != nil {
		if item.SuppressionID != sup.ID || item.SuppressionReason != sup.Reason {
			item.SuppressionID = sup.ID
			item.SuppressionReason = sup.Reason
			return true
		}
		return false
	}
	item.Status = OutboxPending
	item.Kind = OutboxKindSuppressionResume
	item.ResumedAt = now
	s.appendHistoryLocked(inc.ID, "suppression_resumed",
		fmt.Sprintf("step=%d target=%s severity=%s", item.StepIndex, item.Target, inc.Severity), now)
	return true
}

// GetSuppression 读取一条抑制规则。
func (s *Store) GetSuppression(id string) (*SuppressionRule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getSuppressionLocked(id)
}

// ListSuppressions 列出全部抑制规则（含已解除），按创建时间排序。
func (s *Store) ListSuppressions() []*SuppressionRule {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*SuppressionRule, 0, len(s.snap.Suppressions))
	for _, rule := range s.snap.Suppressions {
		out = append(out, cloneSuppression(rule))
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// GetIncidentView 返回事件综合查询视图：事件本体（含级别轨迹与每次交接的
// 接手人/时间）、每一级升级及其抑制原因与最终去向（pending/held/delivered/
// canceled/补发）、now 时刻仍覆盖本事件的抑制规则。抑制期间事件级别变化
// 通过 Incident.SeverityChanges 持续可见。
func (s *Store) GetIncidentView(id string, now time.Time) (*IncidentView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inc, err := s.getIncidentLocked(id)
	if err != nil {
		return nil, err
	}
	view := &IncidentView{Incident: inc}
	for _, item := range s.snap.Outbox {
		if item.IncidentID != id {
			continue
		}
		if item.Kind != OutboxKindEscalation && item.Kind != OutboxKindSuppressionResume {
			continue
		}
		fired := false
		if item.StepIndex >= 0 && item.StepIndex < len(inc.FiredAt) {
			fired = !inc.FiredAt[item.StepIndex].IsZero()
		}
		view.Escalations = append(view.Escalations, EscalationView{
			StepIndex:         item.StepIndex,
			Target:            item.Target,
			Kind:              item.Kind,
			Status:            item.Status,
			OwnerGroup:        item.OwnerGroup,
			CreatedAt:         item.CreatedAt,
			Fired:             fired,
			SuppressionID:     item.SuppressionID,
			SuppressionReason: item.SuppressionReason,
			ResumedAt:         item.ResumedAt,
			CanceledAt:        item.CanceledAt,
			CanceledReason:    item.CanceledReason,
		})
	}
	sort.Slice(view.Escalations, func(i, j int) bool {
		if !view.Escalations[i].CreatedAt.Equal(view.Escalations[j].CreatedAt) {
			return view.Escalations[i].CreatedAt.Before(view.Escalations[j].CreatedAt)
		}
		return view.Escalations[i].Target < view.Escalations[j].Target
	})
	for _, rule := range s.activeRulesAtLocked(now) {
		if rule.Scope.covers(inc) {
			view.ActiveSuppressions = append(view.ActiveSuppressions, cloneSuppression(rule))
		}
	}
	return view, nil
}

// History 返回指定事件的历史；incidentID 为空时返回全部历史。
func (s *Store) History(incidentID string) ([]HistoryEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if incidentID != "" {
		if _, ok := s.snap.Incidents[incidentID]; !ok {
			return nil, fmt.Errorf("%w: incident %q", ErrNotFound, incidentID)
		}
	}
	var out []HistoryEntry
	for _, h := range s.snap.History {
		if incidentID == "" || h.IncidentID == incidentID {
			out = append(out, h)
		}
	}
	return out, nil
}
