package incidentescalation

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// SuppressionInput 是创建抑制规则时由调用方提供的内容。
type SuppressionInput struct {
	// RuleID 可选，为空自动生成。
	RuleID    string
	Scope     SuppressionScope
	Reason    string
	StartsAt  time.Time
	EndsAt    time.Time
	Condition SuppressionReleaseCondition // 为空时按 ReleaseAtWindowEnd 处理
	CreatedBy string
}

// SuppressionRequest 在 SuppressionInput 基础上携带外部请求号（幂等键）。
type SuppressionRequest struct {
	RequestID string
	SuppressionInput
}

// ReleaseSuppressionRequest 手工解除抑制规则。RequestID 是外部请求号，用于幂等。
type ReleaseSuppressionRequest struct {
	RequestID  string
	RuleID     string
	ReleasedBy string
}

// HandoverRequest 交接事件：记录移交人、接手人与时间。同一事件的每次交接都保留。
type HandoverRequest struct {
	RequestID  string
	IncidentID string
	From       string
	To         string
	Note       string
}

// ChangeSeverityRequest 调整事件级别。抑制期间也允许调整，且持续可见。
type ChangeSeverityRequest struct {
	RequestID  string
	IncidentID string
	Severity   string
	Reason     string
	ChangedBy  string
}

// normalizeScope 去掉范围各列表中的空白项并去重排序，便于匹配与规则指纹计算。
func normalizeScope(in SuppressionScope) SuppressionScope {
	clean := func(values []string) []string {
		seen := map[string]struct{}{}
		var out []string
		for _, v := range values {
			v = strings.TrimSpace(v)
			if v == "" {
				continue
			}
			if _, dup := seen[v]; dup {
				continue
			}
			seen[v] = struct{}{}
			out = append(out, v)
		}
		sort.Strings(out)
		return out
	}
	return SuppressionScope{
		IncidentIDs: clean(in.IncidentIDs),
		Severities:  clean(in.Severities),
		PolicyIDs:   clean(in.PolicyIDs),
	}
}

func (sc SuppressionScope) empty() bool {
	return len(sc.IncidentIDs) == 0 && len(sc.Severities) == 0 && len(sc.PolicyIDs) == 0
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

// scopeMatches 判断规则范围是否覆盖某事件。空范围覆盖全部事件。
func (sc SuppressionScope) scopeMatches(inc *Incident) bool {
	if sc.empty() {
		return true
	}
	if len(sc.IncidentIDs) > 0 && contains(sc.IncidentIDs, inc.ID) {
		return true
	}
	if inc.Severity != "" && len(sc.Severities) > 0 && contains(sc.Severities, inc.Severity) {
		return true
	}
	if len(sc.PolicyIDs) > 0 && contains(sc.PolicyIDs, inc.PolicyID) {
		return true
	}
	return false
}

func validateSuppression(in SuppressionInput) error {
	if strings.TrimSpace(in.Reason) == "" {
		return fmt.Errorf("%w: suppression reason is required", ErrInvalidArgument)
	}
	if in.StartsAt.IsZero() || in.EndsAt.IsZero() {
		return fmt.Errorf("%w: suppression window start and end are required", ErrInvalidArgument)
	}
	if !in.EndsAt.After(in.StartsAt) {
		return fmt.Errorf("%w: suppression window end must be after start", ErrInvalidArgument)
	}
	switch in.Condition {
	case "", ReleaseAtWindowEnd, ReleaseManual:
	default:
		return fmt.Errorf("%w: unknown release condition %q", ErrInvalidArgument, in.Condition)
	}
	return nil
}

// scopeFingerprint 对归一化后的范围内容计算稳定指纹。
func scopeFingerprint(scope SuppressionScope) string {
	b, _ := json.Marshal(scope)
	return string(b)
}

// suppressionFingerprint 标识“同一条规则内容”：范围、原因、生效起点、解除条件。
// EndsAt 故意不参与——相同内容仅解除时间不同时必须视为重复并提示已有规则，
// 不能静默延长或覆盖原窗口。
func suppressionFingerprint(in SuppressionInput) string {
	return fingerprint(struct {
		Scope     string                      `json:"scope"`
		Reason    string                      `json:"reason"`
		StartsAt  time.Time                   `json:"starts_at"`
		Condition SuppressionReleaseCondition `json:"condition"`
		CreatedBy string                      `json:"created_by"`
	}{
		Scope:     scopeFingerprint(in.Scope),
		Reason:    in.Reason,
		StartsAt:  in.StartsAt,
		Condition: in.Condition,
		CreatedBy: in.CreatedBy,
	})
}

// isActiveLocked 判断规则在 now 时刻是否实际生效：
// 未解除、已到起点；窗口结束自动解除的规则在窗口外视为无效（到期会被自动解除）。
func (rule *SuppressionRule) isActiveLocked(now time.Time) bool {
	if rule.Status != SuppressionActive {
		return false
	}
	if now.Before(rule.StartsAt) {
		return false
	}
	if rule.Condition == ReleaseAtWindowEnd && now.After(rule.EndsAt) {
		return false
	}
	return true
}

// activeSuppressionLocked 返回此刻覆盖某事件且生效的抑制规则；没有时返回 nil。
// 当存在多条叠加规则时取 ID 最小者，保证行为确定。
func (s *Store) activeSuppressionLocked(inc *Incident, now time.Time) *SuppressionRule {
	var ids []string
	for id, rule := range s.snap.Suppressions {
		if rule.isActiveLocked(now) && rule.Scope.scopeMatches(inc) {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	sort.Strings(ids)
	return s.snap.Suppressions[ids[0]]
}

// CreateSuppression 创建一条维护窗口抑制规则。
// 幂等：同 RequestID 同内容重放返回原规则。
// 去重：已存在“范围/原因/起点/解除条件”相同的有效规则时返回 ErrAlreadyExists；
// 即使解除时间不同也明确报错，不会延长或覆盖已有窗口。
func (s *Store) CreateSuppression(req SuppressionRequest, now time.Time) (*SuppressionRule, error) {
	in := req.SuppressionInput
	in.Scope = normalizeScope(in.Scope)
	if in.Condition == "" {
		in.Condition = ReleaseAtWindowEnd
	}
	if err := validateSuppression(in); err != nil {
		return nil, err
	}

	content := struct {
		Fingerprint string `json:"fingerprint"`
	}{suppressionFingerprint(in)}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existingID, replay, err := s.checkRequestLocked("create_suppression", req.RequestID, content); err != nil {
		return nil, err
	} else if replay {
		rule, ok := s.snap.Suppressions[existingID]
		if !ok {
			return nil, fmt.Errorf("%w: suppression rule %q", ErrNotFound, existingID)
		}
		return cloneSuppression(rule), nil
	}

	for _, id := range in.Scope.IncidentIDs {
		if _, ok := s.snap.Incidents[id]; !ok {
			return nil, fmt.Errorf("%w: incident %q", ErrNotFound, id)
		}
	}

	fp := suppressionFingerprint(in)
	var dupIDs []string
	for id, rule := range s.snap.Suppressions {
		if rule.Status != SuppressionActive {
			continue
		}
		if suppressionFingerprint(SuppressionInput{
			Scope:     rule.Scope,
			Reason:    rule.Reason,
			StartsAt:  rule.StartsAt,
			Condition: rule.Condition,
			CreatedBy: rule.CreatedBy,
		}) == fp {
			dupIDs = append(dupIDs, id)
		}
	}
	if len(dupIDs) > 0 {
		sort.Strings(dupIDs)
		return nil, fmt.Errorf("%w: active suppression rule %q already covers this scope with reason %q (different end time does not extend it)",
			ErrAlreadyExists, dupIDs[0], in.Reason)
	}

	id := strings.TrimSpace(in.RuleID)
	if id == "" {
		s.snap.NextSeq++
		id = fmt.Sprintf("sup-%d", s.snap.NextSeq)
	}
	if _, exists := s.snap.Suppressions[id]; exists {
		return nil, fmt.Errorf("%w: suppression rule %q", ErrAlreadyExists, id)
	}

	rule := &SuppressionRule{
		ID:        id,
		Scope:     in.Scope,
		Reason:    in.Reason,
		StartsAt:  in.StartsAt,
		EndsAt:    in.EndsAt,
		Condition: in.Condition,
		Status:    SuppressionActive,
		CreatedBy: in.CreatedBy,
		CreatedAt: now,
	}
	s.snap.Suppressions[id] = rule
	s.rememberRequestLocked("create_suppression", req.RequestID, id, content)

	for _, inc := range s.snap.Incidents {
		if rule.Scope.scopeMatches(inc) {
			s.appendHistoryLocked(inc.ID, "suppression_created",
				fmt.Sprintf("rule=%s reason=%s until=%s", id, in.Reason, in.EndsAt.Format(time.RFC3339)), now)
		}
	}
	if err := s.commitLocked(); err != nil {
		return nil, err
	}
	return cloneSuppression(rule), nil
}

// GetSuppression 读取一条抑制规则。
func (s *Store) GetSuppression(id string) (*SuppressionRule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rule, ok := s.snap.Suppressions[id]
	if !ok {
		return nil, fmt.Errorf("%w: suppression rule %q", ErrNotFound, id)
	}
	return cloneSuppression(rule), nil
}

// ListSuppressions 列出全部抑制规则，按开始时间、ID 排序。
func (s *Store) ListSuppressions() []*SuppressionRule {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*SuppressionRule, 0, len(s.snap.Suppressions))
	for _, rule := range s.snap.Suppressions {
		out = append(out, cloneSuppression(rule))
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].StartsAt.Equal(out[j].StartsAt) {
			return out[i].StartsAt.Before(out[j].StartsAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// ReleaseSuppression 手工解除一条抑制规则，并立即按事件最新状态补发仍需升级的通知。
// 幂等：同 RequestID 同内容重放返回原规则；对已解除规则发起新请求返回 ErrConflict。
func (s *Store) ReleaseSuppression(req ReleaseSuppressionRequest, now time.Time) (*SuppressionRule, error) {
	content := struct {
		RuleID string `json:"rule_id"`
		By     string `json:"released_by"`
	}{req.RuleID, req.ReleasedBy}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existingID, replay, err := s.checkRequestLocked("release_suppression", req.RequestID, content); err != nil {
		return nil, err
	} else if replay {
		rule, ok := s.snap.Suppressions[existingID]
		if !ok {
			return nil, fmt.Errorf("%w: suppression rule %q", ErrNotFound, existingID)
		}
		return cloneSuppression(rule), nil
	}

	rule, ok := s.snap.Suppressions[req.RuleID]
	if !ok {
		return nil, fmt.Errorf("%w: suppression rule %q", ErrNotFound, req.RuleID)
	}
	if rule.Status == SuppressionReleased {
		return nil, fmt.Errorf("%w: suppression rule %q already released at %s",
			ErrConflict, rule.ID, rule.ReleasedAt.Format(time.RFC3339))
	}
	s.applyReleaseLocked(rule, req.ReleasedBy, now)
	s.rememberRequestLocked("release_suppression", req.RequestID, rule.ID, content)
	if err := s.commitLocked(); err != nil {
		return nil, err
	}
	return cloneSuppression(rule), nil
}

// applyReleaseLocked 把规则标记为已解除，并对覆盖范围内的事件执行补发判定。
// 调用方必须持有 mu。
func (s *Store) applyReleaseLocked(rule *SuppressionRule, by string, now time.Time) {
	rule.Status = SuppressionReleased
	rule.ReleasedAt = now
	rule.ReleasedBy = by

	for _, inc := range s.snap.Incidents {
		// 处理“当前仍在范围内”或“曾在本规则下被抑制过”的事件：
		// 事件可能在窗口内通过级别变化离开了范围，但它被抑制的级别仍需按最新状态处理。
		if !rule.Scope.scopeMatches(inc) && !inc.hasSuppressedFromRuleLocked(rule.ID) {
			continue
		}
		s.appendHistoryLocked(inc.ID, "suppression_released",
			fmt.Sprintf("rule=%s by=%s", rule.ID, by), now)
		// 按“最新状态”判断：已解决/已确认事件不再补发任何通知。
		s.catchUpSuppressedLocked(inc, rule.ID, now)
	}
}

// catchUpSuppressedLocked 为事件补发抑制期间错过的升级通知。
// 旧规则不能绕过当前规则：解除某条规则后，若事件仍被另一条有效规则覆盖，
// 则继续抑制；覆盖该事件的最后一条规则解除后才补发。
// 调用方必须持有 mu。
func (s *Store) catchUpSuppressedLocked(inc *Incident, releasedRuleID string, now time.Time) {
	if inc.Status != StatusFiring {
		// 已解决事件不再因为解除抑制而通知；已确认事件升级已终止。
		// 抑制记录保留供视图展示，但不补发。
		return
	}
	if rule := s.activeSuppressionLocked(inc, now); rule != nil {
		// 仍有其它有效抑制规则覆盖（按事件最新级别重新判断范围），保持抑制。
		return
	}

	emitted := false
	for i := range inc.SuppressedSteps {
		rec := &inc.SuppressedSteps[i]
		if !rec.CaughtUpAt.IsZero() {
			continue
		}
		if rec.StepIndex >= len(inc.FrozenSteps) {
			rec.CaughtUpAt = now
			continue
		}
		step := inc.FrozenSteps[rec.StepIndex]
		for _, target := range step.Targets {
			// 与正常触发共用同一意图去重：每个 (事件,级别,目标) 最多一次有效通知。
			if s.emitIntentLocked(inc, rec.StepIndex, target, now, true) {
				emitted = true
			}
		}
		rec.CaughtUpAt = now
	}

	// 补发只补通知，不重复推进步骤；若此时仍有未触发的后续级别且没有定时器，
	// 按当前时刻重新登记，保证窗口结束后升级链继续走。
	if inc.NextStepIndex < len(inc.FrozenSteps) {
		if _, exists := s.snap.Timers[inc.ID]; !exists {
			s.snap.Timers[inc.ID] = &timerRecord{
				IncidentID: inc.ID,
				StepIndex:  inc.NextStepIndex,
				FireAt:     now.Add(inc.FrozenSteps[inc.NextStepIndex].WaitBefore),
			}
		}
	}

	if emitted {
		inc.UpdatedAt = now
		s.appendHistoryLocked(inc.ID, "suppression_caught_up",
			fmt.Sprintf("rule=%s", releasedRuleID), now)
	}
}

// autoReleaseDueLocked 自动解除所有“窗口已结束”的 window_end 规则，
// 在每次到期扫描开始时执行。返回是否有规则被解除。
func (s *Store) autoReleaseDueLocked(now time.Time) bool {
	var ids []string
	for id, rule := range s.snap.Suppressions {
		if rule.Status == SuppressionActive &&
			rule.Condition == ReleaseAtWindowEnd &&
			!now.Before(rule.EndsAt) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	released := false
	for _, id := range ids {
		s.applyReleaseLocked(s.snap.Suppressions[id], "system", now)
		released = true
	}
	return released
}

// Handover 执行一次值班交接：追加移交记录，不停止升级、不改变事件状态。
// 同一事件的每次交接（移交人、接手人、时间）都会保留。
func (s *Store) Handover(req HandoverRequest, now time.Time) (*Incident, error) {
	if strings.TrimSpace(req.To) == "" {
		return nil, fmt.Errorf("%w: handover requires a receiving on-call (to)", ErrInvalidArgument)
	}
	content := struct {
		IncidentID string `json:"incident_id"`
		From       string `json:"from"`
		To         string `json:"to"`
		Note       string `json:"note"`
	}{req.IncidentID, req.From, req.To, req.Note}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existingID, replay, err := s.checkRequestLocked("handover", req.RequestID, content); err != nil {
		return nil, err
	} else if replay {
		return s.getIncidentLocked(existingID)
	}

	inc, ok := s.snap.Incidents[req.IncidentID]
	if !ok {
		return nil, fmt.Errorf("%w: incident %q", ErrNotFound, req.IncidentID)
	}
	if inc.Status == StatusResolved {
		return nil, fmt.Errorf("%w: cannot hand over resolved incident %q", ErrIncidentResolved, inc.ID)
	}
	inc.Handovers = append(inc.Handovers, HandoverRecord{
		Time: now,
		From: req.From,
		To:   req.To,
		Note: req.Note,
	})
	inc.UpdatedAt = now
	s.appendHistoryLocked(inc.ID, "handed_over",
		fmt.Sprintf("from=%s to=%s", req.From, req.To), now)
	s.rememberRequestLocked("handover", req.RequestID, inc.ID, content)
	if err := s.commitLocked(); err != nil {
		return nil, err
	}
	return s.getIncidentLocked(inc.ID)
}

// ChangeSeverity 调整事件级别。抑制期间同样允许：级别变化立即可见，
// 解除抑制时按最新级别判断规则范围与是否需要通知。
func (s *Store) ChangeSeverity(req ChangeSeverityRequest, now time.Time) (*Incident, error) {
	if strings.TrimSpace(req.Severity) == "" {
		return nil, fmt.Errorf("%w: severity is required", ErrInvalidArgument)
	}
	content := struct {
		IncidentID string `json:"incident_id"`
		Severity   string `json:"severity"`
		Reason     string `json:"reason"`
		ChangedBy  string `json:"changed_by"`
	}{req.IncidentID, req.Severity, req.Reason, req.ChangedBy}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existingID, replay, err := s.checkRequestLocked("change_severity", req.RequestID, content); err != nil {
		return nil, err
	} else if replay {
		return s.getIncidentLocked(existingID)
	}

	inc, ok := s.snap.Incidents[req.IncidentID]
	if !ok {
		return nil, fmt.Errorf("%w: incident %q", ErrNotFound, req.IncidentID)
	}
	if inc.Status == StatusResolved {
		return nil, fmt.Errorf("%w: cannot change severity of resolved incident %q", ErrIncidentResolved, inc.ID)
	}
	if inc.Severity == req.Severity {
		return nil, fmt.Errorf("%w: incident %q already has severity %q", ErrConflict, inc.ID, req.Severity)
	}
	old := inc.Severity
	inc.Severity = req.Severity
	inc.SeverityHistory = append(inc.SeverityHistory, SeverityChange{
		Time:      now,
		From:      old,
		To:        req.Severity,
		Reason:    req.Reason,
		ChangedBy: req.ChangedBy,
	})
	inc.UpdatedAt = now
	s.appendHistoryLocked(inc.ID, "severity_changed",
		fmt.Sprintf("from=%s to=%s", old, req.Severity), now)
	s.rememberRequestLocked("change_severity", req.RequestID, inc.ID, content)
	if err := s.commitLocked(); err != nil {
		return nil, err
	}
	return s.getIncidentLocked(inc.ID)
}

// IncidentViewAt 返回某事件在 now 时刻的综合视图：事件本体、每一级升级状态、
// 每次交接、级别变化以及覆盖该事件的抑制规则（含抑制原因与当前是否生效）。
func (s *Store) IncidentViewAt(id string, now time.Time) (*IncidentView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inc, ok := s.snap.Incidents[id]
	if !ok {
		return nil, fmt.Errorf("%w: incident %q", ErrNotFound, id)
	}
	return s.buildViewLocked(inc, now), nil
}

// IncidentViewsAt 与 IncidentViewAt 相同，但返回全部事件，按创建时间排序。
func (s *Store) IncidentViewsAt(now time.Time) []*IncidentView {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.snap.Incidents))
	for id := range s.snap.Incidents {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return s.snap.Incidents[ids[i]].CreatedAt.Before(s.snap.Incidents[ids[j]].CreatedAt)
	})
	out := make([]*IncidentView, 0, len(ids))
	for _, id := range ids {
		out = append(out, s.buildViewLocked(s.snap.Incidents[id], now))
	}
	return out
}

func (s *Store) buildViewLocked(inc *Incident, now time.Time) *IncidentView {
	view := &IncidentView{
		Incident:        cloneIncident(inc),
		Handovers:       append([]HandoverRecord(nil), inc.Handovers...),
		SeverityHistory: append([]SeverityChange(nil), inc.SeverityHistory...),
		Escalations:     make([]EscalationInfo, 0, len(inc.FrozenSteps)),
	}

	// stepIndex -> 最近一次抑制记录（同一级别理论上只触发一次）。
	suppressed := map[int]*SuppressedStep{}
	for i := range inc.SuppressedSteps {
		rec := inc.SuppressedSteps[i]
		suppressed[rec.StepIndex] = &inc.SuppressedSteps[i]
	}
	for idx, step := range inc.FrozenSteps {
		info := EscalationInfo{
			StepIndex: idx,
			Targets:   append([]string(nil), step.Targets...),
			State:     EscalationPending,
		}
		switch {
		case idx < inc.NextStepIndex:
			info.FiredAt = inc.FiredAt[idx]
			if rec := suppressed[idx]; rec != nil {
				info.RuleID = rec.RuleID
				info.Reason = rec.Reason
				if !rec.CaughtUpAt.IsZero() {
					info.State = EscalationCaughtUp
					info.CaughtUpAt = rec.CaughtUpAt
				} else {
					info.State = EscalationSuppressed
				}
			} else {
				info.State = EscalationNotified
			}
		}
		view.Escalations = append(view.Escalations, info)
	}

	var ruleIDs []string
	for id, rule := range s.snap.Suppressions {
		if rule.Scope.scopeMatches(inc) {
			ruleIDs = append(ruleIDs, id)
		}
	}
	sort.Strings(ruleIDs)
	for _, id := range ruleIDs {
		rule := s.snap.Suppressions[id]
		view.Suppressions = append(view.Suppressions, SuppressionView{
			RuleID:   rule.ID,
			Reason:   rule.Reason,
			Active:   rule.isActiveLocked(now),
			Status:   rule.Status,
			StartsAt: rule.StartsAt,
			EndsAt:   rule.EndsAt,
		})
	}
	return view
}

func cloneSuppression(rule *SuppressionRule) *SuppressionRule {
	cp := *rule
	cp.Scope = SuppressionScope{
		IncidentIDs: append([]string(nil), rule.Scope.IncidentIDs...),
		Severities:  append([]string(nil), rule.Scope.Severities...),
		PolicyIDs:   append([]string(nil), rule.Scope.PolicyIDs...),
	}
	return &cp
}
