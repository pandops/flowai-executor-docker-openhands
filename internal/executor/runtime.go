package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/pandops/flowai-executor-docker-openhands/internal/cache"
	"github.com/pandops/flowai-executor-docker-openhands/internal/dockerclient"
	"github.com/pandops/flowai-executor-docker-openhands/internal/openhands"
)

// openHandsPort is the agent server's port inside the task container.
const openHandsPort = 8000

// maxLogText is the longest text one log event carries.
const maxLogText = 65536

func tagHash(tag string) string {
	sum := sha256.Sum256([]byte(tag))
	return hex.EncodeToString(sum[:])[:16]
}

// launch is what the edges returned for one claim.
type launch struct {
	env map[string]string
	llm llmConfig
}

type llmConfig struct {
	BaseURL string `json:"OPENAI_BASE_URL"`
	Model   string `json:"OPENAI_MODEL"`
	APIKey  string `json:"OPENAI_API_KEY"`
}

// checkOutcome is the result of one check round.
type checkOutcome int

const (
	outcomeReady checkOutcome = iota
	outcomePending
	outcomeDrop
	outcomeFail
)

// prepare runs the model check and opens both edges, keeping the claim
// pending while a temporary reason holds.
func (e *Executor) prepare(s *taskSlot, rec *cache.ClaimRecord, token string) (*launch, bool) {
	l := &launch{}
	var haveEnv, haveLLM bool
	pendingReason := ""
	var pendingSince time.Time
	for {
		outcome, reason := e.checkModel(s)
		if outcome == outcomeReady && !haveEnv {
			outcome, reason = e.openEnv(s, token, l)
			haveEnv = outcome == outcomeReady
		}
		if outcome == outcomeReady && !haveLLM {
			outcome, reason = e.openLLM(s, token, l)
			haveLLM = outcome == outcomeReady
		}
		if s.ctx.Err() != nil {
			return nil, false
		}
		switch outcome {
		case outcomeReady:
			return l, true
		case outcomeDrop:
			if s.claimTerminal() {
				e.log.Info("claim is no longer current; dropped", "task_id", s.key.TaskID)
				e.dropClaim(s)
			}
			return nil, false
		case outcomeFail:
			e.fail(s, reason, false, "")
			return nil, false
		}
		if pendingSince.IsZero() {
			pendingSince = time.Now()
			s.setState("pending")
			rec.State = cache.ClaimPending
			rec.PendingSince = pendingSince.UTC()
			_ = e.store.PutClaim(e.bg, *rec)
		}
		if reason != pendingReason {
			pendingReason = reason
			key := s.key
			if err := e.emit(s.ctx, "pending", &key, fields{"reason": reason}); err != nil {
				return nil, false
			}
		}
		limit := s.task.PendingLimit
		if limit <= 0 {
			limit = 600 * time.Second
		}
		left := time.Until(pendingSince.Add(limit))
		if left <= 0 {
			e.release(s, "pending_timeout")
			return nil, false
		}
		wait := e.cfg.PendingRecheckInterval
		if left < wait {
			wait = left
		}
		select {
		case <-s.ctx.Done():
			return nil, false
		case <-time.After(wait):
		}
	}
}

// checkModel asks model-registry whether the task may start now.
func (e *Executor) checkModel(s *taskSlot) (checkOutcome, string) {
	body, _ := json.Marshal(map[string]any{
		"params": map[string]any{},
		"body":   map[string]any{"task_id": s.key.TaskID, "attempt": s.key.Attempt, "claim_seq": s.key.ClaimSeq},
	})
	reply, err := e.request(s.ctx, fmt.Sprintf("model-registry.executor.%d.check.pub", e.ident.ExecutorID), body)
	if err != nil {
		e.log.Warn("model check unavailable", "task_id", s.key.TaskID, "err", err)
		return outcomePending, "dependency_unavailable"
	}
	var r struct {
		Result string `json:"result"`
		Reason string `json:"reason"`
		Code   string `json:"code"`
	}
	_ = json.Unmarshal(reply.Data, &r)
	switch {
	case reply.Status == http.StatusOK && r.Result == "ok":
		return outcomeReady, ""
	case reply.Status == http.StatusOK && r.Result == "temporary" && pendingReasons[r.Reason]:
		return outcomePending, r.Reason
	case reply.Status == http.StatusNotFound && r.Code == "claim_not_current":
		return outcomeDrop, ""
	}
	return outcomePending, "dependency_unavailable"
}

var pendingReasons = map[string]bool{
	"no_active_model": true, "all_selected_models_paused": true, "budget_exhausted": true,
	"spend_observation_unavailable": true, "dependency_unavailable": true,
}

// edgeGet calls one HTTP edge with the claim token.
func (e *Executor) edgeGet(s *taskSlot, base, suffix, token string) (int, []byte, error) {
	if base == "" {
		return 0, nil, errors.New("edge URL is not configured")
	}
	ctx, cancel := context.WithTimeout(s.ctx, e.cfg.RequestTimeout)
	defer cancel()
	url := strings.TrimRight(base, "/") + "/v1/tasks/" + strconv.FormatInt(s.key.TaskID, 10) + suffix
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := e.deps.HTTPClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, data, err
}

// edgeOutcome maps a non-200 edge answer.
func edgeOutcome(status int, data []byte) (checkOutcome, string) {
	var r struct {
		Code          string `json:"code"`
		FailureReason string `json:"failure_reason"`
	}
	_ = json.Unmarshal(data, &r)
	switch status {
	case http.StatusNotFound:
		return outcomeDrop, ""
	case http.StatusUnprocessableEntity:
		reason := r.FailureReason
		if reason == "" {
			reason = "configuration_error"
		}
		return outcomeFail, reason
	}
	return outcomePending, "dependency_unavailable"
}

func (e *Executor) openEnv(s *taskSlot, token string, l *launch) (checkOutcome, string) {
	status, data, err := e.edgeGet(s, e.cfg.EnvRegistryURL, "/launch-parameters/open", token)
	if err != nil {
		e.log.Warn("launch-parameter edge unavailable", "task_id", s.key.TaskID, "err", err)
		return outcomePending, "dependency_unavailable"
	}
	if status != http.StatusOK {
		return edgeOutcome(status, data)
	}
	var r struct {
		Values map[string]string `json:"values"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return outcomePending, "dependency_unavailable"
	}
	l.env = r.Values
	return outcomeReady, ""
}

func (e *Executor) openLLM(s *taskSlot, token string, l *launch) (checkOutcome, string) {
	status, data, err := e.edgeGet(s, e.cfg.ModelRegistryURL, "/litellm-key", token)
	if err != nil {
		e.log.Warn("LiteLLM key edge unavailable", "task_id", s.key.TaskID, "err", err)
		return outcomePending, "dependency_unavailable"
	}
	if status != http.StatusOK {
		return edgeOutcome(status, data)
	}
	var c llmConfig
	if err := json.Unmarshal(data, &c); err != nil {
		return outcomePending, "dependency_unavailable"
	}
	if c.BaseURL == "" || c.Model == "" || c.APIKey == "" {
		return outcomeFail, "llm_configuration_incomplete"
	}
	l.llm = c
	return outcomeReady, ""
}

// runRuntime starts the task container and follows its conversation.
func (e *Executor) runRuntime(s *taskSlot, rec *cache.ClaimRecord, l *launch) {
	ctx := s.ctx
	m := s.task
	port, ok := e.allocatePort()
	if !ok {
		e.fail(s, "host_port_unavailable", true, "")
		return
	}
	s.mu.Lock()
	s.hostPort = port
	s.mu.Unlock()
	if err := e.deps.Docker.PullImage(ctx, *m.Image, e.cfg.ImagePullPolicy); err != nil {
		if ctx.Err() == nil {
			e.log.Warn("image pull failed", "task_id", s.key.TaskID, "image", *m.Image, "err", err)
			e.fail(s, "image_pull_failed", true, "")
		}
		return
	}
	name := fmt.Sprintf("flowai-executor-%d-task-%d-%d-%d", e.ident.ExecutorID, s.key.TaskID, s.key.Attempt, s.key.ClaimSeq)
	labels := map[string]string{
		dockerclient.LabelExecutorID: strconv.FormatInt(e.ident.ExecutorID, 10),
		dockerclient.LabelCleanupID:  e.cleanupID,
		dockerclient.LabelRuntime:    dockerclient.RuntimeOpenHands,
		dockerclient.LabelTaskID:     strconv.FormatInt(s.key.TaskID, 10),
		dockerclient.LabelTeamID:     strconv.FormatInt(e.ident.TeamID, 10),
		dockerclient.LabelAttempt:    strconv.FormatInt(s.key.Attempt, 10),
		dockerclient.LabelClaimSeq:   strconv.FormatInt(s.key.ClaimSeq, 10),
	}
	if m.ImageSource != "" {
		labels[dockerclient.LabelResolvedImageSource] = m.ImageSource
	}
	spec := dockerclient.ContainerSpec{
		Name:     name,
		Image:    *m.Image,
		Env:      dockerEnvWithSessionAPIKey(l.env, e.cfg.OpenHandsAPIKey),
		Labels:   labels,
		Ports:    []dockerclient.PortMapping{{HostPort: port, ContainerPort: openHandsPort, Protocol: "tcp"}},
		Networks: []string{e.cfg.LiteLLMDataNetwork},
	}
	ref, err := e.deps.Docker.StartContainer(ctx, spec)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		e.log.Warn("task container start failed", "task_id", s.key.TaskID, "err", err)
		if errors.Is(err, dockerclient.ErrNetworkUnavailable) {
			e.fail(s, "llm_route_unavailable", true, "")
		} else {
			e.fail(s, "container_start_failed", true, "")
		}
		return
	}
	s.mu.Lock()
	s.containerID, s.containerName = ref.ID, ref.Name
	s.mu.Unlock()
	rec.ContainerID, rec.ContainerName = ref.ID, ref.Name
	_ = e.store.PutClaim(e.bg, *rec)
	if ctx.Err() != nil {
		// The claim ended while the container started.
		e.removeContainer(context.WithoutCancel(ctx), ref.ID)
		return
	}

	baseURL := e.deps.Docker.ContainerURL(port)
	oh := openhands.NewClient(baseURL, e.cfg.OpenHandsAPIKey, e.deps.HTTPClient)
	if err := oh.HealthCheck(ctx, e.cfg.OpenHandsStartupTO); err != nil {
		if ctx.Err() == nil {
			e.log.Warn("agent server did not become healthy", "task_id", s.key.TaskID, "err", err)
			e.fail(s, "openhands_unavailable", true, "")
		}
		return
	}
	conv, err := oh.StartConversation(ctx, e.conversationConfig(*m.Prompt, l.llm.APIKey, l.llm.BaseURL, l.llm.Model))
	if err != nil {
		if ctx.Err() == nil {
			e.log.Warn("conversation start failed", "task_id", s.key.TaskID, "err", redact(err.Error(), l.llm.APIKey))
			e.fail(s, "openhands_start_failed", true, "")
		}
		return
	}
	s.mu.Lock()
	s.oh, s.convID = oh, conv.ID
	s.mu.Unlock()
	rec.ConversationID = conv.ID
	_ = e.store.PutClaim(e.bg, *rec)

	ws, err := e.dialStream(ctx, baseURL, conv.ID)
	if err != nil {
		if ctx.Err() == nil {
			e.log.Warn("conversation stream unavailable", "task_id", s.key.TaskID, "err", err)
			e.fail(s, "openhands_unavailable", true, "")
		}
		return
	}
	key := s.key
	f := fields{}
	if m.ImageSource == "flow_default" || m.ImageSource == "team_default" {
		f["image_source"] = m.ImageSource
	}
	if err := e.emit(ctx, "running", &key, f); err != nil {
		_ = ws.Close()
		return
	}
	s.setState("running")
	rec.State = cache.ClaimRunning
	_ = e.store.PutClaim(e.bg, *rec)
	e.follow(s, baseURL, conv.ID, ws)
}

func (e *Executor) dialStream(ctx context.Context, baseURL, convID string) (*websocket.Conn, error) {
	dialer := websocket.Dialer{HandshakeTimeout: e.cfg.WebSocketDialTimeout}
	dctx, cancel := context.WithTimeout(ctx, e.cfg.WebSocketDialTimeout)
	defer cancel()
	conn, resp, err := dialer.DialContext(dctx, OpenHandsWSURL(baseURL, convID), httpAPIHeaders(e.cfg.OpenHandsAPIKey))
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	return conn, err
}

// follow reads the conversation stream until the claim ends.
func (e *Executor) follow(s *taskSlot, baseURL, convID string, ws *websocket.Conn) {
	ctx := s.ctx
	for {
		frames := make(chan []byte, 64)
		readErr := make(chan error, 1)
		go func(ws *websocket.Conn) {
			defer close(frames)
			for {
				_, data, err := ws.ReadMessage()
				if err != nil {
					readErr <- err
					return
				}
				frames <- data
			}
		}(ws)
	streamLoop:
		for {
			select {
			case <-ctx.Done():
				_ = ws.Close()
				return
			case <-s.exitCh:
				_ = ws.Close()
				e.fail(s, "container_exited_before_finish", true, "")
				return
			case data, ok := <-frames:
				if !ok {
					break streamLoop
				}
				if done := e.handleFrame(s, data); done {
					_ = ws.Close()
					return
				}
			}
		}
		_ = ws.Close()
		// The stream broke without a terminal status: reconnect while the
		// container lives.
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.exitCh:
				e.fail(s, "container_exited_before_finish", true, "")
				return
			case <-time.After(time.Second):
			}
			next, err := e.dialStream(ctx, baseURL, convID)
			if err == nil {
				ws = next
				break
			}
		}
	}
}

// handleFrame publishes a frame's logs and handles a terminal status. It
// reports whether the claim ended.
func (e *Executor) handleFrame(s *taskSlot, data []byte) bool {
	var frame any
	if err := json.Unmarshal(data, &frame); err != nil {
		return false
	}
	key := s.key
	if s.toolLogs == nil {
		s.toolLogs = toolCallLogs{}
	}
	for _, l := range s.toolLogs.unpublished(frame, publishedLogsFromOpenHands(frame)) {
		text := l.content
		if len(text) > maxLogText {
			text = text[:maxLogText]
		}
		if text == "" {
			continue
		}
		_ = e.emit(s.ctx, "log", &key, fields{"stream": l.stream, "text": text})
	}
	if isBudgetExceededError(frame) {
		s.setFailureReason(failureReasonBudgetExceeded)
	}
	if code := conversationErrorCode(frame); code != "" {
		s.setErrorCode(code)
	}
	status := frameStatus(frame)
	switch status {
	case "finished":
		e.finish(s)
		return true
	case "paused":
		if s.selfPaused.Load() {
			return false
		}
		fallthrough
	case "failed", "error", "stuck":
		reason, code := "openhands_"+status, s.getErrorCode()
		if s.getFailureReason() == failureReasonBudgetExceeded {
			reason = failureReasonBudgetExceeded
		}
		e.fail(s, reason, true, code)
		return true
	}
	return false
}

// frameStatus returns the conversation execution status a frame reports.
func frameStatus(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	if s, ok := m["execution_status"].(string); ok {
		return strings.ToLower(strings.TrimSpace(s))
	}
	if k, _ := m["kind"].(string); k == "ConversationStateUpdateEvent" {
		if key, _ := m["key"].(string); key == "execution_status" {
			if s, ok := m["value"].(string); ok {
				return strings.ToLower(strings.TrimSpace(s))
			}
		}
	}
	return ""
}

// finish ends a claim with finished.
func (e *Executor) finish(s *taskSlot) {
	if !s.claimTerminal() {
		return
	}
	s.setState("terminal")
	key := s.key
	if err := e.emit(s.ctx, "finished", &key, nil); err != nil {
		e.dropClaim(s)
		return
	}
	s.recordAcceptedTerminal("finished")
	e.cleanupContainer(s)
	e.freeSlot(s)
}

// pauseAgent pauses a running agent; the Executor then ignores the
// paused status it causes.
func (e *Executor) pauseAgent(ctx context.Context, s *taskSlot) error {
	s.mu.Lock()
	oh, conv := s.oh, s.convID
	s.mu.Unlock()
	if oh == nil {
		return nil
	}
	s.selfPaused.Store(true)
	pctx, cancel := context.WithTimeout(ctx, e.cfg.RequestTimeout+5*time.Second)
	defer cancel()
	return oh.PauseConversation(pctx, conv)
}

// continueAgent resumes an agent the Executor paused.
func (e *Executor) continueAgent(ctx context.Context, s *taskSlot) {
	s.mu.Lock()
	oh, conv := s.oh, s.convID
	s.mu.Unlock()
	if oh == nil || !s.selfPaused.Load() {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, e.cfg.RequestTimeout+5*time.Second)
	defer cancel()
	if err := oh.RunConversation(cctx, conv); err != nil {
		e.log.Warn("continue after connection loss failed", "task_id", s.key.TaskID, "err", err)
	}
	s.selfPaused.Store(false)
}

// redact removes a secret from a text.
func redact(text, secret string) string {
	if secret == "" {
		return text
	}
	return strings.ReplaceAll(text, secret, "[redacted]")
}
