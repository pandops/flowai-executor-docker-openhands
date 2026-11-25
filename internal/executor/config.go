package executor

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// DefaultConfigPath is where the container image expects the configuration.
const DefaultConfigPath = "/etc/flowai/config.yaml"

// StateFileName is the publisher_seq state file kept next to the YAML
// configuration file.
const StateFileName = "executor-state.json"

// BrokerConfig holds the NATS connection settings.
type BrokerConfig struct {
	// URL is the NATS server URL, for example tls://host.containers.internal:4222.
	URL string `yaml:"url"`
	// Token is the bearer user JWT executor-registry issued. The first
	// start stores it here from the --token flag.
	Token string `yaml:"token"`
	// CAFile is the PEM CA bundle that signed the NATS server certificate.
	CAFile string `yaml:"ca_file"`
}

// Config is the Executor configuration. Every key is listed in
// config.example.yaml.
type Config struct {
	Broker BrokerConfig `yaml:"broker"`

	// HTTP edges, called with the claim token.
	EnvRegistryURL   string `yaml:"env_registry_url"`
	ModelRegistryURL string `yaml:"model_registry_url"`

	// Capacity and timing.
	MaxParallelTasks       int           `yaml:"max_parallel_tasks"`
	HeartbeatInterval      time.Duration `yaml:"heartbeat_interval"`
	HeartbeatMissLimit     int           `yaml:"heartbeat_miss_limit"`
	ConnectionWait         time.Duration `yaml:"connection_wait"`
	PendingRecheckInterval time.Duration `yaml:"pending_recheck_interval"`
	RequestTimeout         time.Duration `yaml:"request_timeout"`
	PublishAckTimeout      time.Duration `yaml:"publish_ack_timeout"`
	PullWait               time.Duration `yaml:"pull_wait"`

	// Local state.
	CacheDir     string `yaml:"cache_dir"`
	CleanupIDDir string `yaml:"cleanup_id_dir"`

	ExecutorAPIBind     string `yaml:"executor_api_bind"`
	DockerSocketPath    string `yaml:"docker_socket_path"`
	DockerPublishedHost string `yaml:"docker_published_host"`
	OpenHandsPortStart  int    `yaml:"openhands_host_port_start"`
	OpenHandsPortEnd    int    `yaml:"openhands_host_port_end"`
	OpenHandsAPIKey     string `yaml:"openhands_api_key"`
	// OpenHandsStartupTO bounds the wait for a task container's agent server.
	OpenHandsStartupTO time.Duration `yaml:"openhands_startup_timeout"`
	// OpenHandsDrainTO is the SIGTERM drain deadline
	// (OPENHANDS_DRAIN_TIMEOUT_SECONDS).
	OpenHandsDrainTO time.Duration `yaml:"openhands_drain_timeout"`
	ImagePullPolicy  string        `yaml:"image_pull_policy"`
	// LiteLLMDataNetwork is the network every task container joins to reach
	// the LiteLLM data plane; the Executor process itself never joins it.
	LiteLLMDataNetwork   string        `yaml:"litellm_data_network"`
	LogLevel             string        `yaml:"log_level"`
	WebSocketDialTimeout time.Duration `yaml:"websocket_dial_timeout"`

	// Retention of a terminal task container after JetStream acknowledged
	// its terminal event. Each defaults to 0s (immediate cleanup).
	FinishedCleanupDelay time.Duration `yaml:"finished_cleanup_delay"`
	FailedCleanupDelay   time.Duration `yaml:"failed_cleanup_delay"`

	// OpenHands V1 conversation startup.
	OpenHandsWorkspace  string `yaml:"openhands_workspace"`
	OpenHandsLLMUsageID string `yaml:"openhands_llm_usage_id"`
	OpenHandsInitialRun bool   `yaml:"openhands_initial_run"`

	// path is the YAML file the configuration came from.
	path string
}

// Path returns the configuration file the Config was loaded from.
func (c *Config) Path() string { return c.path }

// StatePath returns the publisher_seq state file next to the configuration.
func (c *Config) StatePath() string {
	dir := "."
	if c.path != "" {
		dir = filepath.Dir(c.path)
	}
	return filepath.Join(dir, StateFileName)
}

// defaultConfig returns the documented defaults.
func defaultConfig() *Config {
	cfg := &Config{
		MaxParallelTasks:       2,
		HeartbeatInterval:      15 * time.Second,
		HeartbeatMissLimit:     3,
		ConnectionWait:         600 * time.Second,
		PendingRecheckInterval: 30 * time.Second,
		RequestTimeout:         5 * time.Second,
		PublishAckTimeout:      5 * time.Second,
		PullWait:               5 * time.Second,
		ExecutorAPIBind:        "127.0.0.1:8020",
		DockerPublishedHost:    "127.0.0.1",
		OpenHandsPortStart:     18000,
		OpenHandsPortEnd:       18099,
		OpenHandsStartupTO:     60 * time.Second,
		OpenHandsDrainTO:       30 * time.Second,
		ImagePullPolicy:        "if-not-present",
		LiteLLMDataNetwork:     "flowai-litellm-data",
		LogLevel:               "info",
		WebSocketDialTimeout:   5 * time.Second,
		OpenHandsWorkspace:     "/workspace/project",
		OpenHandsLLMUsageID:    "flowai-executor",
		OpenHandsInitialRun:    true,
	}
	if userCacheDir, err := os.UserCacheDir(); err == nil {
		cfg.CacheDir = filepath.Join(userCacheDir, "flowai", "executor_docker_openhands")
	}
	return cfg
}

// LoadConfig reads the YAML configuration at yamlPath (missing file means
// defaults only), applies environment overrides, and validates the result.
// It does not require a broker token: Run refuses to start without one.
func LoadConfig(yamlPath string) (*Config, error) {
	cfg := defaultConfig()
	if yamlPath != "" {
		data, err := os.ReadFile(yamlPath)
		switch {
		case err == nil:
			if err := unmarshalYAML(data, cfg); err != nil {
				return nil, fmt.Errorf("parse config %s: %w", yamlPath, err)
			}
		case errors.Is(err, os.ErrNotExist):
		default:
			return nil, fmt.Errorf("read config %s: %w", yamlPath, err)
		}
		cfg.path = yamlPath
	}
	overrideEnv(cfg)
	// When the operator has not configured an OpenHands session API key,
	// mint an opaque one. Injecting it as SESSION_API_KEY makes the agent
	// server bind on 0.0.0.0 so Docker port publishing reaches it.
	if cfg.OpenHandsAPIKey == "" {
		cfg.OpenHandsAPIKey = "flowai-" + uuid.NewString()
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// StoreToken writes token into the broker.token field of the YAML file at
// path (creating the file when it is missing), keeps every other key, and
// sets the file mode to 0600. A later --token replaces the stored JWT.
func StoreToken(path, token string) error {
	if strings.TrimSpace(token) == "" {
		return errors.New("--token is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read config %s: %w", path, err)
	}
	var doc yaml.Node
	if len(strings.TrimSpace(string(data))) > 0 {
		if err := yaml.Unmarshal(data, &doc); err != nil {
			return fmt.Errorf("parse config %s: %w", path, err)
		}
	}
	if doc.Kind == 0 {
		doc = yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{{Kind: yaml.MappingNode, Tag: "!!map"}}}
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return fmt.Errorf("config %s is not a YAML mapping", path)
	}
	brokerNode := mappingValue(root, "broker")
	if brokerNode == nil || brokerNode.Kind != yaml.MappingNode {
		brokerNode = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		setMappingValue(root, "broker", brokerNode)
	}
	setMappingValue(brokerNode, "token", &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: token})
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o600); err != nil {
		return fmt.Errorf("store token: %w", err)
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		// A bind-mounted file cannot be replaced; rewrite it in place.
		_ = os.Remove(tmp)
		if err := os.WriteFile(path, out, 0o600); err != nil {
			return fmt.Errorf("store token: %w", err)
		}
	}
	return os.Chmod(path, 0o600)
}

func mappingValue(m *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

func setMappingValue(m *yaml.Node, key string, v *yaml.Node) {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			m.Content[i+1] = v
			return
		}
	}
	m.Content = append(m.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, v)
}

func overrideEnv(cfg *Config) {
	cfg.ExecutorAPIBind = envOr("EXECUTOR_API_BIND", cfg.ExecutorAPIBind)
	cfg.DockerSocketPath = envOr("DOCKER_SOCKET_PATH", cfg.DockerSocketPath)
	cfg.DockerPublishedHost = envOr("DOCKER_PUBLISHED_HOST", cfg.DockerPublishedHost)
	cfg.OpenHandsPortStart = envInt("OPENHANDS_HOST_PORT_START", cfg.OpenHandsPortStart)
	cfg.OpenHandsPortEnd = envInt("OPENHANDS_HOST_PORT_END", cfg.OpenHandsPortEnd)
	cfg.OpenHandsAPIKey = envOr("OPENHANDS_API_KEY", cfg.OpenHandsAPIKey)
	cfg.OpenHandsStartupTO = envDur("OPENHANDS_STARTUP_TIMEOUT_SECONDS", cfg.OpenHandsStartupTO)
	cfg.OpenHandsDrainTO = envDur("OPENHANDS_DRAIN_TIMEOUT_SECONDS", cfg.OpenHandsDrainTO)
	cfg.ImagePullPolicy = envOr("IMAGE_PULL_POLICY", cfg.ImagePullPolicy)
	cfg.LiteLLMDataNetwork = envOr("EXECUTOR_LITELLM_DATA_NETWORK", cfg.LiteLLMDataNetwork)
	cfg.LogLevel = envOr("LOG_LEVEL", cfg.LogLevel)
	cfg.CacheDir = envOr("EXECUTOR_CACHE_DIR", cfg.CacheDir)
	cfg.WebSocketDialTimeout = envDur("OPENHANDS_WS_DIAL_TIMEOUT_SECONDS", cfg.WebSocketDialTimeout)
	cfg.FinishedCleanupDelay = envDur("EXECUTOR_FINISHED_CLEANUP_DELAY", cfg.FinishedCleanupDelay)
	cfg.FailedCleanupDelay = envDur("EXECUTOR_FAILED_CLEANUP_DELAY", cfg.FailedCleanupDelay)
	cfg.OpenHandsWorkspace = envOr("OPENHANDS_WORKSPACE", cfg.OpenHandsWorkspace)
	cfg.OpenHandsLLMUsageID = envOr("OPENHANDS_LLM_USAGE_ID", cfg.OpenHandsLLMUsageID)
	cfg.OpenHandsInitialRun = envBool("OPENHANDS_INITIAL_RUN", cfg.OpenHandsInitialRun)
}

func wholeSeconds(d time.Duration) bool { return d >= time.Second && d%time.Second == 0 }

func httpURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != ""
}

// Validate enforces the configuration invariants.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.DockerSocketPath) == "" {
		return errors.New("docker_socket_path (DOCKER_SOCKET_PATH) must be configured explicitly")
	}
	if c.MaxParallelTasks < 1 || c.MaxParallelTasks > 1024 {
		return errors.New("max_parallel_tasks must be between 1 and 1024")
	}
	if c.OpenHandsPortEnd <= c.OpenHandsPortStart {
		return errors.New("openhands_host_port_end must be > openhands_host_port_start")
	}
	if (c.OpenHandsPortEnd - c.OpenHandsPortStart + 1) < c.MaxParallelTasks {
		return fmt.Errorf("host port range size (%d) must be >= max_parallel_tasks (%d)",
			c.OpenHandsPortEnd-c.OpenHandsPortStart+1, c.MaxParallelTasks)
	}
	if !wholeSeconds(c.HeartbeatInterval) {
		return errors.New("heartbeat_interval must be a whole number of seconds, at least 1s")
	}
	if c.HeartbeatMissLimit < 1 || c.HeartbeatMissLimit > 1000 {
		return errors.New("heartbeat_miss_limit must be between 1 and 1000")
	}
	if !wholeSeconds(c.ConnectionWait) {
		return errors.New("connection_wait must be a whole number of seconds, at least 1s")
	}
	if c.PendingRecheckInterval <= 0 || c.RequestTimeout <= 0 || c.PublishAckTimeout <= 0 || c.PullWait <= 0 {
		return errors.New("pending_recheck_interval, request_timeout, publish_ack_timeout and pull_wait must be > 0")
	}
	if c.EnvRegistryURL != "" && !httpURL(c.EnvRegistryURL) {
		return fmt.Errorf("env_registry_url must be an http(s) URL, got %q", c.EnvRegistryURL)
	}
	if c.ModelRegistryURL != "" && !httpURL(c.ModelRegistryURL) {
		return fmt.Errorf("model_registry_url must be an http(s) URL, got %q", c.ModelRegistryURL)
	}
	if c.OpenHandsWorkspace == "" || c.OpenHandsLLMUsageID == "" {
		return errors.New("openhands_workspace and openhands_llm_usage_id are required for the V1 agent")
	}
	if c.WebSocketDialTimeout <= 0 {
		return errors.New("websocket_dial_timeout must be > 0")
	}
	if c.OpenHandsStartupTO <= 0 {
		return errors.New("openhands_startup_timeout must be > 0")
	}
	if c.OpenHandsDrainTO <= 0 {
		return errors.New("openhands_drain_timeout (OPENHANDS_DRAIN_TIMEOUT_SECONDS) must be > 0")
	}
	if c.FinishedCleanupDelay < 0 || c.FailedCleanupDelay < 0 {
		return errors.New("finished_cleanup_delay and failed_cleanup_delay must be >= 0")
	}
	return nil
}
