// Package cli is the command-line entry of executor_docker_openhands:
// it parses --config and --token, stores a given token in the YAML
// configuration and loads the configuration.
package cli

import (
	"flag"
	"fmt"
	"io"

	"github.com/pandops/flowai-executor-docker-openhands/internal/executor"
)

// Prepare handles the command's arguments and returns the loaded
// configuration. --token stores the JWT in broker.token of the --config
// file (mode 0600) before the configuration is loaded, so later starts
// need no flag.
func Prepare(args []string) (*executor.Config, error) {
	fs := flag.NewFlagSet("executor_docker_openhands", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	cfgPath := fs.String("config", executor.DefaultConfigPath, "path of the YAML configuration")
	token := fs.String("token", "", "NATS user JWT from the Web UI; stored in broker.token")
	if err := fs.Parse(args); err != nil {
		return nil, fmt.Errorf("usage: executor_docker_openhands [--config <path>] [--token <jwt>]: %w", err)
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected arguments %q", fs.Args())
	}
	if *token != "" {
		if err := executor.StoreToken(*cfgPath, *token); err != nil {
			return nil, err
		}
	}
	return executor.LoadConfig(*cfgPath)
}
