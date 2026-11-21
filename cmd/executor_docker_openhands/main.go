// executor_docker_openhands is the FlowAI concrete Executor for the Docker
// runtime + OpenHands agent toolchain. Concrete wire names follow
// executor_<runtime>_<tool>; "openhands" is the deliberate service
// identifier (the external product spelling "OpenHands" stays unchanged).
//
// First start: executor_docker_openhands --token <jwt> stores the JWT in
// broker.token of the configuration file (mode 0600); later starts read it.
package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pandops/flowai-executor-docker-openhands/internal/broker"
	"github.com/pandops/flowai-executor-docker-openhands/internal/cli"
	"github.com/pandops/flowai-executor-docker-openhands/internal/dockerclient"
	"github.com/pandops/flowai-executor-docker-openhands/internal/executor"
	"github.com/pandops/flowai-executor-docker-openhands/internal/logging"
	"github.com/pandops/flowai-executor-docker-openhands/internal/netaddr"
	"github.com/pandops/flowai-executor-docker-openhands/internal/platform"
)

func main() {
	cfg, err := cli.Prepare(os.Args[1:])
	if err != nil {
		os.Stderr.WriteString("executor_docker_openhands: " + err.Error() + "\n")
		os.Exit(2)
	}

	logger := logging.New(logging.Level(cfg.LogLevel))
	mainLog := logger.With("service", platform.ExecutorTypeDockerOpenHands)
	docker, err := dockerclient.NewSDKClient(cfg.DockerSocketPath, cfg.DockerPublishedHost)
	if err != nil {
		mainLog.Error("docker client init failed", "err", err.Error())
		os.Exit(1)
	}
	exec := executor.New(cfg, executor.Deps{
		Dialer:     broker.NATSDialer(),
		Docker:     docker,
		Network:    netaddr.System(),
		Logger:     logger,
		HTTPClient: &http.Client{Timeout: cfg.RequestTimeout},
	})

	listener, err := net.Listen("tcp", cfg.ExecutorAPIBind)
	if err != nil {
		mainLog.Error("health API listen failed", "err", err.Error())
		os.Exit(1)
	}
	httpServer := &http.Server{Handler: exec.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			mainLog.Error("health API exited", "err", err.Error())
		}
	}()
	mainLog.Info("executor_docker_openhands starting",
		"config", cfg.Path(), "broker_url", cfg.Broker.URL, "health_bind", listener.Addr().String())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runErr := exec.Run(ctx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		mainLog.Error("executor stopped with error", "err", runErr.Error())
		os.Exit(1)
	}
	mainLog.Info("executor_docker_openhands stopped")
}
