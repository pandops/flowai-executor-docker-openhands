# Docker OpenHands Executor

`executor_docker_openhands` runs one team's tasks on one host. It takes
tasks for its team and execution tag from the platform's NATS JetStream
broker. Each task gets its own OpenHands container, and every lifecycle
event goes back to the broker. Its only HTTP surface is two health probes.

## Start an Executor

1. In the Web UI open **Settings → Executors** and choose **Create
   Executor**. Enter the execution tag (for example `gpu`) and the type
   `executor_docker_openhands`.
2. The Web UI shows the Executor's NATS token once, with the command
   `executor_docker_openhands --token <token>`. Copy the token now: it cannot
   be shown again. **Reissue token** on the Executor's page makes a new one
   and stops the old one.
3. Write the configuration file. Copy [config.example.yaml](config.example.yaml)
   to a directory of its own, for example `/srv/flowai-executor/config`, and
   set at least `broker.url`, `broker.ca_file`, `env_registry_url`,
   `model_registry_url`, `docker_socket_path` and `docker_published_host`.
   Put the broker CA certificate in the same directory.
4. Set `FLOWAI_RUNTIME_SOCKET` to the host socket path and
   `FLOWAI_RUNTIME_SOCKET_GID` to its numeric group as seen in the
   container. Start the container once with the token:

   ```sh
   podman run -d --name flowai-executor \
     -v /srv/flowai-executor/config:/etc/flowai \
     -v flowai-executor-cache:/var/lib/flowai/executor \
     --group-add "$FLOWAI_RUNTIME_SOCKET_GID" \
     -v "$FLOWAI_RUNTIME_SOCKET:/run/flowai/runtime.sock" \
     -p 127.0.0.1:8020:8080 \
     <registry>/executor_docker_openhands@sha256:<digest> \
     --token <token>
   ```

   The token is stored as `broker.token` in `/etc/flowai/config.yaml`. The
   file keeps every other key and becomes mode `0600`. The configuration
   directory must be writable, because it also holds the `publisher_seq`
   state file `executor-state.json`.

5. Restart without `--token`; the stored token is used. Passing `--token`
   again replaces the stored token, for example after **Reissue token**.

The process runs as user `65532`, so that user must be able to write the
configuration directory and the cache volume. Without any token the
Executor exits at once with "no broker token".

## Configuration reference

Every key with its default. Durations use Go syntax (`15s`, `10m`).
Defaults apply when a key is missing. [config.example.yaml](config.example.yaml)
shows the values for a container.

| Key                         | Default                                          | Meaning                                                                     |
| --------------------------- | ------------------------------------------------ | --------------------------------------------------------------------------- |
| `broker.url`                | none                                             | NATS server URL, for example `tls://nats.example:4222`                      |
| `broker.ca_file`            | none                                             | PEM CA bundle of the NATS server certificate                                |
| `broker.token`              | none                                             | Executor JWT; stored by `--token`                                           |
| `env_registry_url`          | none                                             | env-registry base URL for the launch-parameter edge                         |
| `model_registry_url`        | none                                             | model-registry base URL for the LiteLLM key edge                            |
| `max_parallel_tasks`        | `2`                                              | Tasks held at once (1–1024); pending claims hold a slot too                 |
| `heartbeat_interval`        | `15s`                                            | Time between heartbeats; whole seconds                                      |
| `heartbeat_miss_limit`      | `3`                                              | Failed heartbeats in a row that start connection loss                       |
| `connection_wait`           | `600s`                                           | W: how long paused agents are kept without a heartbeat reply; whole seconds |
| `pending_recheck_interval`  | `30s`                                            | Time between checks of a pending claim                                      |
| `request_timeout`           | `5s`                                             | Broker request and HTTP edge timeout                                        |
| `publish_ack_timeout`       | `5s`                                             | Wait for a JetStream publish acknowledgement                                |
| `pull_wait`                 | `5s`                                             | Longest wait of one consumer pull                                           |
| `cache_dir`                 | `<user cache>/flowai/executor_docker_openhands`  | Recovery cache (`executor.db`, `executor.lock`), created owner-only         |
| `cleanup_id_dir`            | `FLOWAI_CLEANUP_ID_DIR` or `<user cache>/flowai` | Holds `cleanup-id`, the stable label that finds leftover containers         |
| `executor_api_bind`         | `127.0.0.1:8020`                                 | Address of the health probes                                                |
| `docker_socket_path`        | none (required)                                  | Container runtime socket                                                    |
| `docker_published_host`     | `127.0.0.1`                                      | Host where task container ports are reached                                 |
| `openhands_host_port_start` | `18000`                                          | First host port for task containers                                         |
| `openhands_host_port_end`   | `18099`                                          | Last host port; the range must hold `max_parallel_tasks`                    |
| `openhands_api_key`         | minted per start                                 | Session key of the agent servers (`SESSION_API_KEY`)                        |
| `openhands_startup_timeout` | `60s`                                            | Wait for a task's agent server to become healthy                            |
| `openhands_drain_timeout`   | `30s`                                            | SIGTERM drain deadline for running tasks                                    |
| `image_pull_policy`         | `if-not-present`                                 | Pull policy for task images                                                 |
| `litellm_data_network`      | `flowai-litellm-data`                            | Network every task container joins to reach LiteLLM                         |
| `websocket_dial_timeout`    | `5s`                                             | Dial timeout of the conversation event stream                               |
| `finished_cleanup_delay`    | `0s`                                             | Keep a finished task's container this long; the slot stays used             |
| `failed_cleanup_delay`      | `0s`                                             | Keep a failed task's container this long; the slot stays used               |
| `openhands_workspace`       | `/workspace/project`                             | Agent working directory in the task container                               |
| `openhands_llm_usage_id`    | `flowai-executor`                                | LLM usage id sent to OpenHands                                              |
| `openhands_initial_run`     | `true`                                           | Start the agent on the first message                                        |
| `log_level`                 | `info`                                           | `debug`, `info`, `warn` or `error`                                          |

These environment variables override the matching keys:
`EXECUTOR_API_BIND`, `DOCKER_SOCKET_PATH`, `DOCKER_PUBLISHED_HOST`,
`OPENHANDS_HOST_PORT_START`, `OPENHANDS_HOST_PORT_END`, `OPENHANDS_API_KEY`,
`OPENHANDS_STARTUP_TIMEOUT_SECONDS`, `OPENHANDS_DRAIN_TIMEOUT_SECONDS`,
`IMAGE_PULL_POLICY`, `EXECUTOR_LITELLM_DATA_NETWORK`, `LOG_LEVEL`,
`EXECUTOR_CACHE_DIR`, `OPENHANDS_WS_DIAL_TIMEOUT_SECONDS`,
`EXECUTOR_FINISHED_CLEANUP_DELAY`, `EXECUTOR_FAILED_CLEANUP_DELAY`,
`OPENHANDS_WORKSPACE`, `OPENHANDS_LLM_USAGE_ID` and `OPENHANDS_INITIAL_RUN`.

## What the Executor does

The token's identity is fixed when executor-registry issues it: team,
`executor_id`, tag and type. The Executor uses only these subjects and
consumers:

- **Connect.** It connects with the JWT and the inbox prefix
  `_INBOX.executor-<executor_id>`, and retries about every second. Logs never
  carry the token.
- **Heartbeat.** Every `heartbeat_interval` it sends a request on
  `executor-registry.executor.<id>.heartbeat.pub`. The request carries its
  type, timing, `max_parallel_tasks`, `slots_in_use`, the address of the
  default-route interface (`ip`, or null with a `heartbeat_ip_unavailable`
  warning) and its claims in state `created`, `pending` or `running`. When a
  reply says task-manager no longer holds a claim, the Executor drops that
  claim and removes its container. It does this only for claims whose claim
  token arrived before that heartbeat was sent.
- **Command stream.** It creates its own durable consumer
  `commands-<id>` on `TASK_MANAGER_COMMAND` before taking any task, again
  after every reconnect, and every heartbeat interval until creation
  succeeds. A cancel publishes `control_acknowledged`, pauses the agent,
  removes the container and publishes `cancelled`. If the pause is refused
  or OpenHands cannot be reached, it publishes `control_failed` and the task
  keeps running.
- **Task consumer.** It pulls from `tasks-<team_id>-<tag hash>` on
  `TASK_MANAGER_TASK`, where the tag hash is the first 16 hex characters of
  SHA-256 of the tag. It asks for at most the free slots, in delivery order.
  A message for another team or tag, or one with a malformed identifier, is
  terminated, and `protocol_violation` is published.
- **Claim.** It stores the claim, publishes `created` and acknowledges the
  message. Then it asks `task-manager.executor.<id>.claim-token.pub` for the
  claim token. "Not processed yet" or no reply is repeated every second for
  up to 10 s; after that, or after "claim rejected", the claim is dropped
  without an event.
- **Checks.** `model-registry.executor.<id>.check.pub` decides whether the
  task may start. A temporary reason keeps the claim `pending`. It is checked
  again every `pending_recheck_interval` and released with `pending_timeout`
  at the task's pending limit.
- **Launch-parameter open edge.** One
  `GET <env_registry_url>/v1/tasks/<task_id>/launch-parameters/open` call
  with `Authorization: Bearer <claim token>`. Its values become the task
  container's environment.
- **LiteLLM key edge.** One
  `GET <model_registry_url>/v1/tasks/<task_id>/litellm-key` call with the
  claim token. It returns the task's own LiteLLM key, alias and data-plane
  URL for the agent. A missing value fails the task with
  `llm_configuration_incomplete`. The Executor never uses its own
  environment for these values.
- **Runtime.** It pulls the task's resolved image and uses it as given. It
  starts the container on a free host port, joined to
  `litellm_data_network`, starts the conversation and publishes `running`.
  Agent and tool output becomes `log` events. The terminal status becomes
  `finished` or `failed`; a container that exits first fails with
  `container_exited_before_finish`. After the terminal event is
  acknowledged, the container is kept for its cleanup delay and then
  removed.

Every event goes to `executor.event.<id>.<kind>.pub`, with
`Nats-Msg-Id: executor-<team_id>-<executor_id>-<publisher_seq>`. The
`publisher_seq` is written to `executor-state.json` with fsync before the
publish. Unacknowledged events are retried with the same body and id.
Event, heartbeat and request bodies follow the JSON Schemas in
[schemas/](schemas/). The HTTP contracts are in [openapi/](openapi/).

### Task container labels

| Label                          | Value                                      |
| ------------------------------ | ------------------------------------------ |
| `flowai.team_id`               | Team of the task                           |
| `flowai.task_id`               | Task                                       |
| `flowai.executor_id`           | This Executor                              |
| `flowai.attempt`               | Attempt of the claim                       |
| `flowai.claim_seq`             | Claim sequence                             |
| `flowai.runtime`               | `openhands`                                |
| `flowai.cleanup_id`            | Stable host label used to remove leftovers |
| `flowai.resolved_image_source` | `flow_default` or `team_default`           |

## Connection loss, restart and shutdown

- **Connection loss.** After `heartbeat_miss_limit` failed heartbeats, or
  at once when the broker revokes the token, the Executor pauses every
  agent and stops pulling, but keeps sending heartbeats. `/v1/readyz`
  answers 503 with `connection_loss: true`. When a reply that lists the
  claims arrives, it publishes `executor_connection_lost` and
  `executor_connection_restored`. The listed agents continue; unlisted ones
  are removed without an event. If no reply comes within `connection_wait`,
  all task containers are removed without events. A revoked token is never
  used again: restart with the reissued token.
- **Restart after a crash.** Only one process may use a cache volume. On
  start, the Executor removes every container with its `cleanup_id`. It
  republishes unacknowledged events with their original numbers. For each
  claim it held, it publishes `released` with reason `executor_restarted`,
  if the last heartbeat reply is younger than `connection_wait`. Older
  claims are dropped without events. It never claims a task again after a
  restart.
- **SIGTERM.** The Executor publishes `executor_stopping` and stops
  heartbeats and pulls. Claims that are created or pending get `released`
  with `executor_shutdown`. Running tasks get up to
  `openhands_drain_timeout` to finish; the rest get `released`
  `executor_shutdown`. All containers are removed, then `executor_stopped`
  is published. During connection loss it publishes nothing and only
  removes the containers.
- **Container runtime failure.** If the Docker events stream breaks, the
  Executor publishes `executor_failed`, stops taking tasks, and
  `/v1/readyz` answers 503 with `can_supervise: false`. `/v1/livez` stays 200.

## Health probes

`GET /v1/livez` answers 200 while the process runs. `GET /v1/readyz`
answers 200 only when the Executor is connected with a valid token, its
command consumer exists, it is not in connection loss and it can supervise
containers. Both carry `service: executor_docker_openhands` and the
`executor_id`. Every other path answers 404 or 405. Bind the probes to
loopback only.

## Standalone qualification

Use Go 1.26.2. Stage the publisher schemas listed in
[the fixture declarations](qualification/schema-declarations.json) under
`FLOWAI_CONTRACT_FIXTURE_DIR`, preserving each `svc/<service>/schemas/`
path. The four `executor/docker_openhands/schemas/` entries refer to
this checkout's `schemas/` directory. Use exact commit SHAs for the
publisher inputs.

Run `go test -mod=readonly -count=1 ./...` for unit tests. For the
real runtime cleanup test, set `DOCKER_HOST` to the configured Docker-compatible
Unix socket and `FLOWAI_OPENHANDS_TEST_IMAGE` to a verified, locally present
`repository@sha256:digest`. Run
`go test -mod=readonly -count=1 -tags=integration -v ./...` to include the
cleanup test. Set `FLOWAI_RESOURCE_LEDGER` to a writable file to record
the container and anonymous volume IDs and independently verify their
absence after the run.

For the service image, use Linux amd64 and grant UID 65532 access to
the configured socket through its numeric supplemental GID. The socket
must have group access. Mount it at `/run/flowai/runtime.sock` and
provide the writable configuration and cache mounts shown above.
