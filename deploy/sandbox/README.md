# Docker Sandbox Deployment

This directory contains the supported Docker sandbox image and egress boundary.
Docker is the only sandbox driver in the MVP.

## Host Prerequisites

The trusted control plane requires Docker CLI/socket access. The default
`small` and `medium` profiles enforce a writable-layer disk quota. Docker
`overlay2` supports that quota only when `/var/lib/docker` is backed by XFS
mounted with `pquota`; `btrfs` and `zfs` have their own quota support. A host
without a quota-capable storage driver fails sandbox creation instead of running
with an unbounded writable layer. See Docker's
[`--storage-opt` documentation](https://docs.docker.com/reference/cli/docker/container/run/#set-storage-driver-options-per-container---storage-opt)
for supported drivers and the `overlay2` XFS requirement.

The Docker host must expose the iptables `DOCKER-USER` and `INPUT` chains to a
short-lived trusted `NET_ADMIN` helper container. The platform fails sandbox
acquisition when those packet-filter rules cannot be installed. Rootless Docker
and Docker's experimental nftables firewall backend are not supported by this
MVP boundary. Tenant networks must have IPv6 disabled; dual-stack host firewall
policy is outside the MVP boundary and therefore fails closed.

The MVP supports one trusted control-plane process per Docker host. Do not run
overlapping platform replicas against the same managed sandbox labels and
networks. Idle tenant containers are stopped, not deleted, so their Codex state
remains resumable and their bounded writable layers still consume host storage.
Applications expanding beyond a small bounded tenant set need an explicit
retention/deletion policy before rollout.

## Automatic Infrastructure

`create_sandboxed_codex_backend(...)` automatically creates one internal network
per tenant, a public proxy network, one shared egress container, and host packet
filter rules when sandbox mode is first used. Tenant traffic is allowed only to
that tenant network's Squid address on port 3128. Direct traffic to peer
containers and the Docker bridge gateway is dropped. An
application consuming the PyPI package does not need this repository or a
separate setup command.
The resolved subnet and proxy address are retained by the runtime. Failed tenant
container acquisition rolls back that network attachment and its exact firewall
rules; destroy can perform the same cleanup even if the proxy container is
temporarily absent.

## Explicit Operations

Operators can use the compose file to pre-create, inspect, or independently
restart the same shared infrastructure:

```bash
docker compose -f deploy/sandbox/compose.yaml up -d
```

The compose project pre-creates the shared resources used by the automatic path:

- proxy `soveren-sandbox-egress:3128`
- public proxy uplink isolated from tenant containers

Tenant networks and host firewall rules are always created dynamically by the
platform when a tenant sandbox is acquired.

The proxy permits public HTTP/HTTPS and denies private, loopback, link-local,
and cloud metadata destinations. It is capped at 64 MiB, 0.25 CPU, and 64 PIDs.

## Trusted Control Plane

The application service that imports `soveren-agent-platform` needs Docker CLI
access. If it runs in compose, mount the host Docker socket only into that
trusted service:

```yaml
services:
  agent:
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
```

Never mount the socket into tenant sandbox containers or expose Docker commands
as model tools.

## Credentials

Use one explicit credential provider:

- `CodexApiKeyCredentials` streams an API key to `codex login --with-api-key`.
- `CodexAuthFileCredentials` streams a trusted file-based Codex login cache.
- `ExistingCodexCredentials` reuses credentials already stored in that tenant
  container.

Credential bytes are sent over process stdin. They are not added to Docker
arguments, environment metadata, labels, or the image. A tenant container still
contains its own Codex login cache and must not be shared across trust
boundaries.

## Verification

```bash
bash scripts/smoke_sandbox.sh
```

The smoke test builds both pinned images, verifies the CLI, exercises automatic
runtime acquisition and all resource limits supported by the test host, waits
for proxy health, confirms
public OpenAI connectivity, and confirms private/metadata, peer-container, and
host-gateway destinations are denied even when proxy variables are bypassed.

The release workflow publishes and anonymously pulls both GHCR images before it
publishes the matching PyPI version. New GHCR packages must therefore be made
public in the organization package settings before the first release can pass.
