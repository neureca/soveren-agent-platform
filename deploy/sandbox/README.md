# Docker Sandbox Deployment

This directory contains the supported Docker sandbox, egress, and credential-broker images.
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
MVP boundary. Conversation networks must have IPv6 disabled; dual-stack host firewall
policy is outside the MVP boundary and therefore fails closed.

The MVP supports one trusted control-plane process per Docker host. Do not run
overlapping platform replicas against the same managed sandbox labels and
networks. Idle conversation containers are stopped, not deleted, so their Codex state
remains resumable and their bounded writable layers still consume host storage.
Applications expanding beyond a small bounded conversation set need an explicit
retention/deletion policy before rollout.

## Automatic Infrastructure

`create_sandboxed_codex_backend(tenant_id=..., source_id=...)` automatically
creates one internal network per conversation, a public proxy network, one shared
egress container, one credential broker per active organization, and host packet
filter rules when sandbox mode is first used. Conversation traffic is allowed
only to that conversation network's Squid address on port 3128 and tenant
credential broker on port 8080. Direct traffic to peer
containers and the Docker bridge gateway is dropped. An
application consuming the PyPI package does not need this repository or a
separate setup command.
The resolved subnet and proxy address are retained by the runtime. Failed conversation
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
- public proxy uplink isolated from conversation containers

Conversation networks and host firewall rules are always created dynamically by
the platform when a conversation sandbox is acquired. Credential brokers are
also runtime-owned because they are tenant-scoped and receive their key only
from the trusted control plane.

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

Never mount the socket into conversation sandbox containers or expose Docker commands
as model tools.

## Credentials

Use one explicit credential provider:

- `CodexApiKeyCredentials` streams an API key only to a tenant credential broker.
- `CodexAuthFileCredentials` streams a trusted file-based Codex login cache.
- `ExistingCodexCredentials` reuses credentials already stored in that conversation
  container.

API-key bytes are sent over process stdin to a read-only broker container, briefly
land in broker tmpfs, are removed after loading, and remain only in broker process
memory. They are never added to Docker arguments, environment metadata, labels,
the image, conversation `CODEX_HOME`, or conversation filesystem. Codex receives
only a custom provider URL. The broker fixes the upstream to
`https://api.openai.com`, accepts only `POST /v1/responses` and
`POST /v1/responses/compact`, strips client auth/project headers, and enforces
tenant rate/concurrency/request-size policy. It is removed when that tenant's
last active conversation sandbox stops. Broker containers start in the first
authorized private conversation network, attach only to conversations that selected
`CodexApiKeyCredentials`, and reach OpenAI through the managed Squid proxy rather
than the shared public bridge network.

The broker prevents key disclosure, not authorized capacity use. Code in a
conversation sandbox can still invoke the allowed broker routes, so use a
project-scoped OpenAI key, an upstream project budget, and an appropriate
`CredentialBrokerPolicy`.

`CodexAuthFileCredentials` and `ExistingCodexCredentials` are trusted personal
login modes. Their auth cache is sandbox-local and readable by code in that
conversation; do not use them for an organization secret shared with untrusted
participants.

## Verification

```bash
bash scripts/smoke_sandbox.sh
```

The smoke test builds all three pinned images, verifies the CLI, exercises automatic
runtime acquisition and all resource limits supported by the test host, waits
for proxy health, confirms
public OpenAI connectivity, broker route restrictions and key non-disclosure,
and confirms private/metadata, peer-container, and
host-gateway destinations are denied even when proxy variables are bypassed.

The release workflow publishes and anonymously pulls all three GHCR images before it
publishes the matching PyPI version. New GHCR packages must therefore be made
public in the organization package settings before the first release can pass.
