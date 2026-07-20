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

`create_sandbox_manager(...)` creates the process-owned infrastructure manager.
Passing that manager to
`create_sandboxed_codex_backend(..., sandbox_manager=manager)` automatically creates
one internal network per conversation, a public proxy network, one shared
egress container, one shared credential broker per Docker host, and host packet
filter rules when sandbox mode is first used. Conversation traffic is allowed
only to that conversation network's Squid address on port 3128 and the broker's
network-specific address on port 8080. Conversation bridge networks disable
Docker inter-container connectivity, with the host firewall rules providing the
explicit proxy and broker exceptions. Direct traffic to peer
containers and the Docker bridge gateway is dropped. An
application consuming the PyPI package does not need this repository or a
separate setup command.
The resolved subnet and proxy address are retained by the manager. Failed conversation
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
the platform when a conversation sandbox is acquired. The shared credential broker
is also manager-owned because its tenant registries receive keys only from the
trusted control plane.

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

- `CodexApiKeyCredentials` streams an API key only to its shared-broker tenant registry.
- `CodexAuthFileCredentials` streams a trusted file-based Codex login cache.
- `ExistingCodexCredentials` reuses credentials already stored in that conversation
  container.

Tenant-scoped credential registry updates are sent over process stdin to a read-only
broker container. An admin process forwards the payload to a broker-only Unix socket;
the server validates and atomically replaces or removes only that tenant registry. No credential file is
created. Secret bytes are never added to Docker arguments, environment metadata,
labels, the image, conversation `CODEX_HOME`, or conversation filesystem. Codex
receives only a custom provider URL. Raw bytes exist only in the trusted manager and
broker process memory. The built-in OpenAI binding fixes the upstream to
`https://api.openai.com`, accepts only `POST /v1/responses` and
`POST /v1/responses/compact`, strips client auth/project headers, and enforces
tenant rate/concurrency/request-size/request-read-time/response-time policy. Live
registry updates retain each binding's active concurrency and rolling rate state. The
broker also caps aggregate
in-flight requests and buffered bodies per tenant and across all tenants. Its global
default body budget is the smaller of 64 MiB and half of the container cgroup memory
limit; the tenant default is 32 MiB. A tenant registry and its network attachments are
removed when that tenant's last active conversation sandbox stops, and the shared
container is removed when no active tenant registry remains. Bindings whose configured
maximum body cannot fit both effective budgets are rejected.

`HttpCredentialBinding` adds bounded static-header credentials to the same tenant
registry. Each binding fixes one public HTTPS port-443 origin, method set, normalized path
prefixes, request-header allowlist, scope, and usage policy. Conversation scope is
the default; tenant scope explicitly attaches the binding to every managed
conversation network in the organization. Requests require an opaque capability URL
and an allowed broker network interface. The broker derives tenant identity from that
local destination address, never from HTTP input. Method and path-prefix allowlists are required;
the public binding contract has no authorize-all path default. Redirects are not
followed, and arbitrary proxy targets, OAuth refresh, cookies, and query/body secret
injection are not supported. Broker containers reach every provider through managed
Squid rather than the shared public bridge network.

The application owns credential collection, authorization, durable encrypted storage,
and rotation policy. Capability URLs authorize bounded use and must not be logged or
shared across conversations. Idle stop removes that tenant from the shared broker while
the current manager process retains and restores its memory-only registry for resumable
sandboxes. On a control-plane restart, the new manager removes the previous shared broker
before returning the first sandbox handle; applications must provision current
credentials again from their own secret stores.
Rotation and revocation share a lock with the final forwarding admission after request-body
validation. A request admitted before the update can finish; a request still uploading is
revalidated. Sandbox start and stop serialize tenant lifecycle, while shared broker
mutations use one host-level lock. A failed start rolls back the prepared broker network
before removing that conversation network.

The broker prevents direct key disclosure, not authorized capacity use or a provider
that reflects credentials in its response. Code in an authorized conversation sandbox
can still invoke allowed routes, so use least-privilege provider credentials, upstream
budgets, narrow path/method policies, and appropriate request limits.

`CodexAuthFileCredentials` and `ExistingCodexCredentials` are trusted personal
login modes. Their auth cache is sandbox-local and readable by code in that
conversation; do not use them for an organization secret shared with untrusted
participants.

## Verification

```bash
bash scripts/smoke_sandbox.sh
```

The smoke test builds all three pinned images, verifies the CLI, exercises automatic
sandbox acquisition and all resource limits supported by the test host, waits
for proxy health, confirms
public OpenAI connectivity, broker route restrictions and key non-disclosure,
generic binding policy/revocation, and confirms private/metadata, peer-container, and
host-gateway destinations are denied even when proxy variables are bypassed.

The release workflow publishes and anonymously pulls all three GHCR images before it
publishes the matching PyPI version. Before the first release, run the manual
`Bootstrap Runtime Packages` workflow from `main`, make all three newly created
packages Public in the organization package settings, and rerun the bootstrap until
its anonymous pulls pass. GitHub does not expose package visibility through its package
API, so the release workflow verifies this prerequisite instead of attempting to mutate it.
