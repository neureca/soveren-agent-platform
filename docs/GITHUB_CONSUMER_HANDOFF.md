# GitHub read-only consumer handoff (Pulsy)

The optional GitHub integration is part of platform version 0.12.0. Applications
own their authorization policy and bootstrap wiring. Pulsy changes belong in its
own repository; publishing the platform does not enable GitHub tools in an app.

## Application responsibilities

Register a GitHub App, install it only on approved repositories, grant
**Contents: read** and (when PR tools are enabled) **Pull requests: read**.
Load App ID (or client ID), installation ID and PEM signing key through the
application's trusted secret store. Do not ask participants to paste secrets
into chats. GitHub installation tokens expire after one hour; the platform
creates/refreshes them on demand before expiry and keeps them only in host
memory. It does not use the static credential broker for token refresh.

Resolve user/chat authorization and select exact repositories, refs (such as
test/dev) and PR numbers in trusted application code. The model may choose only
among that immutable grant. Keep each connector alive with its conversation
registry. A new selection requires a new connector and registry, with the old
connector revoked/closed. Existing runtime registry factories are cached per
conversation; replacing a cached registry requires an app-controlled runtime
lifecycle, not mutating the old authorization grant.

## Bootstrap seam

For the current release, after the 0.13.0 Publish workflow succeeds, install
`soveren-agent-platform[github]==0.13.0` from the package registry. For local
development only, use the guide's uv source override. Add registration
to Pulsy's existing `_conversation_tool_registry_factory`; keep its other tools.
The following is application pseudocode around the public platform API:

```python
from soveren_agent_platform.github import (
    GitHubAccess, GitHubAppCredentials, GitHubConnector,
    GitHubRepositoryAccess, register_github_tools,
)
from soveren_agent_platform.sessions import DynamicToolRegistry

# Bootstrap: application_secrets loads PEM bytes in the trusted process.
credentials = GitHubAppCredentials(app_id, installation_id, private_key_bytes)
connectors = {}  # App-owned lifecycle collection, never exposed as a tool.

def tools_for(scope):
    registry = DynamicToolRegistry()
    # App policy returns grants only for this authenticated tenant/conversation.
    grants = application_authorization.github_grants(scope)
    if grants:
        connector = GitHubConnector(
            credentials=credentials,
            access=GitHubAccess(scope, tuple(
                GitHubRepositoryAccess(
                    repository=g.repository,
                    refs=tuple(g.selected_refs),
                    pull_requests=tuple(g.selected_pr_numbers),
                    operations=frozenset(g.allowed_read_operations),
                ) for g in grants
            )),
        )
        connectors[scope] = connector
        register_github_tools(registry, connector, scope=scope)
    # Register existing history/schedules/database/app tools here too.
    return registry

# Supply tools_for as tool_registry_factory on the existing Codex runtime.
# After runtime/tool calls stop at application shutdown:
# for connector in connectors.values():
#     await connector.close()
# On permission loss, immediately connector.revoke(), then await close().
```

Do not log credentials, provider headers/bodies, or private keys. Do not mount
keys into sandboxes. No direct REST/shell/clone tool is needed. The connector
makes only fixed GitHub.com GET reads plus the authentication token POST.

## Acceptance in the owning consumer

1. An authorized chat resolves its explicitly selected ref; tree and cited file
   lines match the immutable commit. Moving the branch does not change this
   connector's reads. A new trusted selection gets a new registry/connector.
2. Another tenant or chat, unauthorized repo/ref/PR, arbitrary SHA, URL/method,
   or unknown scope argument is denied before any provider request.
3. Concurrent reads near token expiry issue one refresh, including when it
   fails with 403/5xx/timeout. A later read may retry. Cancelling one waiter
   preserves others; cancelling all waiters or closing the connector leaves no
   refresh task running. Invalidated tokens recover once; denied installation/
   permissions surface typed failures. Local revoke prevents further results
   even while refresh is pending.
   Include concurrent initial `resolve_ref` and `read_pull_request` calls: the
   same selection shares its initial read and error, and independent selections
   do not wait behind a global provider-request lock.
4. A PR snapshot identifies base/head/merge-base. Diff windows retain those
   identities after PR movement; missing patches and the 300-file provider cap
   remain explicit. Fork failures never expand repository scope.
5. Binary/oversized files, incomplete trees, timeouts, rate limits and 404
   inaccessible results remain distinguishable from empty successful reads.
   Tree/blob SHA must match the requested object; decoded blob bytes must match
   both declared sizes. Invalid envelopes return `invalid_provider_response`.
6. Verify key/token absence from model input/output, sandbox filesystem and logs;
   stop the app and confirm owned HTTP clients close. Exercise a public dynamic
   tool call through the actual consuming Codex runtime.

Platform tests cover registry -> production connector/auth -> local HTTP
contract fixtures. Release validation and package publication do not establish
live application or Telegram acceptance; the owning consumer verifies its actual
GitHub App and Codex integration separately. Branch-aware content search remains
future scope: indexed default-branch GitHub Search is not a substitute.

## HTTP contract fixtures

Tree fixtures must include the requested top-level `sha` as well as `tree` and
`truncated`. Blob fixtures must include the requested top-level `sha`, `encoding`,
`size` and encoded `content`. Tree entry size, blob size and decoded byte length
must agree. A true empty file has size zero in both envelopes. Missing identities
or inconsistent sizes are `invalid_provider_response`; fixtures must follow the
provider contract instead of weakening adapter validation.

## Package and runtime-image release boundary

Version 0.12.0 is released through a PR merged into `main`, then tag `v0.12.0` on
the confirmed main commit. The Publish workflow validates that ancestry, builds
and publishes the package and the three matching runtime images, and checks
anonymous image pulls before publishing to PyPI. Check that workflow's successful
completion before adopting the registry package in a deployment.

The version-coupled runtime images are:

- `ghcr.io/neureca/soveren-codex-sandbox:0.12.0`
- `ghcr.io/neureca/soveren-sandbox-egress:0.12.0`
- `ghcr.io/neureca/soveren-credential-broker:0.12.0`

The supported composition API allows a broker-image override, while sandbox and
egress image tags follow the package version. Deploy the complete published
package/image set. Locally built wheels and images provide development evidence;
they do not replace the protected Publish workflow or live application acceptance.
