# Read-only GitHub connector

## Accepted result and boundary

Optional `soveren_agent_platform.github` module and `github` extra. Trusted host
process owns GitHub App signing, installation-token refresh, fixed-origin HTTP,
and bounded read tools. No sandbox credentials, clones, shell, service, queue,
DB, generic proxy, GitHub business writes, or Pulsy implementation here.
Pulsy owns App registration/key storage, tenant/user/conversation authorization,
repository allowlists and explicit ref/PR selection. GitHub is authoritative;
refs resolve once per connector lifetime to immutable commit/tree identities.

Base: freshly fetched origin/main c368a6c453caf061aa7f94b8d5e621c7dde31bf9.
Branch: codex/github-readonly-connector; initial HEAD equals base, ancestor check
passed, no upstream. No commit/push/PR/merge/release/deploy authorization.

## Risk preflight: guards and oracles

| Applicable matrix rows | Guard | Oracle through registered tools + real HTTP/auth adapter |
| --- | --- | --- |
| Tenant/ownership/access | Immutable trusted conversation/repo/ref/operation scope, bind registry; reject unknown arguments, repo and SHA before HTTP | Foreign scope/repo/ref/SHA and disallowed operation cause no provider call; no secrets in output/log/exception/repr |
| Source of truth; time/version | Cache explicit ref resolution; pin tree/blob and PR compare to SHA | Move branch/PR between requests; subsequent reads retain original commit/base/head |
| Lifecycle; concurrency | Lazy startup, shared on-demand refresh ahead of expiry, bounded 401 retry, terminal local revoke/close | Concurrent expiry, invalidated token, denied refresh, revoke during refresh, client shutdown |
| Failure/empty/partial | Safe typed errors, inaccessible 404 distinct from local path absence, partial flags | 401/403/404/429, timeout, malformed JSON/non-JSON 5xx, empty file/tree, absent patch |
| Boundedness | Bounded HTTP stream/deadline, file/depth/line/byte/tree/diff caps, explicit continuation/incompleteness | Oversized/binary/symlink, provider truncated tree, output pages, compare 300-file cap |
| Contracts/compatibility; scope fidelity | Optional extra, reuse registry factory/scope, no core imports of optional libraries | Base install imports; public registration and full existing suite |
| Production path/test oracle | Registry -> handler -> connector -> JWT/token -> HTTP fixture | Verify signed JWT, minimal token scopes, exact methods/paths and immutable identifiers |

## Implementation sequence

1. Add typed trusted config, safe failures, authentication/HTTP lifecycle.
2. Add scoped ref/tree/text-file/PR metadata + immutable compare diff tools.
3. Add HTTP-contract tests through public registration and failure/concurrency cases.
4. Document reusable integration exception to app-owned provider clients and a
   self-contained Pulsy handoff. Run ruff, mypy, pytest; review and fix defects.

## Explicit later work

Branch-aware content search remains separate. GitHub REST code search considers
only the default branch and indexed files <384 KB; it is not arbitrary-SHA search.
This slice supplies tree/path discovery and individual files, with no hidden
repository download/index. Live GitHub App acceptance requires app-owned setup;
local HTTP-contract verification does not establish live acceptance.

Primary references checked 2026-10-16:
- https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/authenticating-as-a-github-app-installation
- https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-a-json-web-token-jwt-for-a-github-app
- https://docs.github.com/en/rest/git/trees
- https://docs.github.com/en/rest/commits/commits#compare-two-commits
- https://docs.github.com/en/rest/search/search#search-code

## Actual result and verification (2026-10-16)

Implemented steps 1-4. New module files: `github/contracts.py`, `http.py`,
`client.py`, `tools.py`, `__init__.py`. Packaging: `pyproject.toml`, `uv.lock`.
Tests: `tests/test_github_connector.py`. Contract updates: ARCHITECTURE, PORTS,
API, CONSUMING_APP; separate GITHUB_CONSUMER_HANDOFF for Pulsy.

Selected guards were exercised through public registration, real JWT signing,
installation token POST and production aiohttp adapter against a local HTTP
server (the fixture redirects only the fixed provider origin to loopback).
36 connector cases cover cross-tenant/conversation/repo/ref/PR/SHA rejection,
operation scope, branch pinning, LF line/byte windows/citations, empty/binary/
symlink/oversized/missing files, tree pages/provider truncation, PR compare cap,
rename/delete/add citations, merge-base source reads, expiry/concurrent refresh,
401 recovery, denied refresh without stale fallback, revoke during refresh,
shutdown, safe errors/logs, actual HTTP timeout/stream cap and bounded admission.
Core imports also pass with GitHub-only dependencies explicitly blocked.

Self-review fixes: continuation's default end follows its start line; LF-only
line numbering matches source citations; oversized first page items fail rather
than returning an empty non-advancing cursor; model JSON has an aggregate cap;
explicit SHA resolutions must match provider identity; dot/dotdot repository
names and refs are rejected before URL normalization; public host exceptions
are sanitized; rename base citations use the previous path and absent diff sides
are null; comparison merge-base is authorized for related file reads.

Final required gate: `uv run ruff check src tests && uv run mypy && uv run pytest
--tb=short` passed: ruff clean, mypy clean (134 source files), 859 passed and 13
skipped in 20.32 s. Skips require explicitly selected native Codex binary or
Squid test image; they are unrelated to this module. Local wheel and sdist built
successfully; inspected wheel's 5 module files and extra-marked dependencies,
and confirmed consumer handoff in sdist. `git diff --check` clean.

Branch remains codex/github-readonly-connector; HEAD and origin/main both equal
c368a6c453caf061aa7f94b8d5e621c7dde31bf9, ancestry verified, upstream unset.
All implementation changes are uncommitted; no push/PR/merge/release/deployment.

Limits retained: GitHub.com only; no branch-aware content search, no clone or
index; provider patch completeness cannot be guaranteed, recursive trees and
compare file lists can be partial. Ref/PR snapshots and authorization live for
one connector/registry lifetime. Live GitHub App and Pulsy/Codex acceptance are
not performed. The owning app must supply credentials and grants and integrate
terminal revoke/close with its runtime lifecycle.

## Review fixes preflight (2026-10-17)

Expected result: GitHub's canonical repository casing is accepted without
weakening repository ownership checks; concurrent token-refresh waiters share
both successful and failed attempts. Scope: github client/HTTP lifecycle,
connector HTTP-contract tests and the corresponding lifecycle documentation.
No Pulsy wiring, publication, new service/storage or authorization expansion.
The branch continues from its original recorded base; origin/main has advanced
independently to 5d4c8badc4bf7c0b6c3a4701724a4d4483f85c2e. No branch switch/rebase.

Applicable guards/oracles before editing:
- Access/source-of-truth: compare canonical GitHub repository identity ignoring
  case; preserve rejection of a different owner or repository. Test through
  registered PR tools and the production adapter, including mixed-case input.
- Concurrency/failure: one in-progress refresh attempt and shared outcome for
  existing waiters, including 403 and actual HTTP timeout; a later call can
  explicitly retry after the failed attempt completes. No stale-token fallback.
- Lifecycle/boundedness: cancelling one waiter does not cancel other callers;
  last-waiter cancellation, revoke and close cancel/join refresh work. At most
  one refresh task; no periodic/background refresh loop or detached orphan.
- Production-path/compatibility: assert provider POST counts and tool results,
  retain 401 recovery and scope-denial coverage; run ruff, mypy and full pytest.

## Review fixes result (2026-10-17)

Both P2 findings fixed. PR identity validation compares canonical owner/repo
case-insensitively and still rejects a different owner or repository. Refresh
uses one owned on-demand task shared by current waiters, including failures;
there is no error-cache fallback or automatic retry loop. An individual waiter
can cancel independently. Final-waiter cancellation joins the abandoned task;
revoke cancels it and close joins it before HTTP-client shutdown. New reads
join pending cancellation cleanup before starting a replacement refresh.

The regression tests first reproduced both findings on the old code (5 failures:
2 casing cases, 403/503/actual-timeout shared-refresh cases). The fixed production
path passes 50 connector tests, including foreign owner/repo rejection,
concurrent failed 401 recovery, cancelling either waiter, cancelling all waiters,
new-read/cancellation interleaving, revoke and close. API/ports/consumer handoff
were updated to describe these lifecycle guarantees.

Required final gate passed: ruff clean; mypy clean (134 source files); full pytest
873 passed, 13 skipped in 20.00 s. The same external Codex/Squid skips remain.
Local wheel/sdist rebuilt successfully; no package publication or live GitHub App
acceptance. Changes remain uncommitted on codex/github-readonly-connector.

## Second review fixes preflight (2026-10-20)

Expected result: concurrent initial reads of the same ref or PR share one result,
including a failed token refresh; malformed tree/blob identities or byte counts
cannot produce a successful citation. Ownership remains the platform GitHub
adapter and its HTTP-contract fixture. No new service, persistence, error cache,
release, branch change or application runtime change.

Selected guards and production-path oracles:
- Concurrency/lifecycle: coalesce only in-progress reads by repository/ref or
  repository/PR. Independent selections proceed concurrently. One cancelled
  caller preserves peers; final cancellation and terminal shutdown cancel/join
  owned work. Assert registry results and provider request counts, including a
  later explicit retry after failure.
- Source/failure/bounds: validate returned tree/blob SHA against the requested
  object and decoded bytes against both declared sizes before citing content.
  Invalid envelopes return invalid_provider_response; actual empty files remain
  successful. Existing size limits and access denial remain enforced.
- Contract/test oracle: faithful HTTP envelopes include object SHA; reproduce
  both findings before production edits, then run connector tests and the full
  required gate. The separate consumer owns its fixture update; record the exact
  change in the handoff without editing the sibling checkout or rebuilding it.

## Second review fixes result (2026-10-20)

Both P2 findings fixed within the GitHub client. Initial ref/PR snapshots use
one owned in-progress task per selection, shared by current waiters. Independent
keys run concurrently; only successful snapshots are retained. Cancellation of
one caller preserves peers; final cancellation joins the abandoned read, revoke
cancels it and close joins it. No error TTL, background loop or new service.

Tree responses now match the requested SHA during discovery and file traversal.
Blob responses match their requested SHA, tree entry size and decoded byte
length. Malformed base64 is a protocol error; binary/invalid UTF-8 remains a
non-text error. Existing successful empty-file behavior and bounds are retained.

Before production edits, 16 selected regressions failed on the old client:
403/503/timeout at initial ref/PR pinning issued eight failed refresh POSTs,
independent selections were serialized, and invalid object envelopes were
accepted or misclassified. The final suite adds 36 cases (86 connector cases in
total), including cancellation/shutdown during both token and metadata requests,
shared metadata failures and a later explicit retry.

Required gate passed: `uv run ruff check src tests && uv run mypy && uv run pytest
--tb=short`: ruff clean, mypy clean (134 source files), 909 passed and 13 skipped
in 42.89 s. Skips require explicitly selected native Codex or Squid artifacts.
Self-review checked request ownership, task cleanup, unchanged access checks,
snapshot lifetime and error/citation semantics. `git diff --check` is clean.

Branch and HEAD remain unchanged at the original recorded base; local
origin/main is now 98326d08ee6d4f3d048882609fc4c78738d5a88a. No branch rewrite,
commit, push, publication or deployment. The previously built 0.11.0 candidate
wheel predates these fixes and is not evidence for this revision. The consumer
handoff records the two SHA fields required in Pulsy's own HTTP fixture; that
separate checkout and package environment were not changed. No live check or
fresh combined consumer build is claimed by this fix.

## Local release assembly preflight (2026-10-20)

Expected result: local 0.12.0 wheel/sdist containing current main core plus the
reviewed GitHub connector, ready for the owning consumer to validate. Scope is
platform assembly, version/extra metadata, lock, tests and artifacts; no consumer
edits, commit, tag, publication, image, server, secret or deployment work.

Source of truth: fresh origin/main 98326d08ee6d4f3d048882609fc4c78738d5a88a
(0.11.0) for all existing core files; the unchanged original connector checkout
for the five reviewed github modules and regression tests. New local branch
codex/github-platform-local-release starts exactly at that fresh SHA, with no
upstream. Original codex/github-readonly-connector remains untouched. Version
0.12.0 is absent from both local and remote tags checked before assembly.

Applicable guards/oracles before edits:
- Source/contract/scope: apply only the connector's additive docs/pyproject diff,
  copy its new module/tests/handoff, and regenerate the fresh main lock. Verify
  core files remain byte-identical to main and connector files to reviewed source.
- Concurrency/failure/access: retain production registry/auth regression coverage
  for shared ref/PR attempts, cancellation/close, SHA and byte-count validation.
- Packaging/production path: run full ruff/mypy/pytest in the clean assembly,
  build wheel and sdist, check metadata/dependencies and every packaged module,
  then run connector tests against an isolated installation of the exact wheel.
- Delivery/lifecycle: retain original checkout hashes, record new branch/base,
  artifact sha256 and limitations. Pulsy owns its fixture SHA update and testing
  with this exact package; a local artifact is not a published or deployed release.

Assembly packaging guard adjustment: fresh main validates release-version links,
dependency ranges, Compose image tags and an exact sdist documentation allowlist.
Update those coupled metadata references to 0.12.0 and add only the packaged
GitHub handoff to the allowlist; preserve the validator and all other exclusions.
Default image tags derive from package version, so the handoff explicitly states
that 0.12.0 images are not built/published and local consumers need their explicit
working image selections. No runtime behavior workaround or validation bypass.

The initial assembled gate exposed three release-coupled test cases with literal
0.11.0 image expectations. Update only those release tag literals in two test
files to 0.12.0; retain their runtime composition/capacity assertions unchanged.
Also align the current Telegram install/checklist examples to the new dependency
range. Historical migration notes remain historical. No core source edits.

First fresh-main full run: 1026 passed, 14 skipped, four failures. Three were
release-coupled 0.11.0 image literals already updated. The fourth was the injected
100 ms HTTP timeout remaining active during the success retry in the ref/PR
regression fixture. Restore normal 20-second HTTP timeout only after observing
the required failed cohort; add 120 ms successful metadata latency to prove the
retry is no longer measured against the injected failure budget. Apply the same
fixture repair to its sibling tree-refresh test. All failure/token-count/retry
assertions stay in place; no production timeout or auth code changes. Original
source checkout stays untouched; assembled test delta is recorded explicitly.

Image-boundary clarification from the supported composition API: only the broker
image is publicly overridable; sandbox/egress images remain version-coupled.
Therefore the local wheel supports registry/HTTP checks now, while full Docker
runtime acceptance requires matching 0.12.0 images in a later authorized step.
The handoff supersedes the earlier suggestion to select all images explicitly.
No internal-default patch, old-image retag or image build is performed.

## Local release assembly result (2026-10-20)

Local version 0.12.0 assembled in
/private/tmp/soveren-github-platform-local-release-20261020 on
codex/github-platform-local-release. HEAD/base remains the freshly fetched
origin/main 98326d08ee6d4f3d048882609fc4c78738d5a88a; ancestry verified, no
upstream. The original branch, HEAD, status and all 14 changed/new file hashes
were preserved. Five reviewed connector modules are byte-identical; all 161
existing core/package files match fresh main. The only existing-test edits are
release tag literals in two files; the copied connector tests additionally
restore normal deadlines only for success retries after injected timeout.

Final Python 3.13.3 gate: ruff (including scripts/broker), mypy (139 source files),
1030 passed and 14 skipped in 56.27 s. Skips require an explicit native Codex,
Squid image or egress/broker Docker image pair. Release validation, sdist inventory
and twine checks passed. uv built the wheel from the newly built sdist. Every
packaged module/data file matched the assembled source; optional github dependency
markers, telegram extra and py.typed were checked. An isolated non-editable
installation of the exact wheel passed all 86 connector tests in 5.55 s; imported
module paths are outside both source checkouts and all five module hashes match.

Artifacts:
- /private/tmp/soveren-github-local-release-20261020/soveren_agent_platform-0.12.0-py3-none-any.whl
  sha256: 21ed835714b6bfb9d6ae753fd9bce4c8df405cd81f1f0ec8ceedece01c8e3ca6
- /private/tmp/soveren-github-local-release-20261020/soveren_agent_platform-0.12.0.tar.gz
  sha256: e920976fd51f3707b919752717b3bd4c694abdfbceb9b5b922a85b2b4657b5c5

Artifact manifest: /private/tmp/soveren-github-local-release-20261020/artifact-manifest.json
Installed-wheel proof: /private/tmp/soveren-github-local-release-20261020/installed-wheel-check.json

No commit/push/PR/merge/tag/PyPI/image publish, server change or deployment.
Pulsy checkout/environment and real keys were untouched. The owning consumer
installs this exact wheel with github extra and updates its HTTP fixture SHA
fields. Registry/HTTP integration can proceed locally; full Docker runtime
acceptance requires matching 0.12.0 images, which this step does not build.
Only broker-image override is public; no internal default overlay/retag workaround.

## Local runtime-image completion preflight (2026-10-20)

Continue the same local 0.12.0 package assembly with the three owning runtime
images, linux/amd64 for the consumer server architecture. Local Docker Desktop
unix endpoint and buildx desktop-linux support amd64 emulation on this aarch64
host. Keep owning Dockerfiles, pinned base digests/default version arguments and
root build context from publish workflow. No source, branch or version changes.

Guards/oracles: --load only; no login/push, remote Docker/SSH, host-network or
real credentials/model call. Inspect local image IDs, linux/amd64, entrypoints,
users and copied entrypoint/config content. Run minimal offline node/Codex and
Python/config-parse smoke in --network none containers, with bounded cleanup.
Retain both package archive sha256 values and write a separate runtime report.
No full sandbox isolation/provider/Telegram acceptance is implied by these checks.

## Local runtime-image completion result (2026-10-20)

All three owning Dockerfiles built with local desktop-linux buildx, --platform
linux/amd64 --load, preserving pinned bases and default ARG values. No branch,
version or production source edits. Local image IDs:
- ghcr.io/neureca/soveren-codex-sandbox:0.12.0: sha256:92049e2224a3f107bc3902bd0348708d0f97d3c04cbd69122a93b5a60b951b8f (linux/amd64)
- ghcr.io/neureca/soveren-sandbox-egress:0.12.0: sha256:938d07897442ce724649655c7b049af1e70d46e6f4c61e7e7e46d69e14de776a (linux/amd64)
- ghcr.io/neureca/soveren-credential-broker:0.12.0: sha256:c9e8b3d4a00f888f59e4b63efbe147c655dd5289e82795c185a79ce3126d33fc (linux/amd64)

Offline smoke passed with --network none and no real credentials/host mounts:
Node 22.23.1, codex-cli 0.143.0 through the default sandbox entrypoint, uid 10001;
egress entrypoint/config hashes, compilation and both base/routed Squid parse;
broker entrypoint/admin script hashes, compilation, aiohttp 3.14.1 and creation
of an empty-registry app. Temporary smoke containers were removed. No full
network/isolation/provider/model/Telegram acceptance is claimed. No login/push,
remote Docker/SSH, image transfer, server operation, commit or tag.

Separate proof: /private/tmp/soveren-github-local-release-20261020/runtime-images-report.json
Build logs/metadata/commands and input hashes are stored alongside it. Package
wheel/sdist were not rebuilt; their original sha256 values remain unchanged.
Build report and artifact manifest link the runtime report, superseding the
package-stage note that the images had not yet been built. Packaged handoff
remains the original package-stage snapshot; no archive rewriting was done.

## Main release delivery preflight (2026-10-20)

User explicitly authorized PR/push, merge to main, then tag/Publish of 0.12.0,
superseding the earlier local-only boundary for platform delivery. Original
connector checkout and Pulsy server remain excluded. Fresh origin/main is still
98326d08ee6d4f3d048882609fc4c78738d5a88a; no remote work branch or v0.12.0 tag.

Guards/oracles: preserve reviewed source and full test assertions; update public
handoff to the durable release contract; inventory scripts against the final
commit; use protected PR with all required CI and no bypass/force; verify main
merge SHA and tag ancestry before pushing v0.12.0; let Publish build/upload images
and package, never push local images; verify Publish/PyPI digests and anonymous
runtime manifests. HTTP503 is an immediate stop, without API fallback/retry.

Operational inventory: scripts/validate_sdist.py changes the exact packaged-docs
allowlist only. It is a local/CI read-only archive validator, not a migration or
server operation. No DB/schema/backfill scripts; no post-deploy data work.
