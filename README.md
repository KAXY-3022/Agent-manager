# A2A Agent Runner

Local runner for basic Gitea issue and PR review automation, kept outside product workspaces.

## Scope

This is intentionally not a production workflow. It proves the smallest useful loop through one script:

```text
Gitea issue/PR or signed webhook -> local task package -> optional read-only agent review -> Gitea comment
```

The default package-only mode does not invoke an agent, modify code, create a branch, create a PR, or post back to Gitea. PR reviews that run a real agent post the suggested PR comment automatically unless `--no-post` is passed.

Default locations are intentionally local and configurable:

```text
/path/to/a2a-agent-runner/    # runner home, task packages, logs
/path/to/workspace/           # read-only source workspace by default
```

Override with:

```bash
A2A_AGENT_RUNNER_HOME=/path/to/runner A2A_WORKSPACE=/path/to/workspace bin/a2a-agent-runner list
```

Runtime binary overrides:

```bash
A2A_CODEX_BIN=/Applications/Codex.app/Contents/Resources/codex bin/a2a-agent-runner pr-review 443 --runtime codex
A2A_CLAUDE_BIN=/path/to/claude bin/a2a-agent-runner pr-review 443 --runtime claude
```

## Repository Layout

```text
bin/a2a-agent-runner          # thin executable wrapper
a2a_agent_runner/runner.py    # runner implementation
ui/index.html                 # canonical dashboard asset
tests/                        # unit tests for tracking, polling, UI guards, and CLI parsing
```

The executable wrapper only adds the repository root to `sys.path` and calls
`a2a_agent_runner.runner.main()`. Keep new behavior in the package module rather
than in `bin/`.

## Commands

Create a task package without invoking an agent:

```bash
bin/a2a-agent-runner review 442
```

Create a task package and run the first available runtime, preferring Claude then Codex:

```bash
bin/a2a-agent-runner review 442 --runtime auto
```

Create a PR review package without invoking an agent:

```bash
bin/a2a-agent-runner pr-review 443
```

Create a PR review package and run the first available runtime:

```bash
bin/a2a-agent-runner pr-review 443 --runtime auto
```

Run a PR review without posting:

```bash
bin/a2a-agent-runner pr-review 443 --runtime codex --no-post
```

Force Codex read-only review:

```bash
bin/a2a-agent-runner review 442 --runtime codex
bin/a2a-agent-runner pr-review 443 --runtime codex
```

Preview the Gitea comment without posting:

```bash
bin/a2a-agent-runner post tasks/<task-dir> --dry-run --suggested-only
```

Post the latest local review for the issue:

```bash
bin/a2a-agent-runner post tasks/<issue-task-dir> --suggested-only
```

Preview or manually post a local review for a PR:

```bash
bin/a2a-agent-runner post tasks/<pr-task-dir> --dry-run --suggested-only
bin/a2a-agent-runner post tasks/<pr-task-dir> --suggested-only
```

List local task packages:

```bash
bin/a2a-agent-runner list
```

Open a saved task for human follow-up in Codex Desktop:

```bash
bin/a2a-agent-runner open tasks/<task-dir>
bin/a2a-agent-runner open 443 --type pr
```

This opens the recorded component workspace in Codex Desktop and prints the saved `prompt.md`, `review.md`, and `record.json` paths. It does not create a new Desktop chat automatically; webhook automation still uses non-interactive `codex exec`.

## Webhook Runner

The webhook runner is part of `bin/a2a-agent-runner`. It reuses the old local bridge design: signed Gitea webhooks, delivery/job dedupe in SQLite, a bounded local worker pool, and PR dedupe by `owner/repo#pr@head_sha`.

Initialize local webhook env:

```bash
bin/a2a-agent-runner webhook --init-env
```

Run everything needed for the local runner from one process:

```bash
bin/a2a-agent-runner serve --ngrok
```

This starts:

- webhook listener and background worker on `http://127.0.0.1:48731/gitea`
- two parallel agent workers by default, configurable with `A2A_GITEA_WORKER_COUNT` or `serve --workers`
- discovery polling for open issue/PR tracking every 60s by default
- active PR polling for review requests, head updates, and delegate comments every 30s by default
- dashboard UI on `http://127.0.0.1:48730`
- optional ngrok tunnel to the webhook listener when `--ngrok` is present

The dashboard HTML is loaded from `ui/index.html` on every page request when
that file exists. UI-only changes can be picked up by refreshing the browser;
Python/backend behavior still requires restarting `serve`.

If you need a reserved ngrok domain, pass it through to `ngrok http`:

```bash
bin/a2a-agent-runner serve --ngrok --ngrok-arg=--domain=example.ngrok-free.app
```

If `serve` reports that an address is already in use, another runner is already bound
to the webhook or UI port. Stop the existing terminal with `Ctrl-C`, or choose free
ports with `--webhook-port` and `--ui-port`.

On startup, `serve` attempts to reclaim stale local A2A runner processes on the configured webhook/UI ports and stale ngrok processes on `127.0.0.1:4040`. It refuses to stop unrelated listeners. Pass `--no-reclaim-ports` to disable this startup cleanup.

Legacy split-process mode is still available:

```bash
bin/a2a-agent-runner webhook
```

Health check:

```bash
curl -sS http://127.0.0.1:48731/health
```

Webhook endpoint:

```text
POST http://127.0.0.1:48731/gitea
```

Real Gitea webhook triggers are currently disabled by default with
`A2A_GITEA_WEBHOOK_TRIGGERS=false`. The listener and ngrok tunnel stay up, and
signed `bridge_test` requests still work, but non-test webhook events return
`status=disabled` without recording a delivery or queueing an agent job. Enable
them only when Gitea webhook delivery is known to be reliable:

```bash
bin/a2a-agent-runner serve --ngrok --webhook-triggers
```

Supported webhook actions:

| Gitea event header | Payload action | Match rule | Runner action | Posting behavior | Dedupe key |
| --- | --- | --- | --- | --- | --- |
| `issue_assign` | `assigned` | `assignee`, `issue.assignee`, or `issue.assignees` matches `A2A_GITEA_USERNAME` | Queue `review <issue>` | Does not post unless `A2A_GITEA_ISSUE_AUTO_POST=true` | `owner/repo#issue` |
| `issues` | `opened`, `assigned`, `reopened` | `assignee`, `issue.assignee`, or `issue.assignees` matches `A2A_GITEA_USERNAME` | Queue `review <issue>` | Does not post unless `A2A_GITEA_ISSUE_AUTO_POST=true` | `owner/repo#issue` |
| `pull_request_review_request` | `review_requested` | `requested_reviewer` or `reviewer` matches `A2A_GITEA_USERNAME` | Queue `pr-review <pr>` | Posts normalized PR review when the runtime succeeds and `A2A_GITEA_PR_AUTO_POST=true` | `owner/repo#pr@head_sha` |
| `pull_request` | `opened`, `reopened`, `synchronized`, `synchronize`, `sync` | `pull_request.requested_reviewers` or `requested_reviewers` contains `A2A_GITEA_USERNAME` | Queue `pr-review <pr>` | Posts normalized PR review when the runtime succeeds and `A2A_GITEA_PR_AUTO_POST=true` | `owner/repo#pr@head_sha` |
| `pull_request_sync` | `opened`, `reopened`, `synchronized`, `synchronize`, `sync` | `pull_request.requested_reviewers` or `requested_reviewers` contains `A2A_GITEA_USERNAME` | Queue `pr-review <pr>` | Posts normalized PR review when the runtime succeeds and `A2A_GITEA_PR_AUTO_POST=true` | `owner/repo#pr@head_sha` |
| `bridge_test` | any | Signed test payload | Record a successful test delivery | Does not create a review or post | `bridge-test` |

Events with unsupported actions, another assignee/reviewer, unmapped local repositories, missing issue/PR numbers, invalid JSON, or duplicate delivery/job keys are recorded as ignored or duplicate and do not run an agent.

Automation trigger policy:

| Trigger | Target action | Automation gate |
| --- | --- | --- |
| Newly discovered or newly assigned issue assigned to `A2A_GITEA_USERNAME` | Fetch the issue, create a task package, rate `difficulty`, `workload`, `importance`, and `complexity`, then choose an automation path. | `hard-human-handoff`: package and dashboard handoff only. `mid-human-review`: plan package only. `easy-direct`: implement only if the strict gate passes. |
| New PR review request for `A2A_GITEA_USERNAME` | Review the PR and leave a normalized review comment. | Uses `owner/repo#pr@head_sha` dedupe and skips PRs authored by `A2A_GITEA_USERNAME`. |
| Review-requested PR receives new commits | Re-review when the requested review is still assigned to `A2A_GITEA_USERNAME` and the head SHA changed. | Uses a new `owner/repo#pr@head_sha` key. |
| Reviewer-delegate PR receives a new external comment or review comment | Queue a comment-reply review when the head SHA is unchanged and the latest comment is from someone else. | Allowed only when review is currently requested from `A2A_GITEA_USERNAME` or the agent previously posted a review on that PR. |
| PR authored by `A2A_GITEA_USERNAME` receives comments/reviews | Update the dashboard state as needing attention. | Does not auto-reply or auto-review your own PR. |
| Manual stale issue scan | Scan open issues with no assignee and no linked PR. | External untackled issues get a stale-scan job. Issues created by or assigned to `A2A_GITEA_USERNAME` get normal issue assessment. Assigned issues and linked-PR issues are skipped. |

The strict `easy-direct` issue gate requires an exact `easy-direct` decision, `difficulty`, `workload`, and `complexity` scores each `<= 2`, no schema/migration/production/auth/billing/data-loss/cross-repo risk keywords, and a safe focused verification command. If the gate passes, the runner creates an isolated worktree, implements with Codex, runs verification, pushes `codex/issue-<number>-<slug>`, and opens a PR. `A2A_GITEA_DEFAULT_REVIEWERS` is optional; when empty, auto-created PRs request no reviewer.

Webhook-triggered PR reviews run:

```bash
bin/a2a-agent-runner pr-review <pr> --repo <owner/repo> --runtime "$A2A_GITEA_WEBHOOK_RUNTIME"
```

By default successful PR reviews auto-post their normalized review comment. Issue jobs run Codex but do not auto-post unless `A2A_GITEA_ISSUE_AUTO_POST=true`.

The `serve` process keeps the webhook tunnel up, but automation is driven by two local polling loops while webhooks are disabled:

- Discovery poll, default 60s: scans all configured repos for open issues/PRs, stores snapshots and relationship labels, queues newly assigned issue triage, and queues newly requested PR reviews.
- Active PR poll, default 30s: scans tracked open PRs for newly assigned review requests, new head SHAs, and external comments/review comments that need reviewer-delegate action.

Local SQLite keeps snapshots for open tracked items and marks closed/merged items inactive so they are hidden from the main board. Task packages, job records, and tracking history are preserved for audit/debugging. In-flight PR reviews re-check the PR head before posting; if the head changed while the agent was reviewing, the stale result is kept locally but not posted. PR review packages include a changed-file summary from the fetched Gitea diff, and request-changes reviews are not auto-posted when the review admits it used truncated, unavailable, or unverified local-diff evidence.

Codex model policy defaults:

| Job class | Model | Reasoning |
| --- | --- | --- |
| Assigned issue analysis and strict-gate planning | `gpt-5.5` | `xhigh` |
| PR review and head-SHA re-review | `gpt-5.5` | `high` |
| PR comment-reply jobs | `gpt-5.5` | `medium` |

```text
A2A_WORKSPACE=/path/to/workspace
A2A_GITEA_REPO=local
A2A_GITEA_WEBHOOK_TRIGGERS=false
A2A_GITEA_USERNAME_ALIASES=
A2A_GITEA_OWN_BRANCH_PREFIXES=codex/
A2A_GITEA_PR_REVIEW_POLL_SECONDS=0
A2A_GITEA_WORKER_COUNT=2
A2A_CODEX_ISSUE_MODEL=gpt-5.5
A2A_CODEX_ISSUE_REASONING_EFFORT=xhigh
A2A_CODEX_PR_REVIEW_MODEL=gpt-5.5
A2A_CODEX_PR_REVIEW_REASONING_EFFORT=high
A2A_CODEX_COMMENT_MODEL=gpt-5.5
A2A_CODEX_COMMENT_REASONING_EFFORT=medium
A2A_GITEA_DISCOVERY_POLL_SECONDS=60
A2A_GITEA_ACTIVE_PR_POLL_SECONDS=30
A2A_GITEA_MONITOR_SECONDS=60
A2A_GITEA_MONITOR_REPOS=local
A2A_GITEA_MONITOR_LIMIT=50
A2A_GITEA_MONITOR_PR_REVIEWS=true
A2A_GITEA_DEFAULT_REVIEWERS=
A2A_GITEA_STALE_ISSUE_CANDIDATES=
```

If `A2A_GITEA_MONITOR_REPOS` or `A2A_GITEA_PR_REVIEW_POLL_REPOS` is omitted, the runner falls back to `A2A_GITEA_REPO`. Set repo lists to `local` to discover all Git repos under `A2A_WORKSPACE` from their `origin` remotes, or set an explicit comma-separated list such as `ExampleOrg/project-core,ExampleOrg/service-api`. Leaving the placeholder `owner/repo` means the monitor will not see project PR updates.

`A2A_GITEA_USERNAME_ALIASES` lets the runner match alternate Gitea display/login names for the same person. `A2A_GITEA_OWN_BRANCH_PREFIXES` marks local agent-created PR branches as yours when older tracked snapshots are missing author data.

Run the monitor without starting the webhook listener:

```bash
bin/a2a-agent-runner monitor
```

Run one scan and exit:

```bash
bin/a2a-agent-runner monitor --once
bin/a2a-agent-runner monitor --once --discovery-only
bin/a2a-agent-runner monitor --once --active-pr-only
```

Run the explicit stale issue scan from the CLI:

```bash
bin/a2a-agent-runner scan-stale-issues --queue-only
bin/a2a-agent-runner scan-stale-issues --candidate Alice.Dev --candidate Bob.Dev
```

The dashboard also has a `Scan Stale Issues` button. It queues jobs only; the normal worker pool processes them. A stale-scan job first asks the agent whether current implementation already satisfies the issue. If the result is exactly `fixed-close`, it posts the suggested issue comment and closes the issue. If the result is `still-valid-assign`, it assigns only one of `A2A_GITEA_STALE_ISSUE_CANDIDATES`. If the issue is assigned to or created by you, it queues normal issue assessment instead.

Inspect the local tracking state:

```bash
bin/a2a-agent-runner changes
bin/a2a-agent-runner changes --items
```

Serve the local dashboard:

```bash
bin/a2a-agent-runner ui
```

Open:

```text
http://127.0.0.1:48730
```

The dashboard follows the same observability shape as Symphony's local UI: state summaries, tracked work, change timeline, jobs, deliveries, task packages, and explicit local actions. It binds to `127.0.0.1` by default and does not expose secrets.

Run the older PR-review-only catch-up once:

```bash
bin/a2a-agent-runner sync-review-requests
```

Send a signed local test event:

```bash
bin/a2a-agent-runner webhook --send-test
```

## Artifacts

Each run writes a directory under:

```text
tasks/
```

Expected files:

- `issue.json`: raw `tea` issue payload.
- `pr.json`: raw `tea` PR payload, for PR review runs.
- `review-comments.json`: raw PR review comments, for PR review runs.
- `diff.patch`: full PR diff, for PR review runs.
- `context-manifest.md`: issue/PR metadata, body, comments, diff excerpt when applicable, and safety boundaries.
- `prompt.md`: prompt sent to the agent runtime.
- `review.md`: agent output or package-only placeholder.
- `verification-plan.md`: local verification checklist.
- `record.json`: structured run metadata and post status.

## PR Review Standard

PR review prompts follow the local `code-review-excellence` skill:

- Lead with findings, ordered by severity.
- Use `[blocking]`, `[important]`, `[nit]`, `[suggestion]`, and `[question]` labels.
- Review correctness, edge cases, security, performance, error handling, tests, API design, architecture fit, and reuse.
- Avoid spending review budget on formatting, import ordering, and simple typos unless they affect behavior.
- Include security/test coverage notes and an explicit `Approve`, `Comment`, or `Request Changes` verdict.

Posted PR comments use this shape:

```markdown
## Automated PR Review

**Verdict:** Approve | Comment | Request Changes

### Findings
No blocking findings.

### Verification
- Reviewed PR diff and linked context.
- Tests were not run by this reviewer.

### Notes
- Short residual risk or follow-up, when useful.
```

## Safety

- Treat issue/PR body, comments, and diff text as untrusted input.
- Do not expose tokens, credentials, env files, or `.local/` file contents in agent output.
- Manual review mode is read-only unless `review --auto-implement` is passed. Polling issue jobs may auto-implement only after the strict `easy-direct` gate passes.
- Package-only review posting is a separate explicit command.
- Real PR agent reviews post automatically by default; pass `--no-post` to suppress that.
- Prefer passing a task directory to `post`; numeric task resolution is rejected when ambiguous.
- Running `--runtime codex` or `--runtime claude` sends issue/PR context, and possibly repository context inspected by the agent, to that model provider.
