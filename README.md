# MORAS Agent Runner Demo

Personal local demo for basic Gitea issue and PR review automation, kept outside the MORAS product workspace.

## Scope

This is intentionally not a production workflow. It proves the smallest useful loop through one script:

```text
Gitea issue/PR or signed webhook -> local task package -> optional read-only agent review -> Gitea comment
```

The default package-only mode does not invoke an agent, modify code, create a branch, create a PR, or post back to Gitea. PR reviews that run a real agent post the suggested PR comment automatically unless `--no-post` is passed.

Default locations:

```text
/Users/changyenyu/MORAS-agent-runner/    # runner home, task packages, logs later
/Users/changyenyu/MORAS/                 # read-only MORAS workspace by default
```

Override with:

```bash
MORAS_AGENT_RUNNER_HOME=/path/to/runner MORAS_WORKSPACE=/path/to/MORAS bin/moras-issue-demo list
```

Runtime binary overrides:

```bash
MORAS_CODEX_BIN=/Applications/Codex.app/Contents/Resources/codex bin/moras-issue-demo pr-review 443 --runtime codex
MORAS_CLAUDE_BIN=/path/to/claude bin/moras-issue-demo pr-review 443 --runtime claude
```

## Commands

Create a task package without invoking an agent:

```bash
bin/moras-issue-demo review 442
```

Create a task package and run the first available runtime, preferring Claude then Codex:

```bash
bin/moras-issue-demo review 442 --runtime auto
```

Create a PR review package without invoking an agent:

```bash
bin/moras-issue-demo pr-review 443
```

Create a PR review package and run the first available runtime:

```bash
bin/moras-issue-demo pr-review 443 --runtime auto
```

Run a PR review without posting:

```bash
bin/moras-issue-demo pr-review 443 --runtime codex --no-post
```

Force Codex read-only review:

```bash
bin/moras-issue-demo review 442 --runtime codex
bin/moras-issue-demo pr-review 443 --runtime codex
```

Preview the Gitea comment without posting:

```bash
bin/moras-issue-demo post tasks/<task-dir> --dry-run --suggested-only
```

Post the latest local review for the issue:

```bash
bin/moras-issue-demo post tasks/<issue-task-dir> --suggested-only
```

Preview or manually post a local review for a PR:

```bash
bin/moras-issue-demo post tasks/<pr-task-dir> --dry-run --suggested-only
bin/moras-issue-demo post tasks/<pr-task-dir> --suggested-only
```

List local task packages:

```bash
bin/moras-issue-demo list
```

Open a saved task for human follow-up in Codex Desktop:

```bash
bin/moras-issue-demo open tasks/<task-dir>
bin/moras-issue-demo open 443 --type pr
```

This opens the recorded component workspace in Codex Desktop and prints the saved `prompt.md`, `review.md`, and `record.json` paths. It does not create a new Desktop chat automatically; webhook automation still uses non-interactive `codex exec`.

## Webhook Runner

The webhook runner is part of `bin/moras-issue-demo`. It reuses the old local bridge design: signed Gitea webhooks, delivery/job dedupe in SQLite, one background worker, and PR dedupe by `owner/repo#pr@head_sha`.

Initialize local webhook env:

```bash
bin/moras-issue-demo webhook --init-env
```

Run the listener:

```bash
bin/moras-issue-demo webhook
```

Health check:

```bash
curl -sS http://127.0.0.1:48731/health
```

Webhook endpoint:

```text
POST http://127.0.0.1:48731/gitea
```

Supported webhook actions:

| Gitea event header | Payload action | Match rule | Runner action | Posting behavior | Dedupe key |
| --- | --- | --- | --- | --- | --- |
| `issue_assign` | `assigned` | `assignee`, `issue.assignee`, or `issue.assignees` matches `MORAS_GITEA_USERNAME` | Queue `review <issue>` | Does not post unless `MORAS_GITEA_ISSUE_AUTO_POST=true` | `owner/repo#issue` |
| `issues` | `opened`, `assigned`, `reopened` | `assignee`, `issue.assignee`, or `issue.assignees` matches `MORAS_GITEA_USERNAME` | Queue `review <issue>` | Does not post unless `MORAS_GITEA_ISSUE_AUTO_POST=true` | `owner/repo#issue` |
| `pull_request_review_request` | `review_requested` | `requested_reviewer` or `reviewer` matches `MORAS_GITEA_USERNAME` | Queue `pr-review <pr>` | Posts normalized PR review when the runtime succeeds and `MORAS_GITEA_PR_AUTO_POST=true` | `owner/repo#pr@head_sha` |
| `pull_request` | `opened`, `reopened`, `synchronized`, `synchronize`, `sync` | `pull_request.requested_reviewers` or `requested_reviewers` contains `MORAS_GITEA_USERNAME` | Queue `pr-review <pr>` | Posts normalized PR review when the runtime succeeds and `MORAS_GITEA_PR_AUTO_POST=true` | `owner/repo#pr@head_sha` |
| `pull_request_sync` | `opened`, `reopened`, `synchronized`, `synchronize`, `sync` | `pull_request.requested_reviewers` or `requested_reviewers` contains `MORAS_GITEA_USERNAME` | Queue `pr-review <pr>` | Posts normalized PR review when the runtime succeeds and `MORAS_GITEA_PR_AUTO_POST=true` | `owner/repo#pr@head_sha` |
| `bridge_test` | any | Signed test payload | Record a successful test delivery | Does not create a review or post | `bridge-test` |

Events with unsupported actions, another assignee/reviewer, unmapped local repositories, missing issue/PR numbers, invalid JSON, or duplicate delivery/job keys are recorded as ignored or duplicate and do not run an agent.

Target webhook action policy:

| Trigger | Target action | Automation gate |
| --- | --- | --- |
| Issue assigned to `MORAS_GITEA_USERNAME` | Fetch the issue, create an isolated issue worktree, create a scope package, rate `difficulty`, `workload`, `importance`, and `complexity`, then choose an automation path. | `hard-human-handoff`: stop after report and handoff. `mid-human-review`: prepare scope or draft implementation for human review. `easy-direct`: implement in the isolated worktree, run focused verification, push a `codex/issue-<number>-<slug>` branch, and open a PR. |
| Issue event not assigned to `MORAS_GITEA_USERNAME` | Read the content and decide whether it relates to current work owned by `MORAS_GITEA_USERNAME`. | If related, send a report to the user. If unrelated, ignore after recording delivery. |
| PR review requested from `MORAS_GITEA_USERNAME` | Review the PR and leave a normalized review comment. | Automatic comment is allowed when the review runtime succeeds. |
| PR synchronized / new commits pushed | Decide whether the changed head SHA needs re-review. | Re-review when the head SHA is new and the PR is still relevant to `MORAS_GITEA_USERNAME`; otherwise record as ignored. |
| PR opened or reopened | Read the PR and decide whether it relates to current work owned by `MORAS_GITEA_USERNAME`. | If related, send a report to the user. If unrelated, ignore after recording delivery. |

Current MVS status: PR review request and head-SHA deduped PR re-review already run. Assigned issue runs currently create a review/scope package with ratings and an automation decision; automatic worktree creation, implementation, push, and PR opening are intentionally not enabled yet. Related-work reports for unassigned issues and opened/reopened PRs are the next safe webhook expansion.

Webhook-triggered PR reviews run:

```bash
bin/moras-issue-demo pr-review <pr> --repo <owner/repo> --runtime "$MORAS_GITEA_WEBHOOK_RUNTIME"
```

By default successful PR reviews auto-post their normalized review comment. Issue jobs run Codex but do not auto-post unless `MORAS_GITEA_ISSUE_AUTO_POST=true`.

The webhook process also starts a polling monitor by default. This keeps a local SQLite tracking list of open issues, PRs, comments, review comments, and review-request state. It also queues requested PR reviews with the same `owner/repo#pr@head_sha` dedupe used by webhooks.

```text
MORAS_GITEA_MONITOR_SECONDS=10
MORAS_GITEA_MONITOR_REPOS=K2Lab/moras-brain
MORAS_GITEA_MONITOR_LIMIT=50
MORAS_GITEA_MONITOR_PR_REVIEWS=true
```

Run the monitor without starting the webhook listener:

```bash
bin/moras-issue-demo monitor
```

Run one scan and exit:

```bash
bin/moras-issue-demo monitor --once
```

Inspect the local tracking state:

```bash
bin/moras-issue-demo changes
bin/moras-issue-demo changes --items
```

Serve the local dashboard:

```bash
bin/moras-issue-demo ui
```

Open:

```text
http://127.0.0.1:48730
```

The dashboard follows the same observability shape as Symphony's local UI: state summaries, tracked work, change timeline, jobs, deliveries, task packages, and explicit local actions. It binds to `127.0.0.1` by default and does not expose secrets.

Run the older PR-review-only catch-up once:

```bash
bin/moras-issue-demo sync-review-requests
```

Send a signed local test event:

```bash
bin/moras-issue-demo webhook --send-test
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
- Review mode is read-only.
- Package-only review posting is a separate explicit command.
- Real PR agent reviews post automatically by default; pass `--no-post` to suppress that.
- Prefer passing a task directory to `post`; numeric task resolution is rejected when ambiguous.
- Running `--runtime codex` or `--runtime claude` sends issue/PR context, and possibly repository context inspected by the agent, to that model provider.
