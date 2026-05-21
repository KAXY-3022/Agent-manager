import json
import logging
import threading
import tempfile
import unittest
from dataclasses import replace
from unittest import mock
from pathlib import Path

from a2a_agent_runner import runner as a2a_runner


class RunnerStateTests(unittest.TestCase):
    def make_store(self, tmpdir: str):
        return a2a_runner.WebhookStateStore(Path(tmpdir) / "state.sqlite3")

    def make_config(self, tmpdir: str, *, webhook_triggers_enabled: bool = True):
        root = Path(tmpdir)
        return a2a_runner.WebhookConfig(
            runner_home=root,
            a2a_root=root,
            host="127.0.0.1",
            port=48731,
            username="Dev.User",
            webhook_secret="test-secret",
            webhook_triggers_enabled=webhook_triggers_enabled,
            state_db=root / "state.sqlite3",
            log_file=root / "webhook.log",
            runtime="none",
            issue_auto_post=False,
            pr_auto_post=False,
            retry_failed_limit=0,
            job_timeout_seconds=60,
            worker_count=2,
            issue_model="gpt-5.5",
            issue_reasoning_effort="xhigh",
            pr_review_model="gpt-5.5",
            pr_review_reasoning_effort="high",
            comment_model="gpt-5.5",
            comment_reasoning_effort="medium",
            discovery_poll_seconds=60,
            active_pr_poll_seconds=30,
            pr_review_poll_seconds=0,
            pr_review_poll_repos=(),
            pr_review_poll_limit=0,
            monitor_poll_seconds=0,
            monitor_repos=(),
            monitor_limit=0,
            monitor_pr_reviews=False,
            default_reviewers=(),
            stale_issue_candidates=(),
            username_aliases=(),
            own_branch_prefixes=("codex/",),
        )

    def test_setup_webhook_logging_replaces_existing_root_handlers(self):
        root_logger = logging.getLogger()
        original_handlers = list(root_logger.handlers)
        original_level = root_logger.level
        try:
            for handler in list(root_logger.handlers):
                root_logger.removeHandler(handler)
            root_logger.addHandler(logging.NullHandler())
            root_logger.setLevel(logging.WARNING)

            with tempfile.TemporaryDirectory() as tmpdir:
                config = self.make_config(tmpdir)
                a2a_runner.setup_webhook_logging(config)
                logging.info("runner-log-marker")
                for handler in logging.getLogger().handlers:
                    handler.flush()

                self.assertIn("runner-log-marker", config.log_file.read_text())
        finally:
            for handler in list(root_logger.handlers):
                root_logger.removeHandler(handler)
                handler.close()
            for handler in original_handlers:
                root_logger.addHandler(handler)
            root_logger.setLevel(original_level)

    def test_branch_prefix_does_not_override_explicit_other_author(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = self.make_store(tmpdir)
            store.record_tracking_snapshot(
                {
                    "repo": "ExampleOrg/service-api",
                    "item_type": "pull_request",
                    "number": 1,
                    "title": "Other user PR",
                    "url": "https://gitea.example.com/ExampleOrg/service-api/pulls/1",
                    "state": "open",
                    "author": "Other.User",
                    "head": "codex/other-user-branch",
                    "head_sha": "abc123",
                    "assignees": [],
                    "reviews": [],
                    "review_comments": [],
                    "comments": [],
                }
            )

            items = store.tracked_items_dicts(
                10,
                username="Dev.User",
                own_branch_prefixes=("codex/",),
                state_filter="open",
            )

        self.assertEqual(len(items), 1)
        self.assertFalse(items[0]["relationships"]["created_by_me"])
        self.assertFalse(items[0]["relationships"]["related_to_me"])

    def test_branch_prefix_marks_missing_author_pr_as_created_by_me(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = self.make_store(tmpdir)
            store.record_tracking_snapshot(
                {
                    "repo": "ExampleOrg/project-core",
                    "item_type": "pull_request",
                    "number": 3,
                    "title": "Local Codex PR",
                    "url": "https://gitea.example.com/ExampleOrg/project-core/pulls/3",
                    "state": "open",
                    "author": "",
                    "head": "codex/issue-438-split-long-loops-sub-sessions",
                    "head_sha": "def456",
                    "assignees": [],
                    "reviews": [],
                    "review_comments": [],
                    "comments": [],
                }
            )

            items = store.tracked_items_dicts(
                10,
                username="Dev.User",
                own_branch_prefixes=("codex/",),
                state_filter="open",
            )

        self.assertEqual(len(items), 1)
        self.assertTrue(items[0]["relationships"]["created_by_me"])
        self.assertTrue(items[0]["relationships"]["related_to_me"])

    def test_assigned_pr_counts_as_review_requested_relationship(self):
        pr_relationships = a2a_runner.snapshot_relationships(
            {
                "item_type": "pull_request",
                "author": "Other.User",
                "assignees": ["Dev.User"],
                "requested_reviewers": [],
                "reviews": [],
            },
            "Dev.User",
        )
        issue_relationships = a2a_runner.snapshot_relationships(
            {
                "item_type": "issue",
                "author": "Other.User",
                "assignees": ["Dev.User"],
                "requested_reviewers": [],
                "reviews": [],
            },
            "Dev.User",
        )

        self.assertTrue(pr_relationships["assigned_to_me"])
        self.assertTrue(pr_relationships["review_requested_from_me"])
        self.assertIn("review", a2a_runner.relationship_labels(pr_relationships))
        self.assertTrue(issue_relationships["assigned_to_me"])
        self.assertFalse(issue_relationships["review_requested_from_me"])

    def test_successful_current_head_review_clears_failed_job_badge(self):
        original_iter_records = a2a_runner.iter_records
        with tempfile.TemporaryDirectory() as tmpdir:
            store = self.make_store(tmpdir)
            head_sha = "abc123"
            store.record_tracking_snapshot(
                {
                    "repo": "ExampleOrg/service-api",
                    "item_type": "pull_request",
                    "number": 2,
                    "title": "Reviewed PR",
                    "url": "https://gitea.example.com/ExampleOrg/service-api/pulls/2",
                    "state": "open",
                    "author": "Other.User",
                    "head": "feature/reviewed",
                    "head_sha": head_sha,
                    "assignees": [],
                    "reviews": [],
                    "review_comments": [],
                    "comments": [],
                }
            )
            failed_dir = Path(tmpdir) / "failed"
            success_dir = Path(tmpdir) / "success"
            failed_dir.mkdir()
            success_dir.mkdir()
            records = [
                (
                    success_dir,
                    {
                        "created_at": "2026-05-21T01:01:00+00:00",
                        "repo": "ExampleOrg/service-api",
                        "item_type": "pull_request",
                        "target_index": "2",
                        "head_sha": head_sha,
                        "runtime_status": "succeeded",
                        "runtime_used": "codex",
                        "posted": True,
                    },
                ),
                (
                    failed_dir,
                    {
                        "created_at": "2026-05-21T01:00:00+00:00",
                        "repo": "ExampleOrg/service-api",
                        "item_type": "pull_request",
                        "target_index": "2",
                        "head_sha": head_sha,
                        "runtime_status": "failed",
                        "runtime_used": "codex",
                        "posted": False,
                    },
                ),
            ]
            a2a_runner.iter_records = lambda: records
            try:
                items = store.tracked_items_dicts(10, username="Dev.User", state_filter="open")
            finally:
                a2a_runner.iter_records = original_iter_records

        self.assertEqual(len(items), 1)
        self.assertTrue(items[0]["review_status"]["reviewed"])
        self.assertEqual(items[0]["job_status"], {})

    def test_successful_current_head_review_clears_stale_running_job_badge(self):
        original_iter_records = a2a_runner.iter_records
        with tempfile.TemporaryDirectory() as tmpdir:
            store = self.make_store(tmpdir)
            head_sha = "abc123"
            store.record_tracking_snapshot(
                {
                    "repo": "ExampleOrg/service-api",
                    "item_type": "pull_request",
                    "number": 2,
                    "title": "Reviewed PR",
                    "url": "https://gitea.example.com/ExampleOrg/service-api/pulls/2",
                    "state": "open",
                    "author": "Other.User",
                    "head": "feature/reviewed",
                    "head_sha": head_sha,
                    "assignees": [],
                    "reviews": [],
                    "review_comments": [],
                    "comments": [],
                }
            )
            store.record_delivery("delivery-1", "pr_comment_active", f"ExampleOrg/service-api#2@{head_sha}:comment-1", "running", "stale running")
            store.reserve_or_retry_job(
                a2a_runner.WebhookJob(
                    kind="pr_review",
                    delivery_id="delivery-1",
                    event_type="pr_comment_active",
                    dedupe_key=f"ExampleOrg/service-api#2@{head_sha}:comment-1",
                    owner="ExampleOrg",
                    repo="service-api",
                    number=2,
                    title="Reviewed PR",
                    url="https://gitea.example.com/ExampleOrg/service-api/pulls/2",
                    head_sha=head_sha,
                ),
                0,
            )
            store.update_job(f"ExampleOrg/service-api#2@{head_sha}:comment-1", "running")
            task_dir = Path(tmpdir) / "success"
            task_dir.mkdir()
            a2a_runner.iter_records = lambda: [
                (
                    task_dir,
                    {
                        "created_at": "2026-05-21T01:01:00+00:00",
                        "repo": "ExampleOrg/service-api",
                        "item_type": "pull_request",
                        "target_index": "2",
                        "head_sha": head_sha,
                        "runtime_status": "succeeded",
                        "runtime_used": "codex",
                        "posted": True,
                    },
                )
            ]
            try:
                items = store.tracked_items_dicts(10, username="Dev.User", state_filter="open")
            finally:
                a2a_runner.iter_records = original_iter_records

        self.assertEqual(len(items), 1)
        self.assertTrue(items[0]["review_status"]["reviewed"])
        self.assertEqual(items[0]["job_status"], {})

    def test_previous_head_review_is_stale_for_current_head(self):
        original_iter_records = a2a_runner.iter_records
        with tempfile.TemporaryDirectory() as tmpdir:
            previous_dir = Path(tmpdir) / "previous"
            previous_dir.mkdir()
            a2a_runner.iter_records = lambda: [
                (
                    previous_dir,
                    {
                        "created_at": "2026-05-21T05:09:12+00:00",
                        "repo": "ExampleOrg/project-core",
                        "item_type": "pull_request",
                        "target_index": "464",
                        "head_sha": "ddeefa4e",
                        "runtime_status": "succeeded",
                        "runtime_used": "codex",
                        "posted": True,
                    },
                )
            ]
            try:
                status = a2a_runner.review_status_for_item(
                    "ExampleOrg/project-core", "pull_request", 464, "d0d6ed79"
                )
            finally:
                a2a_runner.iter_records = original_iter_records

        self.assertFalse(status["reviewed"])
        self.assertTrue(status["stale"])

    def test_issue_triage_status_marks_mid_and_hard_as_human_attention(self):
        original_iter_records = a2a_runner.iter_records
        with tempfile.TemporaryDirectory() as tmpdir:
            mid_dir = Path(tmpdir) / "mid"
            hard_dir = Path(tmpdir) / "hard"
            mid_dir.mkdir()
            hard_dir.mkdir()
            a2a_runner.iter_records = lambda: [
                (
                    mid_dir,
                    {
                        "created_at": "2026-05-21T05:10:00+00:00",
                        "repo": "ExampleOrg/project-core",
                        "item_type": "issue",
                        "target_index": "398",
                        "runtime_status": "succeeded",
                        "automation_decision": "mid-human-review",
                        "automation_status": "mid-human-review",
                        "triage_scores": {"difficulty": 3, "workload": 3, "importance": 4, "complexity": 3},
                    },
                ),
                (
                    hard_dir,
                    {
                        "created_at": "2026-05-21T05:11:00+00:00",
                        "repo": "ExampleOrg/project-core",
                        "item_type": "issue",
                        "target_index": "399",
                        "runtime_status": "succeeded",
                        "automation_decision": "hard-human-handoff",
                        "automation_status": "hard-human-handoff",
                    },
                ),
            ]
            try:
                mid_status = a2a_runner.issue_triage_status_for_item("ExampleOrg/project-core", "issue", 398)
                hard_status = a2a_runner.issue_triage_status_for_item("ExampleOrg/project-core", "issue", 399)
            finally:
                a2a_runner.iter_records = original_iter_records

        self.assertEqual(mid_status["label"], "plan review")
        self.assertTrue(mid_status["human_attention"])
        self.assertEqual(mid_status["attention_reason"], "Issue plan needs human review")
        self.assertEqual(hard_status["label"], "hard handoff")
        self.assertTrue(hard_status["human_attention"])
        self.assertEqual(hard_status["attention_reason"], "Issue needs human ownership")

    def test_tracked_issue_includes_latest_triage_status(self):
        original_iter_records = a2a_runner.iter_records
        with tempfile.TemporaryDirectory() as tmpdir:
            store = self.make_store(tmpdir)
            store.record_tracking_snapshot(
                {
                    "repo": "ExampleOrg/project-core",
                    "item_type": "issue",
                    "number": 398,
                    "title": "Assigned issue",
                    "url": "https://gitea.example.com/ExampleOrg/project-core/issues/398",
                    "state": "open",
                    "author": "Other.User",
                    "assignees": ["Dev.User"],
                    "comments": [],
                }
            )
            task_dir = Path(tmpdir) / "task"
            task_dir.mkdir()
            a2a_runner.iter_records = lambda: [
                (
                    task_dir,
                    {
                        "created_at": "2026-05-21T05:10:00+00:00",
                        "repo": "ExampleOrg/project-core",
                        "item_type": "issue",
                        "target_index": "398",
                        "runtime_status": "succeeded",
                        "automation_decision": "mid-human-review",
                        "automation_status": "mid-human-review",
                    },
                )
            ]
            try:
                items = store.tracked_items_dicts(10, username="Dev.User", state_filter="open")
            finally:
                a2a_runner.iter_records = original_iter_records

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["review_status"]["label"], "plan review")
        self.assertTrue(items[0]["review_status"]["human_attention"])

    def test_issue_snapshot_derives_merged_cross_repo_linked_prs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = self.make_store(tmpdir)
            store.record_tracking_snapshot(
                {
                    "repo": "ExampleOrg/project-core",
                    "item_type": "issue",
                    "number": 438,
                    "title": "Split long loops",
                    "url": "https://gitea.example.com/ExampleOrg/project-core/issues/438",
                    "state": "open",
                    "author": "Other.User",
                    "assignees": ["Dev.User"],
                    "labels": [],
                    "linked_prs": [],
                    "comments": [],
                }
            )
            store.record_tracking_snapshot(
                {
                    "repo": "ExampleOrg/agent-service",
                    "item_type": "pull_request",
                    "number": 64,
                    "title": "feat(agent): add trace artifact API (#438)",
                    "url": "https://gitea.example.com/ExampleOrg/agent-service/pulls/64",
                    "state": "merged",
                    "author": "Dev.User",
                    "head_sha": "abc123",
                    "linked_issues": [438],
                    "comments": [],
                    "review_comments": [],
                    "reviews": [],
                }
            )

            items = store.tracked_items_dicts(
                10,
                item_type="issue",
                username="Dev.User",
                state_filter="open",
            )

        self.assertEqual(len(items), 1)
        snapshot = items[0]["snapshot"]
        self.assertTrue(snapshot["resolved_by_merged_prs"])
        self.assertFalse(snapshot["has_open_linked_pr"])
        self.assertEqual(snapshot["tracked_linked_prs"][0]["repo"], "ExampleOrg/agent-service")
        self.assertEqual(snapshot["tracked_linked_prs"][0]["number"], 64)

    def test_issue_snapshot_not_resolved_when_any_linked_pr_is_open(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = self.make_store(tmpdir)
            store.record_tracking_snapshot(
                {
                    "repo": "ExampleOrg/project-core",
                    "item_type": "issue",
                    "number": 438,
                    "title": "Split long loops",
                    "url": "https://gitea.example.com/ExampleOrg/project-core/issues/438",
                    "state": "open",
                    "author": "Other.User",
                    "assignees": ["Dev.User"],
                    "labels": [],
                    "linked_prs": [],
                    "comments": [],
                }
            )
            for number, state in ((64, "merged"), (65, "open")):
                store.record_tracking_snapshot(
                    {
                        "repo": "ExampleOrg/agent-service",
                        "item_type": "pull_request",
                        "number": number,
                        "title": f"PR #{number} (#438)",
                        "url": f"https://gitea.example.com/ExampleOrg/agent-service/pulls/{number}",
                        "state": state,
                        "author": "Dev.User",
                        "head_sha": str(number),
                        "linked_issues": [438],
                        "comments": [],
                        "review_comments": [],
                        "reviews": [],
                    }
                )

            items = store.tracked_items_dicts(
                10,
                item_type="issue",
                username="Dev.User",
                state_filter="open",
            )

        self.assertEqual(len(items), 1)
        snapshot = items[0]["snapshot"]
        self.assertFalse(snapshot["resolved_by_merged_prs"])
        self.assertTrue(snapshot["has_open_linked_pr"])

    def test_pr_snapshot_extracts_tracker_prd_issue_refs(self):
        snapshot = a2a_runner.build_pr_snapshot(
            "ExampleOrg/project-core",
            {
                "index": 441,
                "title": "feat: activate bounded sub-session MVS",
                "body": "Refs #438\n\nAlso mirrors tracker PRD #439 for review.\n\nTracker PRD: #440",
                "state": "closed",
                "hasMerged": True,
                "author": "Dev.User",
                "comments": [],
                "reviews": [],
            },
            [],
        )

        self.assertEqual(snapshot["linked_issues"], [438, 439, 440])
        self.assertEqual(snapshot["state"], "merged")

    def test_new_external_comment_detected_only_for_other_user(self):
        old_snapshot = {
            "comments": [
                {"id": 1, "author": "Dev.User", "created": "2026-05-21T12:00:00+08:00"},
            ],
            "review_comments": [],
        }
        other_user_snapshot = {
            "comments": [
                {"id": 1, "author": "Dev.User", "created": "2026-05-21T12:00:00+08:00"},
                {"id": 2, "author": "Wang.JiaXuan", "created": "2026-05-21T12:01:00+08:00"},
            ],
            "review_comments": [],
        }
        own_reply_snapshot = {
            "comments": [
                {"id": 1, "author": "Dev.User", "created": "2026-05-21T12:00:00+08:00"},
                {"id": 2, "author": "Dev.User", "created": "2026-05-21T12:01:00+08:00"},
            ],
            "review_comments": [],
        }

        expected = ("Dev.User",)
        self.assertTrue(a2a_runner.has_new_external_comment(old_snapshot, other_user_snapshot, expected))
        self.assertFalse(a2a_runner.has_new_external_comment(old_snapshot, own_reply_snapshot, expected))
        self.assertTrue(
            a2a_runner.new_external_pr_author_comment(
                old_snapshot,
                {**other_user_snapshot, "author": "Wang.JiaXuan"},
                expected,
            )
        )
        self.assertFalse(
            a2a_runner.new_external_pr_author_comment(
                old_snapshot,
                {**other_user_snapshot, "author": "Reviewer.Two"},
                expected,
            )
        )

    def test_comment_reply_job_maps_to_pr_item(self):
        key = "ExampleOrg/project-core#450@819a45:comment-f518c8537592"
        self.assertEqual(
            a2a_runner.job_tracking_item_key(key, "pr_review"),
            "ExampleOrg/project-core#pr-450",
        )

    def test_pull_number_parser_prefers_pulls_url_over_domain_digits(self):
        output = "Created: https://gitea.example.com/ExampleOrg/project-core/pulls/472"

        self.assertEqual(a2a_runner.pull_number_from_create_output(output), 472)

    def test_review_comment_reviewer_is_treated_as_author(self):
        comments = a2a_runner.compact_comments(
            [
                {
                    "id": 10,
                    "reviewer": {"login": "Dev.User"},
                    "body": "Inline review note",
                    "created": "2026-05-21T13:11:00+08:00",
                }
            ]
        )

        self.assertEqual(comments[0]["author"], "Dev.User")

    def test_disabled_webhook_triggers_do_not_record_or_queue_jobs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self.make_config(tmpdir, webhook_triggers_enabled=False)
            bridge = a2a_runner.WebhookBridge(config, start_worker=False)
            body = json.dumps({"action": "assigned", "issue": {"number": 1}}).encode("utf-8")
            headers = a2a_runner.signed_webhook_headers(config, "issues", "delivery-1", body)

            response = bridge.handle_webhook(headers, body)

            self.assertEqual(response.status_code, 202)
            self.assertEqual(response.body["status"], "disabled")
            self.assertTrue(bridge.jobs.empty())
            self.assertEqual(bridge.store.recent_deliveries(10), [])

    def test_bridge_test_still_records_when_webhook_triggers_are_disabled(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self.make_config(tmpdir, webhook_triggers_enabled=False)
            bridge = a2a_runner.WebhookBridge(config, start_worker=False)
            body = json.dumps({"message": "ok"}).encode("utf-8")
            headers = a2a_runner.signed_webhook_headers(config, "bridge_test", "delivery-bridge", body)

            response = bridge.handle_webhook(headers, body)

            deliveries = bridge.store.recent_deliveries(10)
            self.assertEqual(response.status_code, 202)
            self.assertEqual(response.body["status"], "ok")
            self.assertEqual(len(deliveries), 1)
            self.assertEqual(deliveries[0]["event_type"], "bridge_test")
            self.assertEqual(deliveries[0]["status"], "done")

    def test_strict_easy_gate_accepts_safe_easy_direct_review(self):
        review = """# Agent Review

## Automation Decision
easy-direct

- difficulty: 1
- workload: 2
- importance: 3
- complexity: 2

## Verification
Focused verification command: `python -m unittest tests/test_a2a_runner.py`
"""

        passed, reasons, metadata = a2a_runner.issue_strict_easy_gate(
            review,
            {"title": "Small UI fix", "body": "Update a local label.", "labels": []},
        )

        self.assertTrue(passed, reasons)
        self.assertEqual(metadata["decision"], "easy-direct")
        self.assertEqual(metadata["scores"]["difficulty"], 1)

    def test_strict_easy_gate_rejects_risky_or_non_easy_review(self):
        review = """# Agent Review

## Automation Decision
mid-human-review

- difficulty: 1
- workload: 1
- importance: 3
- complexity: 1

## Verification
- python -m unittest tests/test_a2a_runner.py
"""

        passed, reasons, metadata = a2a_runner.issue_strict_easy_gate(
            review,
            {"title": "Add schema migration", "body": "Needs migration and production rollout.", "labels": []},
        )

        self.assertFalse(passed)
        self.assertEqual(metadata["decision"], "mid-human-review")
        self.assertTrue(any("not easy-direct" in reason for reason in reasons))
        self.assertTrue(any("risk keywords" in reason for reason in reasons))

    def test_discovery_poll_queues_assigned_issue_once(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = a2a_runner.WebhookConfig(
                **{
                    **self.make_config(tmpdir).__dict__,
                    "monitor_repos": ("ExampleOrg/project-core",),
                    "monitor_limit": 10,
                    "monitor_pr_reviews": False,
                }
            )
            issue = {
                "index": 398,
                "state": "open",
                "author": {"login": "Other.User"},
                "title": "Assigned issue",
                "url": "https://gitea.example.com/ExampleOrg/project-core/issues/398",
                "body": "Please check.",
                "assignees": [{"login": "Dev.User"}],
                "labels": [],
                "comments": [],
            }
            bridge = a2a_runner.WebhookBridge(config, start_worker=False)

            with mock.patch.object(a2a_runner, "list_open_issues", return_value=[{"index": 398}]), \
                mock.patch.object(a2a_runner, "fetch_issue", return_value=issue), \
                mock.patch.object(a2a_runner, "list_open_prs", return_value=[]):
                events = bridge.discovery_once()
                size_after_first = bridge.jobs.qsize()
                bridge.discovery_once()

            self.assertIn("queued assigned issue triage", "\n".join(events))
            self.assertEqual(size_after_first, 1)
            self.assertEqual(bridge.jobs.qsize(), 1)
            job = bridge.jobs.get_nowait()
            self.assertEqual(job.kind, "issue")
            self.assertEqual(job.dedupe_key, "ExampleOrg/project-core#398")

    def test_active_pr_poll_queues_review_when_request_added_after_creation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = a2a_runner.WebhookConfig(
                **{
                    **self.make_config(tmpdir).__dict__,
                    "monitor_repos": ("ExampleOrg/project-core",),
                    "monitor_limit": 10,
                    "monitor_pr_reviews": True,
                }
            )
            bridge = a2a_runner.WebhookBridge(config, start_worker=False)
            bridge.store.record_tracking_snapshot(
                {
                    "repo": "ExampleOrg/project-core",
                    "item_type": "pull_request",
                    "number": 450,
                    "title": "Needs review",
                    "url": "https://gitea.example.com/ExampleOrg/project-core/pulls/450",
                    "state": "open",
                    "author": "Other.User",
                    "head": "feature/review",
                    "head_sha": "abc123",
                    "requested_reviewers": [],
                    "reviews": [],
                    "review_comments": [],
                    "comments": [],
                }
            )
            pr = {
                "index": 450,
                "state": "open",
                "author": {"login": "Other.User"},
                "title": "Needs review",
                "url": "https://gitea.example.com/ExampleOrg/project-core/pulls/450",
                "head": "feature/review",
                "headSha": "abc123",
                "requested_reviewers": [{"login": "Dev.User"}],
                "reviews": [],
                "comments": [],
            }

            with mock.patch.object(a2a_runner, "fetch_pr", return_value=pr), \
                mock.patch.object(a2a_runner, "fetch_pr_review_comments", return_value=[]):
                events = bridge.active_pr_once()

            self.assertIn("queued requested review", "\n".join(events))
            job = bridge.jobs.get_nowait()
            self.assertEqual(job.kind, "pr_review")
            self.assertEqual(job.dedupe_key, "ExampleOrg/project-core#450@abc123")

    def test_active_pr_poll_queues_review_when_pr_assigned_to_user(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = a2a_runner.WebhookConfig(
                **{
                    **self.make_config(tmpdir).__dict__,
                    "monitor_repos": ("ExampleOrg/project-core",),
                    "monitor_limit": 10,
                    "monitor_pr_reviews": True,
                }
            )
            bridge = a2a_runner.WebhookBridge(config, start_worker=False)
            bridge.store.record_tracking_snapshot(
                {
                    "repo": "ExampleOrg/project-core",
                    "item_type": "pull_request",
                    "number": 451,
                    "title": "Assigned review",
                    "url": "https://gitea.example.com/ExampleOrg/project-core/pulls/451",
                    "state": "open",
                    "author": "Other.User",
                    "head": "feature/assigned-review",
                    "head_sha": "abc123",
                    "assignees": [],
                    "requested_reviewers": [],
                    "reviews": [],
                    "review_comments": [],
                    "comments": [],
                }
            )
            pr = {
                "index": 451,
                "state": "open",
                "author": {"login": "Other.User"},
                "title": "Assigned review",
                "url": "https://gitea.example.com/ExampleOrg/project-core/pulls/451",
                "head": "feature/assigned-review",
                "headSha": "abc123",
                "assignees": [{"login": "Dev.User"}],
                "requested_reviewers": [],
                "reviews": [],
                "comments": [],
            }

            with mock.patch.object(a2a_runner, "fetch_pr", return_value=pr), \
                mock.patch.object(a2a_runner, "fetch_pr_review_comments", return_value=[]):
                events = bridge.active_pr_once()

            self.assertIn("queued requested review", "\n".join(events))
            job = bridge.jobs.get_nowait()
            self.assertEqual(job.kind, "pr_review")
            self.assertEqual(job.dedupe_key, "ExampleOrg/project-core#451@abc123")

    def test_active_pr_poll_queues_rereview_when_requested_head_changes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = a2a_runner.WebhookConfig(
                **{
                    **self.make_config(tmpdir).__dict__,
                    "monitor_repos": ("ExampleOrg/project-core",),
                    "monitor_limit": 10,
                    "monitor_pr_reviews": True,
                }
            )
            bridge = a2a_runner.WebhookBridge(config, start_worker=False)
            bridge.store.record_tracking_snapshot(
                {
                    "repo": "ExampleOrg/project-core",
                    "item_type": "pull_request",
                    "number": 450,
                    "title": "Needs review",
                    "url": "https://gitea.example.com/ExampleOrg/project-core/pulls/450",
                    "state": "open",
                    "author": "Other.User",
                    "head": "feature/review",
                    "head_sha": "oldsha",
                    "requested_reviewers": ["Dev.User"],
                    "reviews": [],
                    "review_comments": [],
                    "comments": [],
                }
            )
            pr = {
                "index": 450,
                "state": "open",
                "author": {"login": "Other.User"},
                "title": "Needs review",
                "url": "https://gitea.example.com/ExampleOrg/project-core/pulls/450",
                "head": "feature/review",
                "headSha": "newsha",
                "requested_reviewers": [{"login": "Dev.User"}],
                "reviews": [],
                "comments": [],
            }

            with mock.patch.object(a2a_runner, "fetch_pr", return_value=pr), \
                mock.patch.object(a2a_runner, "fetch_pr_review_comments", return_value=[]):
                bridge.active_pr_once()

            job = bridge.jobs.get_nowait()
            self.assertEqual(job.dedupe_key, "ExampleOrg/project-core#450@newsha")

    def test_active_pr_poll_replies_only_for_reviewer_delegate_not_author_pr(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = a2a_runner.WebhookConfig(
                **{
                    **self.make_config(tmpdir).__dict__,
                    "monitor_repos": ("ExampleOrg/project-core",),
                    "monitor_limit": 10,
                    "monitor_pr_reviews": True,
                }
            )
            bridge = a2a_runner.WebhookBridge(config, start_worker=False)
            previous = {
                "repo": "ExampleOrg/project-core",
                "item_type": "pull_request",
                "number": 460,
                "title": "Delegate PR",
                "url": "https://gitea.example.com/ExampleOrg/project-core/pulls/460",
                "state": "open",
                "author": "Other.User",
                "head": "feature/delegate",
                "head_sha": "abc123",
                "requested_reviewers": ["Dev.User"],
                "reviews": [],
                "review_comments": [],
                "comments": [{"id": 1, "author": "Other.User", "created": "2026-05-21T10:00:00Z"}],
            }
            bridge.store.record_tracking_snapshot(previous)
            pr = {
                "index": 460,
                "state": "open",
                "author": {"login": "Other.User"},
                "title": "Delegate PR",
                "url": "https://gitea.example.com/ExampleOrg/project-core/pulls/460",
                "head": "feature/delegate",
                "headSha": "abc123",
                "requested_reviewers": [{"login": "Dev.User"}],
                "reviews": [],
                "comments": [
                    {"id": 1, "author": {"login": "Other.User"}, "created": "2026-05-21T10:00:00Z"},
                    {"id": 2, "author": {"login": "Other.User"}, "created": "2026-05-21T10:01:00Z"},
                ],
            }

            with mock.patch.object(a2a_runner, "fetch_pr", return_value=pr), \
                mock.patch.object(a2a_runner, "fetch_pr_review_comments", return_value=[]):
                bridge.active_pr_once()

            delegate_job = bridge.jobs.get_nowait()
            self.assertIn(":comment-", delegate_job.dedupe_key)

        with tempfile.TemporaryDirectory() as tmpdir:
            config = a2a_runner.WebhookConfig(
                **{
                    **self.make_config(tmpdir).__dict__,
                    "monitor_repos": ("ExampleOrg/project-core",),
                    "monitor_limit": 10,
                    "monitor_pr_reviews": True,
                }
            )
            bridge = a2a_runner.WebhookBridge(config, start_worker=False)
            bridge.store.record_tracking_snapshot(
                {
                    **previous,
                    "number": 461,
                    "author": "Dev.User",
                    "requested_reviewers": [],
                    "comments": [{"id": 1, "author": "Other.User", "created": "2026-05-21T10:00:00Z"}],
                }
            )
            own_pr = {
                **pr,
                "index": 461,
                "author": {"login": "Dev.User"},
                "requested_reviewers": [],
            }

            with mock.patch.object(a2a_runner, "fetch_pr", return_value=own_pr), \
                mock.patch.object(a2a_runner, "fetch_pr_review_comments", return_value=[]):
                bridge.active_pr_once()

            self.assertTrue(bridge.jobs.empty())

        with tempfile.TemporaryDirectory() as tmpdir:
            config = a2a_runner.WebhookConfig(
                **{
                    **self.make_config(tmpdir).__dict__,
                    "monitor_repos": ("ExampleOrg/project-core",),
                    "monitor_limit": 10,
                    "monitor_pr_reviews": True,
                }
            )
            bridge = a2a_runner.WebhookBridge(config, start_worker=False)
            bridge.store.record_tracking_snapshot(
                {
                    **previous,
                    "number": 462,
                    "comments": [{"id": 1, "author": "Dev.User", "created": "2026-05-21T10:00:00Z"}],
                }
            )
            reviewer_comment_pr = {
                **pr,
                "index": 462,
                "comments": [
                    {"id": 1, "author": {"login": "Dev.User"}, "created": "2026-05-21T10:00:00Z"},
                    {"id": 2, "author": {"login": "Reviewer.Two"}, "created": "2026-05-21T10:01:00Z"},
                ],
            }

            with mock.patch.object(a2a_runner, "fetch_pr", return_value=reviewer_comment_pr), \
                mock.patch.object(a2a_runner, "fetch_pr_review_comments", return_value=[]):
                bridge.active_pr_once()

            self.assertTrue(bridge.jobs.empty())

    def test_stale_issue_scan_queues_only_untackled_external_issues(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = a2a_runner.WebhookConfig(
                **{
                    **self.make_config(tmpdir).__dict__,
                    "monitor_repos": ("ExampleOrg/project-core",),
                    "monitor_limit": 10,
                }
            )
            bridge = a2a_runner.WebhookBridge(config, start_worker=False)
            issues = {
                "1": {
                    "index": 1,
                    "state": "open",
                    "author": {"login": "Other.User"},
                    "title": "Untackled",
                    "url": "https://gitea.example.com/ExampleOrg/project-core/issues/1",
                    "body": "Needs owner.",
                    "assignees": [],
                    "labels": [],
                    "comments": [],
                },
                "2": {
                    "index": 2,
                    "state": "open",
                    "author": {"login": "Other.User"},
                    "title": "Assigned elsewhere",
                    "url": "https://gitea.example.com/ExampleOrg/project-core/issues/2",
                    "body": "Needs owner.",
                    "assignees": [{"login": "Other.Assignee"}],
                    "labels": [],
                    "comments": [],
                },
                "3": {
                    "index": 3,
                    "state": "open",
                    "author": {"login": "Other.User"},
                    "title": "Has PR",
                    "url": "https://gitea.example.com/ExampleOrg/project-core/issues/3",
                    "body": "Handled by PR #44.",
                    "assignees": [],
                    "labels": [],
                    "comments": [],
                },
            }

            with mock.patch.object(
                a2a_runner,
                "list_open_issues",
                return_value=[{"index": 1}, {"index": 2}, {"index": 3}],
            ), mock.patch.object(a2a_runner, "fetch_issue", side_effect=lambda number, repo, timeout=10: issues[number]):
                events = bridge.stale_issues_once()

            self.assertIn("queued stale issue scan", "\n".join(events))
            self.assertIn("skipped assigned issue", "\n".join(events))
            self.assertIn("skipped linked PR", "\n".join(events))
            self.assertEqual(bridge.jobs.qsize(), 1)
            job = bridge.jobs.get_nowait()
            self.assertEqual(job.kind, "issue_stale_scan")
            self.assertTrue(job.dedupe_key.startswith("ExampleOrg/project-core#stale-1:"))

    def test_stale_issue_scan_related_issue_queues_assessment(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = a2a_runner.WebhookConfig(
                **{
                    **self.make_config(tmpdir).__dict__,
                    "monitor_repos": ("ExampleOrg/project-core",),
                    "monitor_limit": 10,
                }
            )
            bridge = a2a_runner.WebhookBridge(config, start_worker=False)
            issue = {
                "index": 4,
                "state": "open",
                "author": {"login": "Dev.User"},
                "title": "My stale issue",
                "url": "https://gitea.example.com/ExampleOrg/project-core/issues/4",
                "body": "Needs assessment.",
                "assignees": [],
                "labels": [],
                "comments": [],
            }

            with mock.patch.object(a2a_runner, "list_open_issues", return_value=[{"index": 4}]), \
                mock.patch.object(a2a_runner, "fetch_issue", return_value=issue):
                events = bridge.stale_issues_once()

            self.assertIn("queued related issue assessment", "\n".join(events))
            job = bridge.jobs.get_nowait()
            self.assertEqual(job.kind, "issue")
            self.assertEqual(job.dedupe_key, "ExampleOrg/project-core#4")

    def test_stale_issue_decision_and_candidate_extraction(self):
        review = """# Stale Issue Scan
## Stale Issue Decision
still-valid-assign

## Candidate Assignment
Alice.Dev

## Suggested Issue Comment
Still valid and should be assigned.
"""

        self.assertEqual(a2a_runner.extract_stale_issue_decision(review), "still-valid-assign")
        self.assertEqual(
            a2a_runner.extract_candidate_assignment(review, ("Alice.Dev", "Bob.Dev")),
            "Alice.Dev",
        )
        self.assertIn("Automated stale issue scan", a2a_runner.format_stale_issue_comment(review))
        self.assertEqual(
            a2a_runner.job_tracking_item_key("ExampleOrg/project-core#stale-1:abc123", "issue_stale_scan"),
            "ExampleOrg/project-core#issue-1",
        )

    def test_discovery_reconcile_archives_closed_issue_snapshot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = a2a_runner.WebhookConfig(
                **{
                    **self.make_config(tmpdir).__dict__,
                    "monitor_repos": ("ExampleOrg/project-core",),
                    "monitor_limit": 10,
                    "monitor_pr_reviews": False,
                }
            )
            bridge = a2a_runner.WebhookBridge(config, start_worker=False)
            bridge.store.record_tracking_snapshot(
                {
                    "repo": "ExampleOrg/project-core",
                    "item_type": "issue",
                    "number": 398,
                    "title": "Closed issue",
                    "url": "https://gitea.example.com/ExampleOrg/project-core/issues/398",
                    "state": "open",
                    "author": "Other.User",
                    "assignees": ["Dev.User"],
                    "labels": [],
                    "comments": [],
                }
            )
            closed_issue = {
                "index": 398,
                "state": "closed",
                "author": {"login": "Other.User"},
                "title": "Closed issue",
                "url": "https://gitea.example.com/ExampleOrg/project-core/issues/398",
                "body": "",
                "assignees": [{"login": "Dev.User"}],
                "labels": [],
                "comments": [],
            }

            with mock.patch.object(a2a_runner, "list_open_issues", return_value=[]), \
                mock.patch.object(a2a_runner, "fetch_issue", return_value=closed_issue), \
                mock.patch.object(a2a_runner, "list_open_prs", return_value=[]):
                bridge.discovery_once()

            snapshot = bridge.store.tracking_snapshot("ExampleOrg/project-core", "issue", 398)
            self.assertEqual(snapshot["state"], "closed")
            open_items = bridge.store.tracked_items_dicts(
                10,
                item_type="issue",
                username="Dev.User",
                state_filter="open",
            )
            self.assertEqual(open_items, [])

    def test_diff_file_summary_counts_full_pr_diff(self):
        diff = "\n".join(
            [
                "diff --git a/a.py b/a.py",
                "--- a/a.py",
                "+++ b/a.py",
                "@@",
                "diff --git a/docs/old.md b/docs/new.md",
                "--- a/docs/old.md",
                "+++ b/docs/new.md",
            ]
        )

        self.assertEqual(a2a_runner.diff_file_paths(diff), ["a.py", "docs/new.md"])
        summary = a2a_runner.render_diff_file_summary(diff)
        self.assertIn("Changed files: 2", summary)
        self.assertIn("`docs/new.md`", summary)

    def test_pr_manifest_warns_about_stale_local_refs_and_lists_diff_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = a2a_runner.render_pr_manifest(
                {
                    "index": 460,
                    "title": "Example",
                    "state": "open",
                    "user": "Reviewer.One",
                    "head": "feature/example",
                    "headSha": "abc123",
                    "comments": [],
                    "reviews": [],
                },
                repo="ExampleOrg/project-core",
                component=Path(tmpdir) / "project-core",
                task_dir=Path(tmpdir) / "task",
                review_comments=[],
                diff_text="diff --git a/a.py b/a.py\n--- a/a.py\n+++ b/a.py\n",
                max_comments=20,
                max_diff_chars=10,
            )

        self.assertIn("Local Git refs may be stale", manifest)
        self.assertIn("Changed files: 1", manifest)
        self.assertIn("Full diff artifact:", manifest)

    def test_low_confidence_request_changes_review_is_not_auto_postable(self):
        review = """# PR Review
## Suggested PR Comment
## Automated PR Review

**Verdict:** Request Changes

### Findings
- `[blocking]` `a.py:1` Bad.

### Verification
- The prompt-provided diff was truncated and the full diff.patch artifact was not available.
"""

        self.assertTrue(a2a_runner.pr_review_low_confidence_reason(review))

    def test_issue_row_cards_do_not_render_linked_prs(self):
        ui = a2a_runner.load_ui_html()
        marker = "const issueCard ="
        start = ui.index(marker)
        end = ui.index("function fitKanbanToWindow", start)
        issue_card = ui[start:end]

        self.assertNotIn("cardLinks", issue_card)

    def test_pr_cards_do_not_render_redundant_type_or_review_badges(self):
        ui = a2a_runner.load_ui_html()
        marker = "const compactStatusBadges ="
        start = ui.index(marker)
        end = ui.index("const hasReviewRequest =", start)
        card_badges = ui[start:end]

        self.assertNotIn("badge(marker(item.item_type))", card_badges)
        self.assertNotIn("reviewBadge(item)", card_badges)
        self.assertNotIn("attentionBadge(item)", card_badges)

    def test_pr_cards_render_compact_identity_only(self):
        ui = a2a_runner.load_ui_html()
        identity_marker = "const prCardIdentity ="
        identity_start = ui.index(identity_marker)
        identity_end = ui.index("const workCard =", identity_start)
        identity_card = ui[identity_start:identity_end]
        marker = "const workCard ="
        start = ui.index(marker)
        end = ui.index("const issueCard =", start)
        work_card = ui[start:end]

        self.assertIn("repoShortName(item.repo)", identity_card)
        self.assertIn("shortUser(item.author", identity_card)
        self.assertIn("PR#${esc(item.number)}", identity_card)
        self.assertIn("prCardIdentity(item)", work_card)
        self.assertIn("jobStatusIcon(item)", work_card)
        self.assertNotIn("titleHref(item)", work_card)
        self.assertNotIn("workCardDetail(item)", work_card)
        self.assertNotIn("cardLinks(item", work_card)
        self.assertNotIn("work-card-footer", work_card)

    def test_kanban_treats_assigned_prs_as_review_requested(self):
        ui = a2a_runner.load_ui_html()
        marker = "const boardColumnFor ="
        start = ui.index(marker)
        end = ui.index("const cardRelationClass =", start)
        board_logic = ui[start:end]

        self.assertIn("assignees.length > 0", ui)
        self.assertIn("rel.review_requested_from_me || rel.assigned_to_me", board_logic)
        self.assertNotIn('if (rel.assigned_to_me) {\n        if (!isWipTitle(item)', board_logic)

    def test_duplicate_comment_reason_detects_existing_pr_comment(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            task_dir = Path(tmpdir) / "task"
            task_dir.mkdir()
            record = {"repo": "ExampleOrg/project-core", "item_type": "pull_request", "target_index": "460"}
            comment = "_Automated PR review from A2A agent runner._\n\n## Automated PR Review\n\nSame body"

            reason = a2a_runner.duplicate_comment_reason(
                task_dir,
                record=record,
                repo="ExampleOrg/project-core",
                target="460",
                comment_body=comment,
                live_comment_bodies=["  _Automated PR review from A2A agent runner._\n\n## Automated PR Review\nSame body  "],
            )

        self.assertEqual(reason, "duplicate of an existing PR comment")

    def test_duplicate_comment_reason_detects_local_posted_task(self):
        original_iter_records = a2a_runner.iter_records
        with tempfile.TemporaryDirectory() as tmpdir:
            current = Path(tmpdir) / "current"
            previous = Path(tmpdir) / "previous"
            current.mkdir()
            previous.mkdir()
            previous_record = {
                "repo": "ExampleOrg/project-core",
                "item_type": "pull_request",
                "target_index": "460",
                "posted": True,
            }
            review = "## Suggested PR Comment\n\n## Automated PR Review\n\nSame body"
            write_json = a2a_runner.write_json
            write_json(previous / "record.json", previous_record)
            (previous / "review.md").write_text(review, encoding="utf-8")
            a2a_runner.iter_records = lambda: [(previous, previous_record)]
            try:
                reason = a2a_runner.duplicate_comment_reason(
                    current,
                    record={"repo": "ExampleOrg/project-core", "item_type": "pull_request", "target_index": "460"},
                    repo="ExampleOrg/project-core",
                    target="460",
                    comment_body=a2a_runner.build_comment_body(previous_record, review, suggested_only=True, max_chars=12000),
                    live_comment_bodies=[],
                )
            finally:
                a2a_runner.iter_records = original_iter_records

        self.assertEqual(reason, "duplicate of a local posted task package")

    def test_board_issue_row_filters_to_related_issues(self):
        ui = a2a_runner.load_ui_html()

        self.assertIn("const issueHasTrackedPr =", ui)
        self.assertIn("snapshot.has_open_linked_pr", ui)
        self.assertIn("snapshot.tracked_linked_prs", ui)
        self.assertIn("const issueResolvedByMergedPrs =", ui)
        self.assertIn("!issueHasTrackedPr(item, indexes)", ui)
        self.assertIn("!issueResolvedByMergedPrs(item)", ui)
        self.assertIn("const issueItems = boardIssueItems(visible, indexes);", ui)

    def test_human_attention_items_blink_in_dashboard(self):
        ui = a2a_runner.load_ui_html()

        self.assertIn("attention-needed", ui)
        self.assertIn("const attentionReason =", ui)
        self.assertIn('reviewState !== "REQUEST_REVIEW"', ui)
        self.assertIn("AI job failed", ui)
        self.assertIn("Your PR has external feedback", ui)
        self.assertIn("review.human_attention", ui)
        self.assertIn("issueTriageBadge", ui)
        self.assertIn("scan-stale-issues", ui)

    def test_dashboard_ui_loads_from_disk_when_available(self):
        self.assertTrue(a2a_runner.UI_HTML_PATH.exists())
        self.assertIn("A2A Agent Runner", a2a_runner.load_ui_html())

    def test_ngrok_command_targets_webhook_url_with_extra_args(self):
        cmd = a2a_runner.build_ngrok_command(
            "/usr/local/bin/ngrok",
            "http://127.0.0.1:48731",
            ("--domain=example.ngrok-free.app",),
        )

        self.assertEqual(
            cmd,
            [
                "/usr/local/bin/ngrok",
                "http",
                "--domain=example.ngrok-free.app",
                "http://127.0.0.1:48731",
            ],
        )

    def test_parser_has_all_in_one_serve_command(self):
        parser = a2a_runner.build_parser()
        args = parser.parse_args(["serve", "--ngrok", "--ui-port", "48730", "--workers", "2"])

        self.assertIs(args.func, a2a_runner.command_serve)
        self.assertTrue(args.ngrok)
        self.assertEqual(args.ui_port, 48730)
        self.assertEqual(args.workers, 2)
        self.assertFalse(args.no_reclaim_ports)

    def test_parser_can_disable_startup_port_reclaim(self):
        parser = a2a_runner.build_parser()
        args = parser.parse_args(["serve", "--no-reclaim-ports"])

        self.assertTrue(args.no_reclaim_ports)

    def test_lsof_field_output_parser_groups_port_owners(self):
        owners = a2a_runner.parse_lsof_field_output(
            "p17480\ncPython\nn127.0.0.1:48731\np17505\ncngrok\nn127.0.0.1:4040\n"
        )

        self.assertEqual(owners[0]["pid"], "17480")
        self.assertEqual(owners[0]["command"], "Python")
        self.assertEqual(owners[1]["pid"], "17505")
        self.assertEqual(owners[1]["command"], "ngrok")

    def test_startup_port_reclaim_only_allows_runner_or_ngrok_processes(self):
        self.assertTrue(
            a2a_runner.is_reclaimable_serve_owner(
                {
                    "pid": "12345",
                    "command": "Python",
                    "command_line": "/usr/bin/python3 /path/to/workspace-agent-runner/bin/a2a-agent-runner serve --ngrok",
                }
            )
        )
        self.assertFalse(
            a2a_runner.is_reclaimable_serve_owner(
                {"pid": "12346", "command": "Python", "command_line": "/usr/bin/python3 other_server.py"}
            )
        )
        self.assertTrue(
            a2a_runner.is_reclaimable_ngrok_owner(
                {"pid": "12347", "command": "ngrok", "command_line": "/opt/homebrew/bin/ngrok http 127.0.0.1:48731"}
            )
        )

    def test_worker_pool_starts_configured_worker_count(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self.make_config(tmpdir)
            bridge = a2a_runner.WebhookBridge(config, start_worker=True)
            try:
                self.assertEqual(len(bridge._workers), 2)
            finally:
                bridge.shutdown()

    def test_running_jobs_are_recovered_and_rehydrated_on_startup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self.make_config(tmpdir)
            store = a2a_runner.WebhookStateStore(config.state_db)
            job = a2a_runner.WebhookJob(
                kind="pr_review",
                delivery_id="delivery-1",
                event_type="pr_review_active",
                dedupe_key="ExampleOrg/project-core#460@abc123",
                owner="ExampleOrg",
                repo="project-core",
                number=460,
                title="",
                url="https://gitea.example.com/ExampleOrg/project-core/pulls/460",
                head_sha="abc123",
            )
            store.record_delivery(job.delivery_id, job.event_type, job.dedupe_key, "queued", "queued")
            store.reserve_or_retry_job(job, retry_limit=0)
            store.update_job(job.dedupe_key, "running")
            store.update_delivery(job.delivery_id, "running", job.dedupe_key)

            recovered = store.recover_running_jobs()
            pending = store.pending_jobs()

            self.assertEqual(recovered, 1)
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0].dedupe_key, job.dedupe_key)
            self.assertEqual(pending[0].owner, "ExampleOrg")
            self.assertEqual(pending[0].repo, "project-core")

    def test_queue_ignores_new_job_when_same_item_already_active(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bridge = a2a_runner.WebhookBridge(self.make_config(tmpdir), start_worker=False)
            head_job = a2a_runner.WebhookJob(
                kind="pr_review",
                delivery_id="head-1",
                event_type="pr_review_active",
                dedupe_key="ExampleOrg/project-core#460@abc123",
                owner="ExampleOrg",
                repo="project-core",
                number=460,
                title="",
                url="https://gitea.example.com/ExampleOrg/project-core/pulls/460",
                head_sha="abc123",
            )
            comment_job = a2a_runner.WebhookJob(
                kind="pr_review",
                delivery_id="comment-1",
                event_type="pr_comment_active",
                dedupe_key="ExampleOrg/project-core#460@abc123:comment-abcdef",
                owner="ExampleOrg",
                repo="project-core",
                number=460,
                title="",
                url="https://gitea.example.com/ExampleOrg/project-core/pulls/460",
                head_sha="abc123",
            )

            first = bridge._queue_job(head_job, "queued head review")
            second = bridge._queue_job(comment_job, "queued comment review")

            self.assertEqual(first.status_code, 202)
            self.assertEqual(second.status_code, 200)
            self.assertEqual(second.body["status"], "ignored")
            self.assertEqual(second.body["reason"], "active item job")
            self.assertEqual(bridge.store.job_status_for_key(head_job.dedupe_key), "queued")
            self.assertEqual(bridge.store.job_status_for_key(comment_job.dedupe_key), "")
            pending = bridge.store.pending_jobs()

        self.assertEqual([job.dedupe_key for job in pending], [head_job.dedupe_key])

    def test_process_job_skips_when_job_is_no_longer_queued(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bridge = a2a_runner.WebhookBridge(self.make_config(tmpdir), start_worker=False)
            job = a2a_runner.WebhookJob(
                kind="pr_review",
                delivery_id="done-1",
                event_type="pr_review_active",
                dedupe_key="ExampleOrg/project-core#460@abc123",
                owner="ExampleOrg",
                repo="project-core",
                number=460,
                title="",
                url="https://gitea.example.com/ExampleOrg/project-core/pulls/460",
                head_sha="abc123",
            )
            bridge.store.record_delivery(job.delivery_id, job.event_type, job.dedupe_key, "done", "already done")
            bridge.store.reserve_or_retry_job(job, retry_limit=0)
            bridge.store.update_job(job.dedupe_key, "done")
            called = False
            original_run_job = bridge._run_job

            def fake_run_job(_job):
                nonlocal called
                called = True

            bridge._run_job = fake_run_job
            try:
                bridge.process_job(job)
            finally:
                bridge._run_job = original_run_job

        self.assertFalse(called)

    def test_transient_job_error_requeues_instead_of_failing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bridge = a2a_runner.WebhookBridge(self.make_config(tmpdir), start_worker=False)
            job = a2a_runner.WebhookJob(
                kind="pr_review",
                delivery_id="timeout-1",
                event_type="pr_review_active",
                dedupe_key="ExampleOrg/project-core#460@abc123",
                owner="ExampleOrg",
                repo="project-core",
                number=460,
                title="",
                url="https://gitea.example.com/ExampleOrg/project-core/pulls/460",
                head_sha="abc123",
            )
            bridge.store.record_delivery(job.delivery_id, job.event_type, job.dedupe_key, "queued", "queued")
            bridge.store.reserve_or_retry_job(job, retry_limit=0)
            original_run_job = bridge._run_job
            bridge._run_job = lambda _job: (_ for _ in ()).throw(
                a2a_runner.DemoError("failed to fetch PR #460: connect: operation timed out")
            )
            try:
                with self.assertLogs(level="WARNING"):
                    bridge.process_job(job)
            finally:
                bridge._run_job = original_run_job

            self.assertEqual(bridge.store.job_status_for_key(job.dedupe_key), "queued")
            pending = bridge.store.pending_jobs()
            deliveries = bridge.store.recent_deliveries(limit=1)

        self.assertEqual([pending_job.dedupe_key for pending_job in pending], [job.dedupe_key])
        self.assertEqual(deliveries[0]["status"], "queued")
        self.assertIn("transient failure", deliveries[0]["summary"])

    def test_non_transient_job_error_still_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bridge = a2a_runner.WebhookBridge(self.make_config(tmpdir), start_worker=False)
            job = a2a_runner.WebhookJob(
                kind="pr_review",
                delivery_id="logic-1",
                event_type="pr_review_active",
                dedupe_key="ExampleOrg/project-core#460@abc123",
                owner="ExampleOrg",
                repo="project-core",
                number=460,
                title="",
                url="https://gitea.example.com/ExampleOrg/project-core/pulls/460",
                head_sha="abc123",
            )
            bridge.store.record_delivery(job.delivery_id, job.event_type, job.dedupe_key, "queued", "queued")
            bridge.store.reserve_or_retry_job(job, retry_limit=0)
            original_run_job = bridge._run_job
            bridge._run_job = lambda _job: (_ for _ in ()).throw(a2a_runner.DemoError("invalid review format"))
            try:
                with self.assertLogs(level="ERROR"):
                    bridge.process_job(job)
            finally:
                bridge._run_job = original_run_job

            self.assertEqual(bridge.store.job_status_for_key(job.dedupe_key), "failed")
            deliveries = bridge.store.recent_deliveries(limit=1)

        self.assertEqual(deliveries[0]["status"], "failed")

    def test_codex_model_args_include_model_and_reasoning_effort(self):
        self.assertEqual(
            a2a_runner.codex_model_args("gpt-5.5", "mid"),
            ["--model", "gpt-5.5", "-c", 'model_reasoning_effort="medium"'],
        )

    def test_job_model_policy_routes_issue_pr_and_comment_jobs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bridge = a2a_runner.WebhookBridge(self.make_config(tmpdir), start_worker=False)
            issue_job = a2a_runner.WebhookJob(
                kind="issue",
                delivery_id="issue-1",
                event_type="issue_discovery",
                dedupe_key="ExampleOrg/project-core#398",
                owner="ExampleOrg",
                repo="project-core",
                number=398,
                title="",
                url="",
            )
            pr_job = a2a_runner.WebhookJob(
                kind="pr_review",
                delivery_id="pr-1",
                event_type="pr_review_active",
                dedupe_key="ExampleOrg/project-core#460@abc123",
                owner="ExampleOrg",
                repo="project-core",
                number=460,
                title="",
                url="",
                head_sha="abc123",
            )
            comment_job = a2a_runner.WebhookJob(
                kind="pr_review",
                delivery_id="comment-1",
                event_type="pr_comment_active",
                dedupe_key="ExampleOrg/project-core#460@abc123:comment-abcdef",
                owner="ExampleOrg",
                repo="project-core",
                number=460,
                title="",
                url="",
                head_sha="abc123",
            )

            self.assertEqual(bridge._model_policy_for_job(issue_job), ("gpt-5.5", "xhigh"))
            self.assertEqual(bridge._model_policy_for_job(pr_job), ("gpt-5.5", "high"))
            self.assertEqual(bridge._model_policy_for_job(comment_job), ("gpt-5.5", "medium"))

    def test_comment_triggered_pr_review_job_does_not_auto_post(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = replace(self.make_config(tmpdir), pr_auto_post=True)
            bridge = a2a_runner.WebhookBridge(config, start_worker=False)
            job = a2a_runner.WebhookJob(
                kind="pr_review",
                delivery_id="comment-1",
                event_type="pr_comment_active",
                dedupe_key="ExampleOrg/project-core#460@abc123:comment-abcdef",
                owner="ExampleOrg",
                repo="project-core",
                number=460,
                title="",
                url="https://gitea.example.com/ExampleOrg/project-core/pulls/460",
                head_sha="abc123",
            )

            captured = {}
            original_command_pr_review = a2a_runner.command_pr_review
            a2a_runner.command_pr_review = lambda args: captured.setdefault("post", args.post) or 0
            try:
                bridge._run_job(job)
            finally:
                a2a_runner.command_pr_review = original_command_pr_review

        self.assertFalse(captured["post"])

    def test_duplicate_comment_skip_marks_job_done_and_delivery_ignored(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self.make_config(tmpdir)
            bridge = a2a_runner.WebhookBridge(config, start_worker=False)
            job = a2a_runner.WebhookJob(
                kind="pr_review",
                delivery_id="duplicate-1",
                event_type="pr_review_active",
                dedupe_key="ExampleOrg/project-core#460@abc123",
                owner="ExampleOrg",
                repo="project-core",
                number=460,
                title="",
                url="https://gitea.example.com/ExampleOrg/project-core/pulls/460",
                head_sha="abc123",
            )
            bridge.store.record_delivery(job.delivery_id, job.event_type, job.dedupe_key, "queued", "queued")
            bridge.store.reserve_or_retry_job(job, retry_limit=0)
            original_run_job = bridge._run_job
            bridge._run_job = lambda _job: (_ for _ in ()).throw(a2a_runner.DuplicateCommentSkipped("duplicate"))
            try:
                bridge.process_job(job)
            finally:
                bridge._run_job = original_run_job

            self.assertEqual(bridge.store.job_status_for_key(job.dedupe_key), "done")
            deliveries = bridge.store.recent_deliveries(limit=1)

        self.assertEqual(deliveries[0]["status"], "ignored")
        self.assertEqual(deliveries[0]["summary"], "duplicate")

    def test_address_in_use_message_explains_duplicate_runner(self):
        message = a2a_runner.address_in_use_message("webhook listener", "127.0.0.1", 48731)

        self.assertIn("127.0.0.1:48731", message)
        self.assertIn("Another A2A runner", message)
        self.assertIn("--webhook-port", message)

    def test_reusable_http_server_releases_ports_promptly(self):
        self.assertTrue(a2a_runner.ReusableThreadingHTTPServer.allow_reuse_address)
        self.assertTrue(a2a_runner.ReusableThreadingHTTPServer.daemon_threads)

    def test_bridge_shutdown_tolerates_second_keyboard_interrupt(self):
        class InterruptingThread:
            name = "interrupting-thread"

            def join(self, timeout=None):
                raise KeyboardInterrupt

        bridge = a2a_runner.WebhookBridge.__new__(a2a_runner.WebhookBridge)
        bridge._stop = threading.Event()
        bridge._worker = InterruptingThread()
        bridge._poller = None
        bridge._monitor = None

        with self.assertLogs(level="WARNING") as logs:
            bridge.shutdown()

        self.assertTrue(bridge._stop.is_set())
        self.assertIn("shutdown interrupted", "\n".join(logs.output))


if __name__ == "__main__":
    unittest.main()
