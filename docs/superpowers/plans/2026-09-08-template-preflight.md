# Template Preflight Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox syntax for tracking.

**Goal:** Select local text-and-JPEG templates during preflight and deliver the exact approved payload.

**Architecture:** Add template loading to existing input validation and pass its immutable contents through preflight, Workflow and RecipientPipeline. Keep existing result storage and delivery state machine.

**Tech Stack:** Python 3.14, stdlib, existing unittest suite, no new dependencies.

**Spec:** docs/superpowers/specs/2026-09-08-template-preflight-design.md

## Global Constraints

- No actual MMS sending, upload or live SENS lookup. Never inspect the project .env or real recipient data. Synthetic test inputs only.
- Keep results/result.csv, its exact existing columns, snapshots, event logs and existing delivery safety. No history reset or campaign manager.
- worker_count=5, poll_interval_seconds=1, confirmation_timeout_seconds=120, retry_delay_seconds=10, max_attempts=3, rate_limit_delays_seconds=[10,20].
- One MMS with exact selected message.txt content, no API subject. One or two ordered JPEGs; bytes <=300*1024, width <=1500, height <=1440. UTF-8 input, EUC-KR-supported content <=2000 bytes.
- No post-validation file reopen; approval binds template and all content. No production fixed-message fallback.

### Task 1: Template selection through actual send payload

**Ownership:** Implementer owns sens_mms/inputs.py, preflight.py, cli.py, workflow.py, pipeline.py, api.py and tests/*.py. Controller owns AGENTS.md, README.md, .gitignore, design/plan documents and local starter input preparation. You are not alone; do not revert others' edits. Do not spawn subagents.

**Requirements:** Read the spec above first; it is the binding exact behavior. The previous user approval supersedes hardcoded body/image rules in the existing AGENTS.md. All other safety rules remain.

**Interfaces:** Add required `template_name` keyword to build_preflight and Workflow, propagate it on every rebuild, and use a frozen loaded-template value from inputs.py. Add `stdin`/`stderr` injection to CLI for deterministic menu tests. Require explicit send content in API and pipeline; pass report.content and no subject through actual calls. Retain existing PreflightReport.body/content fields as the same complete selected text, subject=None, plus template_name. Exact names of the input-loader value are implementer-local, no external consumer depends on them.

- [x] Write failing behavioral tests before implementation. A representative integration check is:

```python
first = build_preflight(root, config, store, template_name="notice")
assert first.body == (root / "input" / "notice" / "message.txt").read_bytes().decode("utf-8")
assert first.subject is None
(root / "input" / "notice" / "message.txt").write_bytes(b"changed\r\n")
second = build_preflight(root, config, store, template_name="notice")
assert first.approval_token != second.approval_token
```

Also test terminal selection maps the number to the sorted folder, flag selection skips stdin, noninteractive/no-selection/live-no-template stops without API calls; malformed templates fail safely; exact UTF-8/CRLF content reaches a scripted HTTP payload; template/image/body changes invalidate old tokens; post-validation mutation cannot change upload/send bytes; same-size different JPEGs get different upload names; SENT and ambiguous pending still never repost. Reuse existing synthetic fixture builders and scripted transports. Migrate hardcoded-message equality tests into selected-file equality tests, not just removal. Suppress synthetic per-send terminal progress in full-suite test execution if necessary, without changing product logging.

- [x] Run focused tests and record expected RED evidence. Baseline is `python -m unittest discover -q`, 309 passing tests at ecf9a9d.
- [x] Implement the shortest integrated change. Reuse ImageInfo.data, approval token hashing, ResultStore, pipeline and existing urllib client. A minimal API handoff is:

```python
self._pipeline = RecipientPipeline(self.api, self.coordinator, report.content_type, content=report.content)
# Inside RecipientPipeline's existing send operation:
response = self.api.send_one(current.receiving_number, file_ids,
                             content_type=self.content_type, content=self.content)
```

Do not add a template engine, config manifest, dependency or automatic image conversion. Images keep original local metadata while upload filename is a truncated cryptographic SHA-256 hex digest plus `.jpg` within 40 chars. Implement file containment checks before reading. Unknown errors stay fixed safe BLOCKED; known local template errors can be fixed actionable text. No dynamic raw exception details.

- [x] Run focused tests then `python -m unittest discover -q`; capture exit codes and summary. Update all affected fixture call sites without weakening existing safety assertions. Read your diff for remaining production MESSAGE constants/call defaults.
- [x] Commit only your owned implementation/test files with `feat: select MMS templates during preflight`. Write report including RED/GREEN evidence, files and concerns to `.superpowers/sdd/2026-09-08-template-preflight/task-1-report.md`.

### Task 2: Documentation and local input setup (controller)

- [x] Update AGENTS.md and README to the approved folder selection contract, supported image count, exact-file validation, no subject and same shared history. Preserve unrelated safety constraints and existing metadata-test requirements where valid.
- [x] Ignore personal input data while retaining input/README.md with folder examples; copy old source body and existing local JPEG into an ignored starter folder without overwrite. No credential/recipient read.
- [x] Review Task 1 via independent task reviewer using brief/report/diff, resolve findings, run final changed-code checks and independent Codex whole-change review.
- [x] Apply reviewed changes to the original project workspace so the user's ordinary commands use them. Preserve all existing local inputs/results/logs. No live API or automatic commit/push on main.
