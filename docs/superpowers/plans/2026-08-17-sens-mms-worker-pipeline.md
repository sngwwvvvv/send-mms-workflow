# SENS Integrated MMS Worker Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the existing serial, one-recipient combined-MMS workflow into a crash-safe fixed five-worker pipeline while preserving the exact eight-column result model, one MMS POST per recipient, pending-first reconciliation, privacy controls, and no-uncertain-resend guarantees.

**Architecture:** Keep `results/result.csv` as the single durable eight-column checkpoint and move shared concurrent writes, event sequencing, global stop, and HTTP 429 gating into `sens_mms/coordination.py`. Move one recipient's POST, correlation, polling, ambiguity, and retry state machine into `sens_mms/pipeline.py`; keep `Workflow` responsible for approval revalidation, pending-first phase barriers, conditional immutable image upload, atomic batch reservation, fixed five-worker execution, and immutable completion snapshots.

**Tech Stack:** Python 3.14 standard library (`argparse`, `concurrent.futures`, `csv`, `dataclasses`, `datetime`, `hashlib`, `json`, `os`, `pathlib`, `secrets`, `tempfile`, `threading`, `time`, `typing`, `unittest`, `urllib`, `zoneinfo`).

## Global Constraints

- Implement only in `C:\workflows\send-mms-workflow-lms-then-mms` on `experiment/lms-then-mms-worker-pipeline` until Task 9 performs the approved `--no-ff` merge. Do not edit implementation files in the main worktree first.
- Do not perform a live SENS request. Implementation, tests, review, commits, and the main merge are not live-send approval.
- Do not read, print, copy, link, or share the main worktree's `.env`, actual `receiving_numbers.csv`, `mms_img`, `results`, or `logs`. Tests use fake credentials, generated JPEG bytes, non-real numbers, and mock transports only.
- Re-check the official SENS [send](https://api.ncloud-docs.com/docs/sens-sms-send), [list](https://api.ncloud-docs.com/docs/sens-sms-list), [get](https://api.ncloud-docs.com/docs/sens-sms-get), [attachment](https://api.ncloud-docs.com/docs/sens-sms-attachment-create), and [overview](https://api.ncloud-docs.com/docs/sens-overview) contracts before implementation and again before any future live execution. The 2026-08-17 check found no conflict: MMS supports one-recipient `messages`, ordered `files`, `COMM|AD`, 300KB JPEGs up to 1500x1440, accepted `202`, list `202`, get `200`, and `READY|PROCESSING|COMPLETED`.
- Preserve `MESSAGE_BODY` in `sens_mms/inputs.py` byte-for-byte as the approved UTF-8 body. Every recipient request is one `type="MMS"` POST with that body, no `subject`, one `messages=[{"to": ...}]`, and the two file IDs in `mms_01_intro.jpg`, `mms_02_details.jpg` order.
- Result columns remain exactly `receiving_number,delivery_id,delivery_status,is_sent,attempts,request_id,message_id,error`. Do not add LMS, pipeline, stage, or 14-column migration fields.
- A new or separately approved failed recipient is atomically reserved as `PENDING_CONFIRMATION`, `attempts=0`, non-empty `delivery_id`, blank request/message IDs, and `error=null` before any worker starts.
- Only a newly approved `attempts=0` blank-ID reservation may POST with the existing `delivery_id`. A positive-attempt blank-ID pending row is ambiguous and must never POST automatically.
- Use exactly five workers. This value is not a CLI or constructor option. A worker owns one recipient until `SENT`, `FAILED`, or `PENDING_CONFIRMATION`.
- Poll immediately and then every one second. Each accepted POST or restart reconciliation gets a 120-second monotonic confirmation window. Global 429 waits consume that same window.
- Allow at most three actual message POST starts per recipient in an approved execution. Retry only explicit non-acceptance or correlated `COMPLETED + fail`, after ten seconds. Never retry an ambiguous POST.
- The first HTTP 429 in a run gates every later upload/send/list/get call for ten seconds; every subsequent 429 gates them for a fixed twenty seconds. Merge overlapping windows with `max(current_deadline, now + delay)` and never add the message retry delay to the rate-limit delay.
- Read each JPEG once during final preflight. Metadata, SHA-256, approval token, and upload Base64 must all derive from that immutable byte copy. Upload each image at most once per approved run and only if a new message POST will occur.
- Serialize the full read-validate-write result transaction and every JSONL sequence/write. A checkpoint or log failure stops all later API calls and suppresses the success snapshot and `WORKFLOW_COMPLETED`.
- Event logs use `delivery_id`, never a full recipient. Never log sender or recipient numbers, names, body, subject, file IDs, service ID, credentials, signatures, auth headers, or raw request/response bodies.
- Preserve total API-value sanitizers: inspect exact built-in types only; never invoke caller-controlled `__str__`, equality, truthiness, length, iteration, or related methods. Preserve only approved status values, correlation IDs, times, pagination types, `SKT|ETC`, and fixed `redacted` human messages.
- Naive datetime values at log, snapshot, and workflow boundaries mean Asia/Seoul wall time; aware values convert to Asia/Seoul. Never infer the host-local timezone.
- Hold the canonical process-local plus OS live-execution lock for the full run before `WORKFLOW_STARTED` and before any API call. Do not automatically delete results, snapshots, logs, the experiment branch, or the worktree.

---

## Planned File Structure

**Create:**

- `sens_mms/coordination.py` — fixed run settings, thread-safe result and event serialization, fail-closed run stop, and shared 429 gate.
- `sens_mms/pipeline.py` — approved per-recipient work types and the combined-MMS POST/correlation/poll/retry state machine.
- `tests/test_coordination.py` — deterministic concurrency, batch checkpoint, event sequence, stop, and global rate-gate tests.
- `tests/test_pipeline.py` — per-recipient crash boundaries, timing, retry, ambiguity, correlation, and privacy-safe event tests.
- `docs/operations/2026-08-17-sens-mms-worker-experiment-worktree-policy.md` — active experiment and promotion policy for the integrated MMS design.

**Modify:**

- `AGENTS.md` — replace the obsolete 14-column LMS→MMS rules with the active eight-column integrated MMS contract.
- `docs/operations/2026-08-16-lms-then-mms-experiment-worktree-policy.md` — mark it superseded without deleting history.
- `docs/superpowers/specs/2026-08-16-sens-lms-then-mms-worker-pipeline-design.md` and `docs/superpowers/plans/2026-08-16-sens-lms-then-mms-worker-pipeline.md` — mark both superseded by the 2026-08-17 integrated MMS documents.
- `sens_mms/api.py` — content-type-aware integrated MMS POST, typed message type, exact correlation filters, and safe HTTP metadata for 429 classification.
- `sens_mms/event_log.py` — keep the eight-column state allowlist and prove thread serialization occurs only through the coordinator.
- `sens_mms/preflight.py` — immutable categorized work items, fixed settings, public counts, and approval-token binding.
- `sens_mms/results.py` — add coordinator-facing atomic batch publication without changing `COLUMNS` or historical seven-to-eight-column migration.
- `sens_mms/workflow.py` — pending-first barrier, conditional upload, all-or-nothing reservation, two fixed worker phases, and safe completion.
- `sens_mms_cli.py` — wire the revised workflow without exposing worker/timing knobs and retain deidentified output.
- `README.md` — document integrated MMS worker behavior, pending categories, and the separate live approval boundary.
- `tests/test_api.py`, `tests/test_event_log.py`, `tests/test_preflight.py`, `tests/test_results.py`, `tests/test_workflow.py`, and `tests/test_cli.py` — focused and end-to-end regressions.

---

### Task 1: Replace the Obsolete LMS→MMS Governance Contract

**Files:**

- Modify: `AGENTS.md`
- Create: `docs/operations/2026-08-17-sens-mms-worker-experiment-worktree-policy.md`
- Modify: `docs/operations/2026-08-16-lms-then-mms-experiment-worktree-policy.md`
- Modify: `docs/superpowers/specs/2026-08-16-sens-lms-then-mms-worker-pipeline-design.md`
- Modify: `docs/superpowers/plans/2026-08-16-sens-lms-then-mms-worker-pipeline.md`

**Interfaces:**

- Produces the active repository-wide policy used by every later task.
- Preserves the old design, plan, branch, and worktree as decision history; it deletes none of them.
- Declares `docs/superpowers/specs/2026-08-17-sens-mms-worker-pipeline-design.md` and this plan as the active design and execution contract.

- [ ] **Step 1: Record the current policy mismatch before changing files**

Run:

```powershell
rg -n "LMS_THEN_MMS|LEGACY_COMBINED_MMS|lms_status|mms_status|14열" AGENTS.md docs/operations/2026-08-16-lms-then-mms-experiment-worktree-policy.md
```

Expected: matches show the obsolete 14-column and two-stage policy is still active in both files.

- [ ] **Step 2: Replace `AGENTS.md` with the exact integrated-MMS policy**

Use the eight-column result/status/log text from the main worktree policy as the base, then apply the spec's fixed execution values verbatim:

```text
worker_count=5
poll_interval_seconds=1
confirmation_timeout_seconds=120
retry_delay_seconds=10
max_attempts=3
rate_limit_delays_seconds=[10,20]
```

The active policy must explicitly include the `attempts=0` same-delivery-ID resume exception and prohibit positive-attempt blank-ID reposts. Replace every LMS-stage instruction with one `MMS` request containing the approved body and both ordered images.

- [ ] **Step 3: Publish the active experiment policy and mark history superseded**

The new operations document must contain this exact ownership table:

```markdown
| 구분 | 브랜치 | 작업 경로 | 책임 |
| --- | --- | --- | --- |
| 기존 운영 | `main` | `C:\workflows\send-mms-workflow` | 현재 통합 MMS 운영 코드와 운영 데이터 |
| worker 구현 | `experiment/lms-then-mms-worker-pipeline` | `C:\workflows\send-mms-workflow-lms-then-mms` | 8열 통합 MMS worker 구현·모의 테스트·승격 준비 |
```

Add `상태: 대체됨` and a direct pointer to the 2026-08-17 replacement at the top of each 2026-08-16 policy/spec/plan. Do not rewrite or remove their historical body.

- [ ] **Step 4: Verify there is one active contract**

Run:

```powershell
rg -n "receiving_number,delivery_id,delivery_status,is_sent,attempts,request_id,message_id,error" AGENTS.md docs/operations/2026-08-17-sens-mms-worker-experiment-worktree-policy.md
rg -n "LMS_THEN_MMS|LEGACY_COMBINED_MMS|lms_status|mms_status" AGENTS.md docs/operations/2026-08-17-sens-mms-worker-experiment-worktree-policy.md
```

Expected: the first command matches both active files; the second command returns no matches.

- [ ] **Step 5: Commit the policy transition**

```powershell
git add AGENTS.md docs/operations docs/superpowers/specs/2026-08-16-sens-lms-then-mms-worker-pipeline-design.md docs/superpowers/plans/2026-08-16-sens-lms-then-mms-worker-pipeline.md
git commit -m "docs: adopt integrated MMS worker policy"
```

---

### Task 2: Strengthen the Integrated MMS API Contract

**Files:**

- Modify: `sens_mms/api.py`
- Modify: `tests/test_api.py`
- Modify: `tests/test_event_log.py`

**Interfaces:**

- Changes `SensClient.send_one(to: str, file_ids: Sequence[str], *, content_type: str) -> SendResponse`.
- Changes `SensClient.list_by_time_and_recipient(start: str, end: str, to: str, *, message_type: str = "MMS") -> MessageListResponse`.
- Adds `MessageRecord.message_type: str` while retaining existing exact-string correlation fields.
- Adds `TransientLookupError(message: str, *, http_status: int | None = None)` and retains `ExplicitApiFailure.http_status` as an exact integer or `None`.
- Preserves `upload_bytes(file_name: str, raw: bytes) -> str`, `list_by_request(request_id: str)`, and `get_message(message_id: str)`.

- [ ] **Step 1: Write RED tests for the exact request and typed lookup contract**

```python
def test_send_one_builds_exact_approved_integrated_mms(self):
    transport = RecordingTransport([response(202, {
        "requestId": "request-1",
        "requestTime": "2026-08-17T10:00:00.000",
        "statusCode": "202",
        "statusName": "success",
    })])
    client(transport).send_one(
        "01000000000", ("file-1", "file-2"), content_type="COMM"
    )
    self.assertEqual(json.loads(transport.calls[0][3]), {
        "type": "MMS",
        "contentType": "COMM",
        "countryCode": "82",
        "from": "01011112222",
        "content": MESSAGE_BODY,
        "messages": [{"to": "01000000000"}],
        "files": [{"fileId": "file-1"}, {"fileId": "file-2"}],
    })
    self.assertNotIn("subject", json.loads(transport.calls[0][3]))
```

Add tests that reject non-exact or non-allowlisted content types before transport, include `type=MMS` in time/recipient lookup, retain exact message `type`, and turn GET 429 into `TransientLookupError(http_status=429)` without retaining a response body or API message.

- [ ] **Step 2: Run the focused API tests and confirm RED**

Run: `python -m unittest tests.test_api.ApiTests -v`

Expected: FAIL because `send_one` has no keyword content type, `MessageRecord` has no message type, time lookup omits `type`, and GET HTTP errors are classified as explicit send/upload failures.

- [ ] **Step 3: Implement exact built-in validation and safe HTTP classification**

Use these signatures and validation boundaries:

```python
class TransientLookupError(SensApiError):
    def __init__(self, message: str, *, http_status: int | None = None):
        self.http_status = http_status if type(http_status) is int else None
        super().__init__(message)

def send_one(self, to, file_ids, *, content_type):
    if type(content_type) is not str or content_type not in {"COMM", "AD"}:
        raise ExplicitApiFailure("INVALID_REQUEST", "content type is invalid")
    return self._send_message({
        "type": "MMS",
        "contentType": content_type,
        "countryCode": "82",
        "from": self._from_number,
        "content": MESSAGE_BODY,
        "messages": [{"to": to}],
        "files": [{"fileId": file_id} for file_id in file_ids],
    })
```

Extract the current accepted-response validation into `_send_message(payload)`. For a non-2xx GET, raise `TransientLookupError("SENS lookup was not successful", http_status=response.status)`; for message POST keep `ExplicitApiFailure(http_status=...)`; for upload keep an explicit upload failure. Store no raw error payload.

- [ ] **Step 4: Add message-type correlation and run API plus sanitizer regressions**

Populate the new field only through `_exact_string(data.get("type"))`, and add `type=MMS` to time recovery:

```python
query = urlencode({
    "requestStartTime": request_start_time,
    "requestEndTime": request_end_time,
    "to": to,
    "type": message_type,
})
```

Run: `python -m unittest tests.test_api.ApiTests tests.test_event_log.EventLogTests -v`

Expected: PASS; all adversarial exact-type tests remain green and the serializers still omit recipient and message body fields.

- [ ] **Step 5: Commit the API boundary**

```powershell
git add sens_mms/api.py tests/test_api.py tests/test_event_log.py
git commit -m "feat: strengthen integrated MMS API contract"
```

---

### Task 3: Add Thread-Safe Durable Coordination and the Global 429 Gate

**Files:**

- Create: `sens_mms/coordination.py`
- Create: `tests/test_coordination.py`
- Modify: `sens_mms/results.py`
- Modify: `tests/test_results.py`
- Modify: `sens_mms/event_log.py`
- Modify: `tests/test_event_log.py`

**Interfaces:**

- Produces immutable `RunSettings` and module constant `RUN_SETTINGS` with exact values `5, 1, 120, 10, 3, (10, 20)`.
- Moves the structural `Clock` protocol from `workflow.py` to `coordination.py`.
- Produces `RunCoordinator.read_rows() -> dict[str, ResultRow]`.
- Produces `RunCoordinator.commit(row: ResultRow, *, expected: ResultRow | None = None) -> None`.
- Produces `RunCoordinator.commit_batch(expected: Mapping[str, ResultRow | None], replacements: Sequence[ResultRow]) -> None`.
- Produces `RunCoordinator.write(event: str, **fields) -> None`, `raise_if_stopped()`, `stop()`, `before_api_call()`, `record_429()`, and `wait_until(deadline: float)`.
- Adds `ResultStore.replace_rows_atomic(expected, replacements) -> None`, called only while the coordinator's checkpoint lock is held.

- [ ] **Step 1: Write RED settings, serialization, and batch-publication tests**

```python
def test_fixed_settings_are_exact_and_not_runtime_options(self):
    self.assertEqual(RUN_SETTINGS, RunSettings(
        worker_count=5,
        poll_interval_seconds=1,
        confirmation_timeout_seconds=120,
        retry_delay_seconds=10,
        max_attempts=3,
        rate_limit_delays_seconds=(10, 20),
    ))

def test_batch_commit_is_all_or_nothing_against_fresh_rows(self):
    coordinator.commit_batch(
        {FIRST: None, SECOND: None},
        (reservation(FIRST, ID_1), reservation(SECOND, ID_2)),
    )
    self.assertEqual(set(ResultStore(store.path).load()), {FIRST, SECOND})
    with self.assertRaises(ResultFormatError):
        coordinator.commit_batch(
            {FIRST: reservation(FIRST, ID_3)},
            (reservation(FIRST, ID_4),),
        )
    self.assertEqual(ResultStore(store.path).load()[FIRST].delivery_id, ID_1)
```

Use a five-thread barrier test to commit five distinct rows and write five events. Assert no row is lost and the real event log sequences are exactly `[1, 2, 3, 4, 5]`.

- [ ] **Step 2: Write RED fail-closed and deterministic rate-gate tests**

```python
def test_first_429_waits_ten_and_later_429s_wait_twenty(self):
    clock = LockedManualClock()
    coordinator = make_coordinator(clock)
    coordinator.record_429()
    coordinator.before_api_call()
    coordinator.record_429()
    coordinator.before_api_call()
    coordinator.record_429()
    coordinator.before_api_call()
    self.assertEqual(clock.sleeps, [10, 20, 20])
```

Add a barrier test proving five callers share one active wait window rather than sleeping five times. Add checkpoint and event failure tests proving the original safe exception is re-raised, `raise_if_stopped()` then raises fixed `RunSafetyError("workflow safety stop is active")`, and no later commit, event, upload, send, list, or get is allowed.

- [ ] **Step 3: Implement the fixed settings and coordinator locks**

```python
@dataclass(frozen=True)
class RunSettings:
    worker_count: int
    poll_interval_seconds: float
    confirmation_timeout_seconds: float
    retry_delay_seconds: float
    max_attempts: int
    rate_limit_delays_seconds: tuple[float, float]

RUN_SETTINGS = RunSettings(5, 1, 120, 10, 3, (10, 20))

class RunCoordinator:
    def __init__(self, store, event_log, clock):
        self.store = store
        self.event_log = event_log
        self.clock = clock
        self._checkpoint_lock = threading.Lock()
        self._event_lock = threading.Lock()
        self._rate_lock = threading.Lock()
        self._stopped = threading.Event()
        self._blocked_until = 0.0
        self._rate_limit_count = 0
```

`commit` and `commit_batch` must load the current file inside `_checkpoint_lock`, compare exact expected rows, validate every replacement, and perform one `write_atomic()`. `write` must hold `_event_lock` across the whole `JsonlEventLog.write`. Any durable exception sets `_stopped` before re-raising.

- [ ] **Step 4: Implement max-deadline rate gating and verify the coordinator**

`record_429()` increments the run count under `_rate_lock`, chooses ten for count one and twenty thereafter, then executes:

```python
self._blocked_until = max(
    self._blocked_until,
    self.clock.monotonic() + delay,
)
```

`before_api_call()` checks safety, holds `_rate_lock`, sleeps only the positive remaining duration, releases the lock, then checks safety again. `wait_until()` sleeps only `max(0, deadline - monotonic())`; callers invoke `before_api_call()` afterward so retry and rate windows converge on the later deadline instead of being blindly added.

Run: `python -m unittest tests.test_coordination tests.test_results tests.test_event_log -v`

Expected: PASS with exact fixed settings, atomic batches, deterministic event sequence, 10/20/20 gate behavior, and durable fail-closed state.

- [ ] **Step 5: Commit shared coordination**

```powershell
git add sens_mms/coordination.py sens_mms/results.py sens_mms/event_log.py tests/test_coordination.py tests/test_results.py tests/test_event_log.py
git commit -m "feat: coordinate concurrent MMS workers"
```

---

### Task 4: Bind Restart Categories and Fixed Settings into Preflight Approval

**Files:**

- Modify: `sens_mms/preflight.py`
- Modify: `tests/test_preflight.py`

**Interfaces:**

- Produces `WorkAction = Literal["RECONCILE", "RESUME_RESERVATION", "START_FRESH", "HOLD_AMBIGUOUS", "RETRY_EXPLICIT"]`.
- Produces immutable `ApprovedWork(receiving_number, action, allow_retry_after_explicit_failure, source_delivery_id)` with recipient and delivery ID hidden from `repr`.
- Adds `PreflightReport.work_items: tuple[ApprovedWork, ...]` and `PreflightReport.content_type: str`.
- Preserves `build_preflight(root, config, result_store, *, resend_failed=False) -> PreflightReport` and the immutable `approved_images` byte copies.
- Consumes `RUN_SETTINGS` from Task 3 and exact eight-column `result_row_identity`.

- [ ] **Step 1: Write RED classification tests for every legal current row**

```python
def test_preflight_classifies_pending_without_weakening_no_resend(self):
    rows = (
        pending_with_ids(FIRST, attempts=1),
        pending_reservation(SECOND),
        pending_blank_ids(THIRD, attempts=1),
    )
    seed_current(root, rows)
    report = build_preflight(root, config(), ResultStore.for_root(root))
    self.assertEqual(
        tuple((work.receiving_number, work.action,
               work.allow_retry_after_explicit_failure)
              for work in report.work_items),
        (
            (FIRST, "RECONCILE", True),
            (SECOND, "RESUME_RESERVATION", False),
            (THIRD, "HOLD_AMBIGUOUS", False),
        ),
    )
```

Cover new input as `START_FRESH`, current `SENT` exclusion, ordinary `FAILED` exclusion, separately approved snapshot-matching `FAILED` as `START_FRESH`, validation failure exclusion, pending with only request ID as `RECONCILE`, and pending with message ID but no request ID as an invalid result row.

- [ ] **Step 2: Write RED public-report and token-binding tests**

Assert the public report contains only counts and masked recipient samples for pending lookup, safe reservation resume, ambiguous hold, and fresh/resend start. It must expose the registered sender number for operator confirmation, the exact body, `MMS`, no subject, content type, image name/bytes/size/dimensions/hash/order, and this exact settings object:

```python
{
    "worker_count": 5,
    "poll_interval_seconds": 1,
    "confirmation_timeout_seconds": 120,
    "retry_delay_seconds": 10,
    "max_attempts": 3,
    "rate_limit_delays_seconds": [10, 20],
}
```

Assert it never emits full recipient/work-item numbers, `delivery_id`, `request_id`, or `message_id`. The registered sender and exact body are the deliberate preflight-only exceptions required for approval. Mutation tests must change the token for every `ResultRow` field, action, retry permission, source delivery ID, immutable image byte/hash, body, content type, target order, resend snapshot name/hash, or run setting.

- [ ] **Step 3: Implement immutable approved work and exact classification**

```python
@dataclass(frozen=True)
class ApprovedWork:
    receiving_number: str = field(repr=False)
    action: WorkAction
    allow_retry_after_explicit_failure: bool
    source_delivery_id: str = field(default="", repr=False)

def classify_existing(row: ResultRow) -> ApprovedWork:
    if row.delivery_status != "PENDING_CONFIRMATION":
        raise ResultFormatError("result row is not pending")
    if row.attempts == 0:
        return ApprovedWork(row.receiving_number, "RESUME_RESERVATION", False,
                            row.delivery_id)
    if not row.request_id and not row.message_id:
        return ApprovedWork(row.receiving_number, "HOLD_AMBIGUOUS", False,
                            row.delivery_id)
    return ApprovedWork(row.receiving_number, "RECONCILE", row.attempts < 3,
                        row.delivery_id)
```

Keep content type as the exact currently approved project value `COMM` in the report and token. Do not infer advertising classification at runtime; future live preflight still requires the operator to confirm it, and a different approved classification must create a new token and API value.

- [ ] **Step 4: Build canonical JSON from private identities and run preflight regressions**

Canonicalize work items explicitly rather than serializing dataclass repr:

```python
"workItems": [
    {
        "receivingNumber": work.receiving_number,
        "action": work.action,
        "allowRetryAfterExplicitFailure": work.allow_retry_after_explicit_failure,
        "sourceDeliveryId": work.source_delivery_id,
    }
    for work in work_items
],
"settings": asdict(RUN_SETTINGS),
```

Run: `python -m unittest tests.test_preflight.PreflightTests -v`

Expected: PASS with all restart categories, no public identifiers, single-read image bytes, and complete approval-token invalidation.

- [ ] **Step 5: Commit approval work planning**

```powershell
git add sens_mms/preflight.py tests/test_preflight.py
git commit -m "feat: bind MMS worker actions into approval"
```

---

### Task 5: Implement the Per-Recipient Combined-MMS State Machine

**Files:**

- Create: `sens_mms/pipeline.py`
- Create: `tests/test_pipeline.py`

**Interfaces:**

- Consumes `ApprovedWork`, `RunCoordinator`, `RUN_SETTINGS`, `ResultRow`, `SensClient`, and the existing safe typed response serializers.
- Produces immutable `PipelineResult(row: ResultRow, needs_post: bool = False, retry_not_before: float | None = None)`.
- Produces `RecipientPipeline.run(row: ResultRow, work: ApprovedWork, file_ids: Sequence[str] = ()) -> PipelineResult`.
- Keeps POST, time-based ambiguous recovery, request-list correlation, result polling, retry, and final row transitions private to `RecipientPipeline`.

- [ ] **Step 1: Write RED happy-path and crash-boundary tests**

```python
def test_accepted_processing_success_checkpoints_every_boundary(self):
    api = ScriptedPipelineApi(
        sends=(send_response("request-1"),),
        lists=(list_response(message("PROCESSING")),),
        gets=(
            result_response(message("PROCESSING")),
            result_response(message("COMPLETED", "success")),
        ),
    )
    result = make_pipeline(api).run(
        reservation(), resume_work(), ("file-1", "file-2")
    )
    self.assertEqual(result.row.delivery_status, "SENT")
    self.assertEqual(api.calls, ["send", "list", "get", "get"])
    self.assertEqual(clock.sleeps, [1])
    self.assertEqual(
        state_boundaries(events),
        [
            ("PENDING_CONFIRMATION", 1, "", ""),
            ("PENDING_CONFIRMATION", 1, "request-1", ""),
            ("PENDING_CONFIRMATION", 1, "request-1", "message-1"),
            ("SENT", 1, "request-1", "message-1"),
        ],
    )
```

Inject a checkpoint or event failure immediately after reservation, attempt increment, request ID, message ID, and final state. Assert the coordinator stops, no later API call begins, and the last durable row never implies an unrecorded safe resend.

- [ ] **Step 2: Write RED failure, ambiguity, correlation, and timing tests**

Cover all of these as separate deterministic tests:

- first explicit POST failure then success after exactly ten seconds;
- three explicit failures become `FAILED` with the final sanitized error;
- correlated `COMPLETED + fail` returns `needs_post=True` only when approval permits and attempts remain;
- restart reconciliation with attempts three plus `COMPLETED + fail` becomes `FAILED` without POST;
- POST timeout, disconnect, unreadable/malformed 2xx, invalid `202` request ID, and positive-attempt blank IDs remain pending with no repost;
- an ambiguous POST performs at most the approved time/recipient/`MMS` lookup and adopts only one exact recipient/type match;
- list correlation requires exact request ID, recipient, type `MMS`, and exactly one non-empty exact-string message ID;
- get correlation requires exact stored request ID, message ID, and recipient;
- transient list/get errors and GET 429 are logged and retried every one second inside the same 120-second window;
- every repeated `READY`/`PROCESSING` response is logged;
- a gate wait that exhausts the 120-second deadline returns pending without a late GET;
- retry delay and rate gate finish at the later deadline, not their sum.

- [ ] **Step 3: Implement action dispatch and POST safety ordering**

```python
class RecipientPipeline:
    def run(self, row, work, file_ids=()):
        if work.action == "HOLD_AMBIGUOUS":
            return PipelineResult(row)
        if work.action == "RECONCILE":
            return self._reconcile(row, work)
        if work.action not in {
            "RESUME_RESERVATION", "START_FRESH", "RETRY_EXPLICIT"
        }:
            raise ResultFormatError("approved work action invalid")
        return self._send(row, work, tuple(file_ids))
```

Immediately before every actual send: wait for retry if any, pass `before_api_call()`, replace the row with attempts plus one and blank correlation IDs, durably commit, write `SEND_ATTEMPT_STARTED`, call `raise_if_stopped()` again, then POST. Never increment attempts for list/get.

- [ ] **Step 4: Implement exact acceptance, polling, and failure classification**

After exact acceptance, checkpoint `request_id` immediately and set `deadline = monotonic() + 120`. Query list and get immediately; after each nonterminal response or transient error, sleep `min(1, remaining)` and continue. Record every actual send/list/get response with the existing typed safe serializers.

Use one classifier with these exact terminal branches:

```python
if correlated and message.status == "COMPLETED" and message.status_name == "success":
    return self._sent(row)
if correlated and message.status == "COMPLETED" and message.status_name == "fail":
    return self._explicit_failure(row, work, message)
if correlated and message.status in {"READY", "PROCESSING"}:
    return None
return PipelineResult(row)
```

On any exact HTTP 429 call `coordinator.record_429()` before returning transient GET or explicit POST/upload classification. An explicit failure schedules `retry_not_before=monotonic()+10`; the third explicit failure checkpoints `FAILED` before writing the final response event. `safe_error_dict` is the only source of persisted external failure data.

Run: `python -m unittest tests.test_pipeline -v`

Expected: PASS for every state, timing boundary, crash boundary, privacy boundary, and no-resend case.

- [ ] **Step 5: Commit the recipient state machine**

```powershell
git add sens_mms/pipeline.py tests/test_pipeline.py
git commit -m "feat: add per-recipient MMS state machine"
```

---

### Task 6: Orchestrate Pending-First Reconciliation and Five Workers

**Files:**

- Modify: `sens_mms/workflow.py`
- Modify: `tests/test_workflow.py`

**Interfaces:**

- Consumes `PreflightReport.work_items`, `RecipientPipeline`, `PipelineResult`, `RunCoordinator`, and `RUN_SETTINGS`.
- Preserves `Workflow.current_token() -> str` and `Workflow.run_live(approval_token: str) -> RunSummary`.
- Uses `ThreadPoolExecutor(max_workers=RUN_SETTINGS.worker_count)` separately for pending reconciliation and new/retry POST phases.
- Adds private `_run_phase(items, file_ids) -> tuple[PipelineResult, ...]`, `_upload_approved_images(report) -> tuple[str, str]`, and `_publish_reservations(report, send_items) -> tuple[tuple[ResultRow, ApprovedWork], ...]`.

- [ ] **Step 1: Write RED pending-first, conditional-upload, and reservation-order tests**

```python
def test_all_pending_futures_join_before_upload_and_new_post(self):
    pending_done = threading.Event()
    api = BarrierApi(pending_done=pending_done)
    workflow = make_workflow(api, rows=(pending_row(),), numbers=(PENDING, NEW))
    workflow.run_live(workflow.current_token())
    self.assertLess(api.index("pending:get:done"), api.index("upload:first"))
    self.assertLess(api.index("upload:second"), api.index("new:send"))
```

Add tests that pending-only success/pending/ambiguous runs upload nothing; a pending explicit failure uploads only when its approved `PipelineResult.needs_post` is true; upload failure or upload 429 performs no automatic upload retry and starts no message POST; both uploaded file IDs are shared read-only in the approved order.

Test that all new and separately approved failed recipients are published in one atomic reservation write before the first worker starts. `START_FRESH` gets a new unique delivery ID and attempts zero; `RESUME_RESERVATION` keeps the existing delivery ID; `RETRY_EXPLICIT` keeps the same row and attempts count.

- [ ] **Step 2: Write RED maximum-concurrency and global-stop tests**

Use seven recipients, a five-party barrier, and an active-counter lock. Assert `maximum_active == 5`, each recipient stays on one thread from POST through terminal poll, and result/event writes remain serialized.

Inject one worker checkpoint failure and one worker event failure while the other four are poised before API calls. Assert every future is joined, the global stop prevents all later API calls, no snapshot is created, and neither `RESULT_SNAPSHOT_WRITTEN` nor `WORKFLOW_COMPLETED` is logged. A normal recipient's explicit delivery failure must not stop unrelated recipients.

- [ ] **Step 3: Replace the serial loop with two bounded phases**

```python
def _run_phase(self, jobs, file_ids=()):
    with ThreadPoolExecutor(max_workers=RUN_SETTINGS.worker_count) as pool:
        futures = [
            pool.submit(self._run_one, row, work, file_ids)
            for row, work in jobs
        ]
        results = []
        for future in as_completed(futures):
            results.append(future.result())
        return tuple(results)
```

Under the live lock: write `WORKFLOW_STARTED`; rebuild preflight and compare the token; checkpoint input validation failures; run all `RECONCILE` jobs; join every future; reload and validate the latest eight-column result; fail if safety stopped or an unexpected worker exception occurred. `HOLD_AMBIGUOUS` never enters the executor.

Convert only approved explicit-failure results into `RETRY_EXPLICIT` jobs. Combine them with `START_FRESH` and `RESUME_RESERVATION`. If this set is empty, skip upload and phase two.

- [ ] **Step 4: Implement conditional immutable upload and atomic reservations**

Before each of the two upload calls, use `coordinator.before_api_call()`. On exact upload 429, call `record_429()`, log only a fixed safe upload failure event without file ID or name, set global stop, and abort without retry.

After both uploads succeed, re-read approval-bound rows and publish all reservations at once:

```python
expected = {
    work.receiving_number: current_rows.get(work.receiving_number)
    for work in send_items
}
replacements = tuple(
    self._reservation_for(work, current_rows, existing_delivery_ids)
    for work in send_items
    if work.action in {"START_FRESH", "RESUME_RESERVATION"}
)
self.coordinator.commit_batch(expected, replacements)
```

Generate all IDs before publication, reject duplicates against current and every snapshot, and write `DELIVERY_ID_ASSIGNED` plus initial state events through the serialized event coordinator before starting any worker. If the batch or any reservation event fails, start no worker.

- [ ] **Step 5: Preserve lock, immutable completion, and run workflow tests**

Join every phase-two future. Only if safety remains healthy, take a Seoul-named immutable snapshot, reload that snapshot for counts, write `RESULT_SNAPSHOT_WRITTEN`, then `WORKFLOW_COMPLETED`. Keep `WORKFLOW_ABORTED` best-effort without replacing the original safe failure.

Run: `python -m unittest tests.test_workflow.WorkflowTests -v`

Expected: PASS with pending-first barrier, conditional upload, all-or-nothing reservations, maximum concurrency five, fail-closed global stop, immutable bytes, and snapshot-derived completion.

- [ ] **Step 6: Commit workflow orchestration**

```powershell
git add sens_mms/workflow.py tests/test_workflow.py
git commit -m "feat: orchestrate five MMS workers"
```

---

### Task 7: Wire the CLI and Operator Documentation to the Worker Contract

**Files:**

- Modify: `sens_mms_cli.py`
- Modify: `tests/test_cli.py`
- Modify: `README.md`

**Interfaces:**

- Preserves the commands `preflight` and `live`, flags `--approval-token`, `--confirm-sender-registered`, and `--resend-failed`.
- Adds no `--workers`, poll, timeout, retry, or rate-limit CLI options.
- Preserves `_completion_dict(summary, store, approved_at)` and the fixed blocked output.
- Wires `SeoulClock`, `RunCoordinator`, and the revised `Workflow` without exposing secrets or identifiers.

- [ ] **Step 1: Write RED CLI wiring and fixed-settings tests**

```python
def test_cli_has_no_runtime_concurrency_or_timing_options(self):
    for option in (
        "--workers", "--poll-interval", "--confirmation-timeout",
        "--retry-delay", "--rate-limit-delay",
    ):
        code, public, _, transport = run_cli(
            ["preflight", option, "99"], root
        )
        self.assertEqual((code, public), (2, BLOCKED_OUTPUT))
        self.assertEqual(transport.calls, [])
```

Update end-to-end mock responses for one integrated MMS request and assert the outgoing body is byte-equal to `MESSAGE_BODY`, contains both file IDs in order, has one recipient and no subject, and uses the preflight-approved content type.

- [ ] **Step 2: Add CLI restart, safety, and public-output regressions**

Cover pending-only no-upload, approved attempts-zero same-ID resume, positive-attempt blank-ID no-call, pending explicit failure conditional retry, upload 429 no retry/no send, seven-recipient maximum-five behavior, stale token before API, event creation failure before API, mid-run durable failure without completion, and unchanged resend snapshot matching.

Assert preflight contains no full recipient number, name, delivery/request/message ID, credentials, signature, or raw error; it deliberately includes the registered sender and exact body for approval. Assert completion contains no sender or recipient number, name, body, delivery/request/message ID, credentials, signature, raw error, snapshot path, or log path. Completion remains limited to total/SENT/FAILED/PENDING counts, fixed failed summary, pending follow-up boolean, the absolute `results/result.csv` path, `live_send`, and approval time.

- [ ] **Step 3: Wire the revised dependencies and keep safe exception handling**

Construct the client and workflow with the approved report's content type and the shared coordinator created inside the live-lock path. Keep `preflight` free of event-log creation and network calls. The production wiring must remain structurally equivalent to:

```python
workflow = Workflow(
    project_root,
    config,
    store,
    client,
    active_clock,
    event_log,
    delivery_id_factory,
    resend_failed=args.resend_failed,
)
summary = workflow.run_live(args.approval_token)
```

Do not catch and print exception text; continue returning only `_BLOCKED_OUTPUT` on failure.

- [ ] **Step 4: Rewrite the active README flow**

Document the same preflight/live commands, fixed settings, integrated MMS request, pending-first phase, attempts-zero resume exception, conditional image upload, global 429 gate, maximum five recipients, results/log locations, and separate per-run live approval. Remove active LMS→MMS language and state clearly that implementation and main merge are not send approval.

Run: `python -m unittest tests.test_cli.CliTests -v`

Expected: PASS with exact public output, fixed settings, no real network, and no unsafe runtime knobs.

- [ ] **Step 5: Commit CLI and operator guidance**

```powershell
git add sens_mms_cli.py tests/test_cli.py README.md
git commit -m "docs: wire integrated MMS worker operation"
```

---

### Task 8: Close Cross-Module Regression, Privacy, and Determinism Gaps

**Files:**

- Modify: `tests/test_inputs.py`
- Modify: `tests/test_api.py`
- Modify: `tests/test_event_log.py`
- Modify: `tests/test_results.py`
- Modify: `tests/test_preflight.py`
- Modify: `tests/test_coordination.py`
- Modify: `tests/test_pipeline.py`
- Modify: `tests/test_workflow.py`
- Modify: `tests/test_cli.py`
- Modify only if a failing test proves necessary: corresponding `sens_mms/*.py` or `sens_mms_cli.py`

**Interfaces:**

- Produces no new production interface; it verifies the complete spec and removes obsolete two-stage test assumptions.
- Preserves seven-column root-result to eight-column `results/result.csv` migration, atomic replacement, snapshots, lock behavior, exact sanitizers, and immutable image bytes.

- [ ] **Step 1: Add the exact approved body byte regression**

```python
EXPECTED_BODY = (
    "[개업소연 안내]\n\n"
    "안녕하세요.\n"
    "국세청에서의 오랜 경험을 바탕으로 호연회계법인에서 새로운 출발을 하게 된 윤성중 세무사입니다.\n\n"
    "그동안 보내주신 관심과 성원에 감사드리며, 앞으로도 많은 관심과 응원 부탁드립니다.\n\n"
    "새로운 시작을 기쁜 마음으로 함께해 주시면 감사하겠습니다.\n"
)

def test_message_body_matches_approved_utf8_bytes(self):
    self.assertEqual(MESSAGE_BODY.encode("utf-8"), EXPECTED_BODY.encode("utf-8"))
```

The expected literal must be independent of the production constant and preserve every newline.

- [ ] **Step 2: Complete the spec scenario matrix with deterministic fakes**

Ensure named tests cover: `202 -> PROCESSING -> COMPLETED/success`; one explicit failure then success; three failures; 120-second pending then restart success; POST response loss; transient list/get errors; SENT and pending no-new-send; duplicate recipient; invalid input/image blocking; attempts-zero resume; positive-attempt blank-ID block; pending-first barrier; pending-only no upload; conditional retry upload; upload failure/429 stop; fixed five-worker maximum; global 429 10/20 and max-deadline merging; log/checkpoint global stop; every crash boundary; exact eight columns; atomic recovery and immutable snapshots.

Use thread-safe fake clocks plus `threading.Barrier`/`Event`. No test may use wall-clock sleep or depend on accidental thread scheduling.

- [ ] **Step 3: Run the complete suite twice and check repository hygiene**

```powershell
python -m unittest discover -s tests -v
python -m unittest discover -s tests -v
git diff --check
rg -n "LMS_THEN_MMS|LEGACY_COMBINED_MMS|lms_status|mms_status|send_lms" sens_mms sens_mms_cli.py README.md AGENTS.md docs/operations/2026-08-17-sens-mms-worker-experiment-worktree-policy.md
git status --short
```

Expected: both test runs pass; `git diff --check` is silent; the active-code/policy search has no matches; status lists only intentional implementation, test, and documentation changes and never `.env`, recipients, images, results, or logs.

- [ ] **Step 4: Review the diff against every spec section**

Run:

```powershell
git diff --stat main...HEAD
git diff --name-status main...HEAD
git log --oneline --decorate main..HEAD
```

Review Section 1 through Section 11 of `docs/superpowers/specs/2026-08-17-sens-mms-worker-pipeline-design.md` and record each section's implementing task in the commit message body or review notes. Confirm no 14-column migration exists, no live API was called, and historical files were marked superseded rather than deleted.

- [ ] **Step 5: Commit regression-only fixes, if the task changed files**

```powershell
git add tests sens_mms sens_mms_cli.py README.md AGENTS.md docs
git commit -m "test: verify integrated MMS worker pipeline"
```

If `git status --short` is empty after the two passing runs and review, do not create an empty commit.

---

### Task 9: Review, Promote with `--no-ff`, and Re-verify Main

**Files:**

- Review all files changed by `main...experiment/lms-then-mms-worker-pipeline`.
- Merge into the main worktree at `C:\workflows\send-mms-workflow`.
- Do not delete either worktree or branch.

**Interfaces:**

- Produces one explicit merge commit on `main` using `--no-ff`.
- Produces no live SENS request and changes no runtime data file.
- Preserves the experiment branch/worktree after promotion.

- [ ] **Step 1: Request code review and resolve every actionable finding**

Use the required execution workflow's review gate against the final branch diff. Re-run the focused test for every accepted fix, then rerun:

```powershell
python -m unittest discover -s tests -v
python -m unittest discover -s tests -v
git diff --check
git status --short --branch
```

Expected: review has no unresolved correctness, safety, privacy, or maintainability findings; both suites pass; the experiment worktree is clean.

- [ ] **Step 2: Verify main and experiment identities before merging**

```powershell
git -C C:\workflows\send-mms-workflow status --short --branch
git -C C:\workflows\send-mms-workflow branch --show-current
git -C C:\workflows\send-mms-workflow-lms-then-mms branch --show-current
git -C C:\workflows\send-mms-workflow log -1 --oneline main
git -C C:\workflows\send-mms-workflow-lms-then-mms log -1 --oneline experiment/lms-then-mms-worker-pipeline
```

Expected: main worktree is on `main`, experiment worktree is on `experiment/lms-then-mms-worker-pipeline`, and neither worktree contains uncommitted changes. If main has unrelated user changes, stop before merge and report them instead of overwriting or stashing them.

- [ ] **Step 3: Merge the reviewed experiment with an explicit merge commit**

```powershell
git -C C:\workflows\send-mms-workflow merge --no-ff experiment/lms-then-mms-worker-pipeline -m "merge: promote integrated MMS worker pipeline"
```

Expected: merge completes without deleting history. If Git reports a conflict, do not use reset, checkout, or automatic deletion; inspect each conflicted file against the 2026-08-17 spec, resolve with normal edits, rerun the full verification, and commit the merge only after the reviewed integrated-MMS contract is preserved.

- [ ] **Step 4: Run the complete suite twice on main and inspect the merge**

```powershell
python -m unittest discover -s tests -v
python -m unittest discover -s tests -v
git diff --check HEAD^1..HEAD
git show --stat --oneline --decorate HEAD
git status --short --branch
```

Run all commands from `C:\workflows\send-mms-workflow`.

Expected: both main test runs pass, the merge diff is clean, `HEAD` is the `--no-ff` merge commit, and main has no unintended runtime data changes.

- [ ] **Step 5: Report promotion without implying live send**

Report the reviewed branch and merge commit, two experiment test runs, two main test runs, the absolute `results/result.csv` path defined by the code, and the statement `실발송: 수행하지 않음; 실행 직전 별도 승인 필요`. Do not report credentials, signatures, names, full numbers, correlation IDs, or actual result/log contents. Leave both the experiment branch and worktree intact.
