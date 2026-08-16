# SENS LMS-Then-MMS Worker Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (\`- [ ]\`) syntax for tracking.

**Goal:** Replace the experimental branch's one-recipient combined MMS workflow with a crash-safe five-worker pipeline that sends the approved LMS body first and sends an empty-body two-image MMS only after correlated LMS delivery success.

**Architecture:** Keep \`results/result.csv\` as the single atomic checkpoint, expanded to the approved 14-column pipeline schema. Move per-recipient stage transitions into \`RecipientPipeline\`, put shared checkpoint/event/safety/backoff state in focused coordinators, and leave \`Workflow\` responsible for approval validation, pending-first reconciliation, immutable image upload, worker scheduling, and completion.

**Tech Stack:** Python 3.14 standard library (\`argparse\`, \`concurrent.futures\`, \`csv\`, \`dataclasses\`, \`datetime\`, \`hashlib\`, \`json\`, \`os\`, \`pathlib\`, \`secrets\`, \`tempfile\`, \`threading\`, \`time\`, \`typing\`, \`unittest\`, \`urllib\`, \`zoneinfo\`).

## Global Constraints

- Work only in \`C:\workflows\send-mms-workflow-lms-then-mms\` on \`experiment/lms-then-mms-worker-pipeline\`. Do not implement this plan in the main worktree.
- Follow \`docs/operations/2026-08-16-lms-then-mms-experiment-worktree-policy.md\`: no live SENS request, no real \`.env\`, recipient file, MMS images, results, or logs may be copied, linked, or shared from main.
- Before implementation and before any future live execution, re-check the official SENS [send](https://api.ncloud-docs.com/docs/sens-sms-send), [list](https://api.ncloud-docs.com/docs/sens-sms-list), [get](https://api.ncloud-docs.com/docs/sens-sms-get), [attachment](https://api.ncloud-docs.com/docs/sens-sms-attachment-create), and [overview](https://api.ncloud-docs.com/docs/sens-overview) contracts. Stop if fields, authentication, limits, or statuses differ.
- Preserve the exact approved \`MESSAGE_BODY\` bytes in \`sens_mms/inputs.py\`. LMS uses \`type="LMS"\`, \`contentType="COMM"\`, \`countryCode="82"\`, no subject, no files, and one recipient.
- Image MMS uses \`type="MMS"\`, \`content=""\`, \`contentType="COMM"\`, \`countryCode="82"\`, no subject, one recipient, and file IDs in \`mms_01_intro.jpg\`, \`mms_02_details.jpg\` order.
- Read each required JPEG once during final preflight; metadata, SHA-256, approval token, and Base64 upload must use that immutable byte copy. Upload each image at most once per approved run.
- Use exactly five fixed recipient workers. A worker owns one recipient until success, final failure, or pending; LMS and MMS for one recipient are never parallel.
- Poll list and get at one-second intervals. Each accepted POST gets its own 120-second monotonic confirmation window.
- Allow at most three actual POST starts per stage. Retry only explicit non-acceptance or correlated \`COMPLETED + fail\`, after ten seconds and only when the approval work item allows it.
- First HTTP 429 blocks all new send/list/get calls for ten seconds; every later 429 in the same run blocks them for at most twenty seconds. GET 429 is transient; an explicit message-POST HTTP 429 is non-acceptance.
- \`RESERVED\` means the reserved POST has not started. Immediately before a POST, change the stage to \`PENDING_CONFIRMATION\`, increment attempts, persist atomically, log the attempt, re-check global safety, then call SENS.
- A positive-attempt \`PENDING_CONFIRMATION\` stage with blank correlation IDs is ambiguous and never automatically reposted.
- Result columns are exactly \`receiving_number,delivery_id,pipeline,delivery_status,is_sent,lms_status,lms_attempts,lms_request_id,lms_message_id,mms_status,mms_attempts,mms_request_id,mms_message_id,error\`.
- Never log full sender/recipient numbers, names, content, subject, file IDs, service ID, credentials, signatures, auth headers, or raw bodies. Logs use \`delivery_id\` and exact \`stage=LMS|MMS\`.
- Preserve total sanitizers: inspect only exact built-in API value types, retain only approved values, and never invoke caller-controlled string conversion, equality, truthiness, length, or iteration.
- A checkpoint or event-log failure sets global stop and prevents every later new POST. It must not cause an uncertain resend.
- Naive datetimes at workflow, log, snapshot, and migration boundaries mean Asia/Seoul wall time; aware values convert to Asia/Seoul. Never infer host-local timezone.
- Keep the live execution lock for the entire live workflow before \`WORKFLOW_STARTED\` or any API call.
- Tests use fake credentials, non-real numbers, generated JPEG bytes, scripted transports, barriers, and controlled clocks. Passing this plan or its tests is not live-send approval.

---

## Planned File Structure

**Create:**

- \`sens_mms/coordination.py\` — shared clock protocol plus thread-safe checkpoint, event, run-stop, and global 429 coordination.
- \`sens_mms/pipeline.py\` — immutable approved work items and the per-recipient LMS→MMS state machine.
- \`tests/test_coordination.py\` — coordinator serialization, fail-closed behavior, and global backoff tests.
- \`tests/test_pipeline.py\` — stage ordering, polling, retry, pending, and resume tests.

**Modify:**

- \`sens_mms/results.py\` — 14-column row model, stage helpers, strict invariants, 7/8-column migration, and legacy snapshot loading.
- \`sens_mms/api.py\` — separate LMS/MMS POSTs, typed message type, safe transient HTTP metadata, and typed list filtering.
- \`sens_mms/event_log.py\` — allowlisted stage field and expanded 14-column state serialization.
- \`sens_mms/preflight.py\` — categorized immutable approval work, public counts, and token binding.
- \`sens_mms/workflow.py\` — pending-first reconciliation, conditional image upload, five-worker scheduling, and immutable completion.
- \`sens_mms_cli.py\` — migration timestamp, pending continuation flags, dependency wiring, and safe public output.
- \`README.md\` — experimental preflight semantics and explicit no-live-send boundary.
- \`tests/test_results.py\`, \`tests/test_api.py\`, \`tests/test_event_log.py\`, \`tests/test_preflight.py\`, \`tests/test_workflow.py\`, \`tests/test_cli.py\` — focused and integration regressions.

---

### Task 1: Introduce the 14-Column Pipeline Result Model

**Files:**

- Modify: \`sens_mms/results.py:18-319\`
- Modify: \`tests/test_results.py\`

**Interfaces:**

- Produces \`StageName = Literal["LMS", "MMS"]\`.
- Produces \`StageState(status: str, attempts: int, request_id: str, message_id: str)\`.
- Produces \`get_stage(row: ResultRow, stage: StageName) -> StageState\`.
- Produces \`replace_stage(row: ResultRow, stage: StageName, state: StageState) -> ResultRow\`.
- Expands \`ResultRow\` to the exact 14 persisted fields required by the spec.

- [ ] **Step 1: Write RED tests for columns, helpers, and valid representative rows**

Add a literal \`pipeline_row()\` factory and tests covering new LMS reservation, LMS pending, LMS sent/MMS reservation, MMS pending, LMS final failure, MMS final failure, complete success, legacy combined success, and validation failure:

\`\`\`python
def pipeline_row(**changes):
    fields = {
        "receiving_number": "01000000000",
        "delivery_id": VALID_ID,
        "pipeline": "LMS_THEN_MMS",
        "delivery_status": "PENDING_CONFIRMATION",
        "is_sent": "",
        "lms_status": "RESERVED",
        "lms_attempts": 0,
        "lms_request_id": "",
        "lms_message_id": "",
        "mms_status": "NOT_STARTED",
        "mms_attempts": 0,
        "mms_request_id": "",
        "mms_message_id": "",
        "error": None,
    }
    fields.update(changes)
    return ResultRow(**fields)

def test_pipeline_schema_and_stage_helpers_are_exact(self):
    self.assertEqual(COLUMNS, (
        "receiving_number", "delivery_id", "pipeline", "delivery_status",
        "is_sent", "lms_status", "lms_attempts", "lms_request_id",
        "lms_message_id", "mms_status", "mms_attempts",
        "mms_request_id", "mms_message_id", "error",
    ))
    row = pipeline_row()
    self.assertEqual(get_stage(row, "LMS"), StageState("RESERVED", 0, "", ""))
    updated = replace_stage(row, "LMS", StageState(
        "PENDING_CONFIRMATION", 1, "", ""
    ))
    self.assertEqual(updated.lms_attempts, 1)
\`\`\`

- [ ] **Step 2: Run the focused result tests and confirm RED**

Run: \`python -m unittest tests.test_results.ResultTests -v\`

Expected: FAIL because \`ResultRow\`, \`COLUMNS\`, \`StageState\`, \`get_stage\`, and \`replace_stage\` still expose the eight-column combined-MMS model.

- [ ] **Step 3: Implement the exact row fields and stage helper interface**

Add these definitions and serialize fields only in \`COLUMNS\` order:

\`\`\`python
StageName = Literal["LMS", "MMS"]

@dataclass(frozen=True)
class StageState:
    status: str
    attempts: int
    request_id: str
    message_id: str

def get_stage(row: "ResultRow", stage: StageName) -> StageState:
    prefix = stage.lower()
    return StageState(
        status=getattr(row, f"{prefix}_status"),
        attempts=getattr(row, f"{prefix}_attempts"),
        request_id=getattr(row, f"{prefix}_request_id"),
        message_id=getattr(row, f"{prefix}_message_id"),
    )

def replace_stage(row: "ResultRow", stage: StageName, state: StageState) -> "ResultRow":
    prefix = stage.lower()
    return replace(row, **{
        f"{prefix}_status": state.status,
        f"{prefix}_attempts": state.attempts,
        f"{prefix}_request_id": state.request_id,
        f"{prefix}_message_id": state.message_id,
    })
\`\`\`

Implement explicit new-pipeline invariants: \`NOT_STARTED\` has zero attempts and blank IDs; \`RESERVED\` has 0-2 attempts and blank IDs; \`PENDING_CONFIRMATION\` has 1-3 attempts and message ID never exists without request ID; \`SENT\` has at least one attempt and both IDs; stage \`FAILED\` has exactly three attempts. A validation failure is the explicit exception: overall FAILED, blank delivery ID, both stages NOT_STARTED, attempts zero, and \`VALIDATION_ERROR\`.

Validate \`LEGACY_COMBINED_MMS\` separately: LMS is always \`NOT_APPLICABLE\` with zero attempts/blank IDs, while the MMS stage preserves the old combined row's status, attempts, and possibly blank historical IDs without inventing data. Derive valid overall state combinations from the spec table instead of accepting arbitrary cross-products.

- [ ] **Step 4: Add RED adversarial and invalid-combination tests, then make them GREEN**

Add cases for string subclasses, booleans as attempts, legacy LMS values other than \`NOT_APPLICABLE\`, overall \`SENT\` with only LMS sent, MMS sent before LMS, reserved with an ID, and ambiguous pending with attempts zero:

\`\`\`python
def test_invalid_pipeline_combinations_fail_closed(self):
    invalid = (
        pipeline_row(lms_status="SENT", lms_attempts=1,
                     lms_request_id="r", lms_message_id="m"),
        pipeline_row(mms_status="SENT", mms_attempts=1,
                     mms_request_id="r", mms_message_id="m"),
        pipeline_row(lms_status="RESERVED", lms_request_id="r"),
        pipeline_row(lms_status="PENDING_CONFIRMATION", lms_attempts=0),
    )
    for row in invalid:
        with self.subTest(row=row):
            with self.assertRaises(ResultFormatError):
                row.validate()
\`\`\`

Run: \`python -m unittest tests.test_results.ResultTests -v\`

Expected: PASS with every representative state and rejection case covered.

- [ ] **Step 5: Commit the pipeline result model**

\`\`\`powershell
git add sens_mms/results.py tests/test_results.py
git commit -m "feat: add pipeline result state model"
\`\`\`

---

### Task 2: Migrate Existing 7/8-Column Results Without Rewriting History

**Files:**

- Modify: \`sens_mms/results.py:321-482\`
- Modify: \`tests/test_results.py\`
- Modify: \`tests/test_cli.py\`

**Interfaces:**

- Changes \`ResultStore.load(*, allow_legacy_8: bool = False) -> dict[str, ResultRow]\`.
- Changes \`migrate_legacy_result(root: Path, migrated_at: datetime) -> ResultStore\`.
- Preserves \`select_latest_snapshot()\` and \`load_failed_candidates()\`, with snapshot reads mapping immutable eight-column rows in memory.

- [ ] **Step 1: Write RED tests for current-file migration and immutable legacy snapshots**

Create an eight-column \`results/result.csv\`, call migration with a fixed Seoul time, and assert exact mappings:

\`\`\`python
def test_eight_column_current_migrates_to_pipeline_schema_once(self):
    migrated_at = datetime(2026, 8, 16, 12, 34, 56)
    write_current_eight_column_result(self.root, status="SENT")
    store = migrate_legacy_result(self.root, migrated_at)
    row = store.load()["01000000000"]
    self.assertEqual((row.pipeline, row.lms_status, row.mms_status),
                     ("LEGACY_COMBINED_MMS", "NOT_APPLICABLE", "SENT"))
    backup = self.root / "results" / "result_20260816_123456.csv"
    self.assertEqual(backup.read_bytes(), EIGHT_COLUMN_BYTES)
    self.assertEqual(
        migrate_legacy_result(self.root, migrated_at).load(),
        store.load(),
    )
\`\`\`

Cover old root seven-column input, current eight-column input, already-14-column input, marker mismatch, same-second snapshot suffixes, and ambiguous/corrupt rows that must leave every source byte unchanged.

- [ ] **Step 2: Run migration tests and confirm RED**

Run: \`python -m unittest tests.test_results.ResultTests tests.test_cli.CliTests -v\`

Expected: FAIL because migration has no timestamp parameter and only converts root seven-column data into the old eight-column schema.

- [ ] **Step 3: Implement one-way migration and read-only legacy snapshot mapping**

Use exact header dispatch and separate mapping functions:

\`\`\`python
def _decode_error(value: str) -> dict | None:
    return None if value == "null" else json.loads(value)

def _validation_pipeline_row(number: str, error: dict) -> ResultRow:
    return ResultRow(
        receiving_number=number,
        delivery_id="",
        pipeline="LMS_THEN_MMS",
        delivery_status="FAILED",
        is_sent="false",
        lms_status="NOT_STARTED",
        lms_attempts=0,
        lms_request_id="",
        lms_message_id="",
        mms_status="NOT_STARTED",
        mms_attempts=0,
        mms_request_id="",
        mms_message_id="",
        error=error,
    )

def _legacy_combined_row(fields: dict[str, str], delivery_id: str) -> ResultRow:
    return ResultRow(
        receiving_number=fields["receiving_number"],
        delivery_id=delivery_id,
        pipeline="LEGACY_COMBINED_MMS",
        delivery_status=fields["delivery_status"],
        is_sent=fields["is_sent"],
        lms_status="NOT_APPLICABLE",
        lms_attempts=0,
        lms_request_id="",
        lms_message_id="",
        mms_status=fields["delivery_status"],
        mms_attempts=int(fields["attempts"]),
        mms_request_id=fields["request_id"],
        mms_message_id=fields["message_id"],
        error=_decode_error(fields["error"]),
    )

def _map_combined_row(fields: dict[str, str], *, has_delivery_id: bool) -> ResultRow:
    delivery_id = fields["delivery_id"] if has_delivery_id else ""
    status = fields["delivery_status"]
    error = _decode_error(fields["error"])
    validation = (
        status == "FAILED"
        and int(fields["attempts"]) == 0
        and fields["request_id"] == ""
        and fields["message_id"] == ""
        and type(error) is dict
        and error.get("status") == "VALIDATION_ERROR"
    )
    if validation:
        return _validation_pipeline_row(fields["receiving_number"], error)
    return _legacy_combined_row(fields, delivery_id=delivery_id)
\`\`\`

Capture source bytes once, validate/hash/map from that copy, publish the untouched bytes as a collision-safe immutable timestamped snapshot, atomically replace only the current checkpoint with 14-column bytes, and write a versioned migration marker. For old snapshots, \`load_failed_candidates()\` calls \`ResultStore(path).load(allow_legacy_8=True)\`; normal current loads remain strict.

- [ ] **Step 4: Verify migration failure atomicity and CLI timestamp wiring**

Update CLI tests to inject a fixed clock and assert \`migrate_legacy_result(project_root, active_clock.now())\`. Run:

\`python -m unittest tests.test_results.ResultTests tests.test_cli.CliTests -v\`

Expected: PASS; corrupt or changing source data creates no replacement or marker, and repeat migration is byte-stable.

- [ ] **Step 5: Commit result migration**

\`\`\`powershell
git add sens_mms/results.py tests/test_results.py tests/test_cli.py
git commit -m "feat: migrate results to pipeline schema"
\`\`\`

---

### Task 3: Split the SENS Client into LMS and MMS Contracts

**Files:**

- Modify: \`sens_mms/api.py:19-400\`
- Modify: \`tests/test_api.py\`

**Interfaces:**

- Produces \`SensClient.send_lms(to: str, content: str) -> SendResponse\`.
- Produces \`SensClient.send_mms(to: str, file_ids: Sequence[str]) -> SendResponse\`.
- Changes \`SensClient.list_by_time_and_recipient(start: str, end: str, to: str, message_type: str) -> MessageListResponse\`.
- Adds \`MessageRecord.message_type: str\`.
- Adds \`TransientLookupError(message: str, *, http_status: int | None = None)\`.

- [ ] **Step 1: Write RED request-contract and typed-correlation tests**

\`\`\`python
def test_lms_and_mms_bodies_are_exact_and_one_recipient(self):
    transport = RecordingTransport([
        response(202, ACCEPTED), response(202, ACCEPTED),
    ])
    api = client(transport)
    api.send_lms("01000000000", MESSAGE_BODY)
    api.send_mms("01000000000", ("file-1", "file-2"))
    lms = json.loads(transport.calls[0].body)
    mms = json.loads(transport.calls[1].body)
    self.assertEqual(lms, {
        "type": "LMS", "contentType": "COMM", "countryCode": "82",
        "from": "01011112222", "content": MESSAGE_BODY,
        "messages": [{"to": "01000000000"}],
    })
    self.assertEqual(mms, {
        "type": "MMS", "contentType": "COMM", "countryCode": "82",
        "from": "01011112222", "content": "",
        "messages": [{"to": "01000000000"}],
        "files": [{"fileId": "file-1"}, {"fileId": "file-2"}],
    })
\`\`\`

Also assert the time lookup query contains \`type=LMS|MMS\`, message records reject hostile/non-exact type values to empty, GET non-2xx becomes \`TransientLookupError\` with exact integer HTTP status, and POST 429 stays \`ExplicitApiFailure(http_status=429)\`.

- [ ] **Step 2: Run API tests and confirm RED**

Run: \`python -m unittest tests.test_api.ApiTests -v\`

Expected: FAIL because only \`send_one()\` exists, records omit message type, and GET HTTP failures use the send/attachment failure class.

- [ ] **Step 3: Implement the two exact send methods and safe GET failures**

\`\`\`python
def send_lms(self, to: str, content: str) -> SendResponse:
    return self._send_message({
        "type": "LMS",
        "contentType": "COMM",
        "countryCode": "82",
        "from": self._from_number,
        "content": content,
        "messages": [{"to": to}],
    })

def send_mms(self, to: str, file_ids: Sequence[str]) -> SendResponse:
    return self._send_message({
        "type": "MMS",
        "contentType": "COMM",
        "countryCode": "82",
        "from": self._from_number,
        "content": "",
        "messages": [{"to": to}],
        "files": [{"fileId": file_id} for file_id in file_ids],
    })
\`\`\`

Keep exact built-in validation for accepted \`statusCode\` and \`requestId\`. In \`_request_json\`, route non-2xx GET responses to fixed \`TransientLookupError\` while retaining only exact integer \`http_status\`; do not retain raw error text or response bodies.

- [ ] **Step 4: Run API and sanitizer regression tests**

Run: \`python -m unittest tests.test_api.ApiTests tests.test_event_log.EventLogTests -v\`

Expected: PASS; request bodies are exact and adversarial response values remain sanitized.

- [ ] **Step 5: Commit the API contracts**

\`\`\`powershell
git add sens_mms/api.py tests/test_api.py
git commit -m "feat: add separate LMS and MMS API calls"
\`\`\`

---

### Task 4: Add Thread-Safe Coordinators and Stage-Safe Event Logging

**Files:**

- Create: \`sens_mms/coordination.py\`
- Create: \`tests/test_coordination.py\`
- Modify: \`sens_mms/event_log.py:20-431\`
- Modify: \`tests/test_event_log.py\`

**Interfaces:**

- Produces \`RunSafetyCoordinator.stop() -> None\` and \`raise_if_stopped() -> None\`.
- Produces \`CheckpointCoordinator.commit(row: ResultRow) -> None\`.
- Produces \`EventCoordinator.write(event: str, *, stage: str | None = None, **fields) -> None\`.
- Produces \`GlobalBackoff.before_call() -> None\` and \`record_429() -> None\`.
- Moves the structural \`Clock\` protocol from \`workflow.py\` to \`coordination.py\`, with \`monotonic() -> float\`, \`now() -> datetime\`, and \`sleep(seconds: float) -> None\`.
- Adds a keyword-only \`stage: str | None = None\` parameter to \`JsonlEventLog.write\`, with exact allowed values \`None\`, \`"LMS"\`, \`"MMS"\`.

- [ ] **Step 1: Write RED serialization and fail-closed tests**

\`\`\`python
def test_checkpoint_and_event_coordinators_serialize_threads(self):
    safety = RunSafetyCoordinator()
    checkpoints = CheckpointCoordinator(store, safety)
    events = EventCoordinator(log, safety)
    def persist(index):
        row = pipeline_row(
            receiving_number=f"0100000000{index}",
            delivery_id=VALID_IDS[index],
        )
        checkpoints.commit(row)
        events.write("RESULT_STATE_CHANGED", stage="LMS",
                     delivery_id=row.delivery_id, state={
                         "pipeline": row.pipeline,
                         "delivery_status": row.delivery_status,
                         "is_sent": row.is_sent,
                         "lms_status": row.lms_status,
                         "lms_attempts": row.lms_attempts,
                         "lms_request_id": row.lms_request_id,
                         "lms_message_id": row.lms_message_id,
                         "mms_status": row.mms_status,
                         "mms_attempts": row.mms_attempts,
                         "mms_request_id": row.mms_request_id,
                         "mms_message_id": row.mms_message_id,
                         "error": row.error,
                     })
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [
            pool.submit(persist, index)
            for index in range(5)
        ]
        for future in futures:
            future.result()
    self.assertEqual(len(ResultStore(store.path).load()), 5)
    self.assertEqual(log.sequences, [1, 2, 3, 4, 5])
\`\`\`

Add separate tests proving a store/log exception poisons safety before returning, later commits/writes fail with a fixed safe error, stage values outside the allowlist are rejected before bytes are written, and event state uses the 12 non-identity fields from the 14-column row while \`delivery_id\` remains top-level.

- [ ] **Step 2: Write RED global-backoff tests with a locked manual clock**

\`\`\`python
def test_global_backoff_uses_ten_then_twenty_seconds_once_per_window(self):
    clock = LockedManualClock()
    gate = GlobalBackoff(clock, RunSafetyCoordinator())
    gate.record_429()
    gate.before_call()
    gate.record_429()
    gate.before_call()
    gate.record_429()
    gate.before_call()
    self.assertEqual(clock.sleeps, [10, 20, 20])
\`\`\`

Use five threads and a barrier to prove only the first caller advances a shared active window while the other four wait behind the same lock and do not each add another sleep.

- [ ] **Step 3: Implement the coordinator locks and exact stage allowlist**

\`\`\`python
class RunSafetyCoordinator:
    def __init__(self):
        self._stopped = threading.Event()

    def stop(self) -> None:
        self._stopped.set()

    def raise_if_stopped(self) -> None:
        if self._stopped.is_set():
            raise RunSafetyError("workflow safety stop is active")

class Clock(Protocol):
    def monotonic(self) -> float:
        raise NotImplementedError

    def now(self) -> datetime:
        raise NotImplementedError

    def sleep(self, seconds: float) -> None:
        raise NotImplementedError

class GlobalBackoff:
    def __init__(self, clock, safety: RunSafetyCoordinator):
        self._clock = clock
        self._safety = safety
        self._lock = threading.Lock()
        self._blocked_until = 0.0
        self._limit_count = 0
\`\`\`

\`before_call()\` holds the backoff lock while sleeping the remaining monotonic duration so one shared fake-clock advance releases the window for every waiter. \`record_429()\` sets \`blocked_until=max(blocked_until, now + delay)\`, using 10 for count one and 20 thereafter. Checkpoint and event coordinators hold their own locks around the entire durable operation, call \`safety.stop()\` on any exception, and re-raise the original safe exception.

- [ ] **Step 4: Run coordinator and event tests**

Run: \`python -m unittest tests.test_coordination tests.test_event_log -v\`

Expected: PASS with deterministic sequence, no lost rows, exact 10/20/20 sleeps, and no unsafe stage or state fields.

- [ ] **Step 5: Commit shared coordination**

\`\`\`powershell
git add sens_mms/coordination.py sens_mms/event_log.py tests/test_coordination.py tests/test_event_log.py
git commit -m "feat: coordinate concurrent pipeline state"
\`\`\`

---

### Task 5: Build Immutable Approval Work Items

**Files:**

- Modify: \`sens_mms/preflight.py:12-172\`
- Modify: \`tests/test_preflight.py\`

**Interfaces:**

- Consumes 14-column \`ResultRow\` and stage helpers from Task 1.
- Produces \`ApprovedWork(receiving_number, initial_action, allow_followup_mms, allow_retry_on_explicit_failure)\`. The approval token binds the separate current 14-column row identity, including its existing delivery ID.
- Changes \`build_preflight(root, config, result_store, *, resend_failed=False, allow_pending_retry=False, allow_pending_lms_followup_mms=False) -> PreflightReport\`.
- Adds \`PreflightReport.work_items: tuple[ApprovedWork, ...]\`.

- [ ] **Step 1: Write RED categorization tests for every restart state**

\`\`\`python
def test_preflight_builds_explicit_pending_lms_permissions(self):
    seed_lms_pending(self.store)
    report = build_preflight(
        self.root,
        CONFIG,
        self.store,
        allow_pending_retry=True,
        allow_pending_lms_followup_mms=True,
    )
    self.assertEqual(report.work_items, (
        ApprovedWork(
            receiving_number=RECIPIENT,
            initial_action="RECONCILE_LMS",
            allow_followup_mms=True,
            allow_retry_on_explicit_failure=True,
        ),
    ))
\`\`\`

Cover new number→\`START_LMS\`, LMS reserved→\`START_LMS\`, LMS pending→\`RECONCILE_LMS\`, LMS sent/MMS reserved→\`START_MMS\`, MMS pending→\`RECONCILE_MMS\`, current MMS failed resend→\`START_MMS\`, LMS failed/legacy failed resend→\`START_LMS\`, SENT exclusion, ambiguous blank-ID pending reconciliation-only, validation failure exclusion, and legacy pending never retrying the old combined MMS.

- [ ] **Step 2: Run preflight tests and confirm RED**

Run: \`python -m unittest tests.test_preflight.PreflightTests -v\`

Expected: FAIL because the report only exposes eligible and pending number tuples and cannot bind stage permissions.

- [ ] **Step 3: Implement immutable work items and public category counts**

\`\`\`python
@dataclass(frozen=True)
class ApprovedWork:
    receiving_number: str = field(repr=False)
    initial_action: Literal[
        "RECONCILE_LMS", "RECONCILE_MMS", "START_LMS", "START_MMS"
    ]
    allow_followup_mms: bool
    allow_retry_on_explicit_failure: bool
\`\`\`

Public output reports total targets, each action count, masked samples per action, exact LMS body, empty MMS content/no subject, image hashes/order, worker/poll/deadline/retry settings, \`contentType\`, and both conditional permission flags. It never emits work-item recipient numbers or correlation IDs.

- [ ] **Step 4: Bind every permission and current row field into the token**

Canonical JSON must include ordered work items, all exact 14-column current row fields, immutable image hashes, sender, body, \`contentType\`, worker count 5, poll 1, deadline 120, retry delay 10, maximum attempts 3, and the selected resend snapshot name/hash. Add mutation tests for each field.

Run: \`python -m unittest tests.test_preflight.PreflightTests -v\`

Expected: PASS; changing any state, work action, permission, image byte, or setting changes the token.

- [ ] **Step 5: Commit approval planning**

\`\`\`powershell
git add sens_mms/preflight.py tests/test_preflight.py
git commit -m "feat: bind pipeline work into approvals"
\`\`\`

---

### Task 6: Implement the Per-Recipient LMS→MMS State Machine

**Files:**

- Create: \`sens_mms/pipeline.py\`
- Create: \`tests/test_pipeline.py\`

**Interfaces:**

- Consumes \`ApprovedWork\`, \`ResultRow\`, \`SensClient\`, \`coordination.Clock\`, and the four coordinators.
- Produces \`RecipientPipeline.run(row: ResultRow, work: ApprovedWork, file_ids: Sequence[str]) -> ResultRow\`.
- Keeps stage polling and retry private to \`RecipientPipeline\`.

- [ ] **Step 1: Write RED happy-path and hard-ordering tests**

\`\`\`python
def test_lms_success_is_checkpointed_before_mms_post(self):
    api = ScriptedPipelineApi(lms_success(), mms_success())
    pipeline = make_pipeline(api)
    result = pipeline.run(new_lms_reservation(), start_lms_work(), FILE_IDS)
    self.assertEqual(result.delivery_status, "SENT")
    self.assertEqual(api.calls, [
        ("send_lms", RECIPIENT),
        ("list", "LMS"),
        ("get", "LMS"),
        ("send_mms", RECIPIENT, FILE_IDS),
        ("list", "MMS"),
        ("get", "MMS"),
    ])
    self.assertLess(
        checkpoint_index(lms_status="SENT"),
        api.call_index("send_mms"),
    )
\`\`\`

Add a failure-injection test at every LMS checkpoint/event boundary and assert \`send_mms\` never appears.

- [ ] **Step 2: Write RED retry, ambiguity, polling, and resume tests**

Cover each stage's explicit POST failure then success, three failures, accepted \`PROCESSING\` until exactly 120 seconds, transient list/get recovery after one-second sleeps, ambiguous POST with unique typed recovery, ambiguous POST with zero/multiple matches, positive-attempt blank-ID pending no-call behavior, \`RESERVED\` fresh-approved resume, MMS-only resend preserving LMS, and permission flags that stop follow-up/retry.

\`\`\`python
def test_ambiguous_lms_without_unique_typed_match_never_posts_again(self):
    api = ScriptedPipelineApi(AmbiguousPostOutcome("fixed"), lms_matches=())
    result = make_pipeline(api).run(
        new_lms_reservation(), start_lms_work(), FILE_IDS
    )
    self.assertEqual(result.lms_status, "PENDING_CONFIRMATION")
    self.assertEqual(result.lms_attempts, 1)
    self.assertEqual(api.count("send_lms"), 1)
    self.assertEqual(api.count("send_mms"), 0)
\`\`\`

- [ ] **Step 3: Implement stage entry, POST safety ordering, and correlation**

\`\`\`python
class RecipientPipeline:
    def run(
        self,
        row: ResultRow,
        work: ApprovedWork,
        file_ids: Sequence[str],
    ) -> ResultRow:
        if work.initial_action == "RECONCILE_LMS":
            return self._reconcile("LMS", row, work)
        elif work.initial_action == "RECONCILE_MMS":
            return self._reconcile("MMS", row, work)
        elif work.initial_action == "START_LMS":
            row = self._run_stage("LMS", row, work, file_ids)
        else:
            return self._run_stage("MMS", row, work, file_ids)
        if row.lms_status == "SENT" and work.allow_followup_mms:
            row = self._reserve_mms(row)
            return self._run_stage("MMS", row, work, file_ids)
        return row
\`\`\`

Before each send/list/get call: \`backoff.before_call()\`, \`safety.raise_if_stopped()\`, then call the API. For send, persist pending+increment first, write \`SEND_ATTEMPT_STARTED\` with stage, re-check safety, then POST. After exact acceptance, persist request ID immediately; persist message ID immediately after one exact typed correlated list match.

- [ ] **Step 4: Implement exact timing and failure classification**

Set the deadline only after accepted POST. List/get immediately, then sleep \`min(1, remaining)\`; transient errors log a safe response/error event and continue inside the same window. Repeated READY/PROCESSING responses are all logged. Explicit failures reserve the same stage with cleared IDs, sleep ten seconds, and retry only if attempts remain and permission is true. On HTTP 429 call \`record_429()\` before classification. On the third explicit failure, persist stage/overall FAILED before logging the final response.

Run: \`python -m unittest tests.test_pipeline -v\`

Expected: PASS for both stages, every timing boundary, all permissions, and every ambiguous/no-resend case.

- [ ] **Step 5: Commit the recipient state machine**

\`\`\`powershell
git add sens_mms/pipeline.py tests/test_pipeline.py
git commit -m "feat: add recipient LMS to MMS pipeline"
\`\`\`

---

### Task 7: Orchestrate Pending-First Reconciliation and Five Workers

**Files:**

- Modify: \`sens_mms/workflow.py:1-573\`
- Modify: \`tests/test_workflow.py\`

**Interfaces:**

- Consumes \`PreflightReport.work_items\`, \`RecipientPipeline\`, and coordinators.
- Preserves \`Workflow.current_token() -> str\` and \`Workflow.run_live(approval_token: str) -> RunSummary\`.
- Uses the fixed module constant \`WORKER_COUNT = 5\`; no CLI or constructor setting may increase or reduce it.

- [ ] **Step 1: Write RED pending-first and upload-order integration tests**

\`\`\`python
def test_pending_batch_finishes_before_upload_and_new_posts(self):
    workflow = make_workflow(
        work_items=(reconcile_lms_work(), start_lms_work()),
        scripted_pipeline=recording_pipeline,
    )
    workflow.run_live(APPROVAL_TOKEN)
    self.assertLess(recording_pipeline.index("reconcile:LMS:done"),
                    api.index("upload:first"))
    self.assertLess(api.index("upload:second"),
                    recording_pipeline.index("start:LMS"))
\`\`\`

Assert no upload when all work is reconcile-only, upload failure prevents every LMS/MMS POST, and both uploaded IDs are shared read-only in approved order.

- [ ] **Step 2: Write RED concurrency and global-stop tests**

Use barriers to hold five workers active and count entries; enqueue at least seven recipients and assert maximum active is exactly five. Inject one checkpoint/event failure while other workers are between reservation and POST and assert every later \`send_lms\`/\`send_mms\` is absent while accepted lookups preserve possible state.

- [ ] **Step 3: Replace recipient loops with two bounded worker phases**

\`\`\`python
def _run_work_items(self, items, file_ids):
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [
            pool.submit(self._run_one, item, file_ids)
            for item in items
        ]
        for future in as_completed(futures):
            future.result()
\`\`\`

Partition \`RECONCILE_LMS|RECONCILE_MMS\` into phase one. A reconcile action always returns after its lookup; it never starts MMS inside phase one. Rebuild and validate current state against the approval's allowed conditional paths, convert an approved successful LMS reconciliation into a new \`START_MMS\` execution item, determine whether any phase-two POST can occur, upload immutable bytes only then, and run approved START/follow-up/retry work with the same five-worker bound.

Before uploading, atomically reserve every phase-two item that needs a new delivery ID. New and LMS-failed/legacy-failed \`START_LMS\` rows receive LMS \`RESERVED\`; LMS-success/MMS-failed \`START_MMS\` rows receive a new delivery ID while preserving the LMS success fields and setting MMS \`RESERVED\`. Existing \`RESERVED\` rows keep their delivery ID. Re-read all rows immediately before publishing the reservation batch and fail closed if any approval-bound identity changed.

- [ ] **Step 4: Preserve lock, completion, and snapshot safety**

Keep the live lock around \`WORKFLOW_STARTED\`, both phases, snapshot, and \`WORKFLOW_COMPLETED\`. Snapshot only after every future is joined. Derive counts from the immutable snapshot, and keep completion paths/summary free of recipient and correlation IDs.

Run: \`python -m unittest tests.test_workflow.WorkflowTests -v\`

Expected: PASS including legacy tests updated to stage semantics, maximum-five concurrency, fail-closed global stop, pending-first order, and immutable image reuse.

- [ ] **Step 5: Commit workflow orchestration**

\`\`\`powershell
git add sens_mms/workflow.py tests/test_workflow.py
git commit -m "feat: run five isolated recipient pipelines"
\`\`\`

---

### Task 8: Wire CLI Approval Flags and Safe Operator Documentation

**Files:**

- Modify: \`sens_mms_cli.py:81-207\`
- Modify: \`tests/test_cli.py\`
- Modify: \`README.md\`

**Interfaces:**

- Adds CLI flags \`--allow-pending-retry\` and \`--allow-pending-lms-followup-mms\`.
- Passes both flags to both preflight builds and approval-token validation.
- Passes \`active_clock.now()\` to \`migrate_legacy_result()\`.
- Keeps public blocked output fixed and completion output correlation-free.

- [ ] **Step 1: Write RED CLI token and output tests**

\`\`\`python
def test_pending_permissions_are_explicit_and_token_bound(self):
    without_flags = run_cli(["preflight"], root)
    with_flags = run_cli([
        "preflight",
        "--allow-pending-retry",
        "--allow-pending-lms-followup-mms",
    ], root)
    self.assertNotEqual(
        without_flags.json["approval_token"],
        with_flags.json["approval_token"],
    )
    self.assertFalse(without_flags.json["allow_pending_retry"])
    self.assertTrue(with_flags.json["allow_pending_retry"])
\`\`\`

Assert a token produced with either flag fails if live invocation omits or changes it, argument errors remain fixed JSON, and no public output contains request/message IDs or full numbers.

- [ ] **Step 2: Run CLI tests and confirm RED**

Run: \`python -m unittest tests.test_cli.CliTests -v\`

Expected: FAIL because the parser and preflight wiring do not expose stage permissions and migration lacks the explicit Seoul timestamp.

- [ ] **Step 3: Implement parser and dependency wiring**

\`\`\`python
parser.add_argument("--allow-pending-retry", action="store_true")
parser.add_argument(
    "--allow-pending-lms-followup-mms",
    action="store_true",
)
store = migrate_legacy_result(project_root, active_clock.now())
report = build_preflight(
    project_root,
    config,
    store,
    resend_failed=args.resend_failed,
    allow_pending_retry=args.allow_pending_retry,
    allow_pending_lms_followup_mms=args.allow_pending_lms_followup_mms,
)
\`\`\`

Use the same arguments for the second in-lock preflight. Do not add any CLI that bypasses approval or enables experimental live sending without the existing live gate.

- [ ] **Step 4: Document experimental commands without authorizing live send**

Update README with the sibling worktree/branch, preflight-only example, action counts, both conditional flags, 14 columns, five workers, 1/120/10 timing, and the statement that this branch is mock-test only under the worktree policy. Do not include credentials, real paths to private inputs, or a runnable live-send command.

Run: \`python -m unittest tests.test_cli.CliTests tests.test_preflight.PreflightTests -v\`

Expected: PASS with exact token binding and privacy-safe public JSON.

- [ ] **Step 5: Commit CLI and operator documentation**

\`\`\`powershell
git add sens_mms_cli.py tests/test_cli.py README.md
git commit -m "feat: expose pipeline approval scope"
\`\`\`

---

### Task 9: Complete Cross-Module Regression and Spec Verification

**Files:**

- Modify: \`tests/test_workflow.py\`
- Modify: \`tests/test_cli.py\`
- Modify: \`tests/test_event_log.py\`
- Modify: \`tests/test_results.py\`
- Verify: \`AGENTS.md\`
- Verify: \`docs/superpowers/specs/2026-08-16-sens-lms-then-mms-worker-pipeline-design.md\`
- Verify: \`docs/operations/2026-08-16-lms-then-mms-experiment-worktree-policy.md\`

**Interfaces:**

- Consumes every interface from Tasks 1-8.
- Produces no new production API; this task closes coverage and verifies the written contract.

- [ ] **Step 1: Add one full scripted two-recipient run**

Use two non-real recipients, fixed IDs, immutable generated JPEG bytes, controlled time, interleaved LMS/MMS responses, one transient GET, and one explicit MMS retry. Assert exact per-recipient rows, ordered API calls, every send/list/get event with stage, one upload per image, and correlation-free completion JSON.

- [ ] **Step 2: Add crash-boundary and migration-to-run regressions**

Parameterize failures after reservation, pending-attempt checkpoint, attempt event, accepted request checkpoint, message checkpoint, LMS success checkpoint, MMS reservation, and final success. Assert whether a restart may start, reconcile, or must hold each stage. Start a second integration case from an eight-column legacy SENT/pending/failed mixture and prove only explicitly approved rows enter the new pipeline.

- [ ] **Step 3: Run focused security and privacy suites**

Run:

\`python -m unittest tests.test_api tests.test_event_log tests.test_results tests.test_preflight -v\`

Expected: PASS with hostile subclasses never reaching durable JSON and no API-controlled human text retained.

- [ ] **Step 4: Run the entire suite twice**

Run:

\`python -m unittest discover -s tests -v\`

Then run the same command a second time.

Expected: both runs PASS with zero failures/errors, demonstrating no order-dependent global backoff, lock, migration, or worktree state.

- [ ] **Step 5: Verify repository scope and commit final regressions**

Run:

\`\`\`powershell
git status --short
git diff --check
git branch --show-current
git worktree list
\`\`\`

Expected: branch is \`experiment/lms-then-mms-worker-pipeline\`; main remains in \`C:\workflows\send-mms-workflow\`; no \`.env\`, real recipients, MMS images, results, or logs are tracked; diff check is clean.

\`\`\`powershell
git add tests/test_workflow.py tests/test_cli.py tests/test_event_log.py tests/test_results.py
git commit -m "test: verify LMS then MMS pipeline"
\`\`\`

---

## Spec Coverage Map

| Spec requirement | Implemented by |
| --- | --- |
| Exact LMS/MMS API bodies and immutable image bytes | Tasks 3, 7 |
| 14-column result schema and stage invariants | Task 1 |
| Existing result/snapshot migration | Task 2 |
| Stage-aware privacy-safe event records | Task 4 |
| Explicit approval actions and conditional permissions | Task 5 |
| LMS-before-MMS ordering, polling, retries, ambiguity | Task 6 |
| Five workers, pending-first execution, global stop | Tasks 4, 7 |
| Global HTTP 429 backoff | Tasks 3, 4, 6 |
| CLI token binding and safe completion | Task 8 |
| Crash, privacy, migration, and full regressions | Task 9 |

## Execution Checkpoints

- After Tasks 1-2: the experimental branch can safely read/migrate results but still uses the old sender.
- After Tasks 3-4: API and shared concurrency primitives are independently tested but not wired live.
- After Tasks 5-6: approvals and one-recipient LMS→MMS behavior are complete under fakes.
- After Tasks 7-8: the full experimental workflow and CLI are integrated, still mock-test only.
- After Task 9: the branch is ready for code review and promotion assessment, not live sending.
