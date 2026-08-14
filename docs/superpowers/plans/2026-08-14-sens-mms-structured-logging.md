# SENS MMS Structured Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-delivery 16-character identifiers, durable per-run JSONL API-response logs, restart-safe result checkpoints, timestamped completion snapshots, and latest-failure resend selection without exposing recipient numbers or credentials in logs.

**Architecture:** Keep `results/result.csv` as the authoritative crash-recovery checkpoint and create immutable `results/result_{completion_timestamp}.csv` snapshots after normal completion. A focused JSONL logger records every send, list, and 30-second result-poll response using allowlisted response DTOs and a stable `delivery_id`; the workflow remains the only component that decides retries and state transitions. Failed-resend preflight selects the filename-latest completed snapshot, validates it, cross-checks it against the current checkpoint, and binds the source snapshot hash and candidate set into the approval token.

**Tech Stack:** Python 3.14 standard library (`argparse`, `csv`, `dataclasses`, `datetime`, `hashlib`, `json`, `os`, `pathlib`, `secrets`, `tempfile`, `time`, `unittest`, `urllib`, `zoneinfo`).

## Global Constraints

- Before implementation and before any live execution, re-check the current official [send](https://api.ncloud-docs.com/docs/sens-sms-send), [list](https://api.ncloud-docs.com/docs/sens-sms-list), [get](https://api.ncloud-docs.com/docs/sens-sms-get), [attachment](https://api.ncloud-docs.com/docs/sens-sms-attachment-create), and [overview](https://api.ncloud-docs.com/docs/sens-overview) documentation. Stop live work and report any field, status, authentication, or limit mismatch.
- Do not initialize Git. `C:\workflows\sens-mms` is not a Git repository, so commit steps are intentionally replaced by testable file checkpoints.
- Do not open or print the real root `.env`. Only the approved Python configuration loader may read it, and tests must use injected fake values.
- Never make a live SENS request in tests. All tests use fake transports, APIs, clocks, ID factories, and event sinks.
- Preserve the exact approved Korean `MESSAGE_BODY` already defined in `sens_mms/inputs.py`, including every newline. Do not add a subject.
- Preserve `type="MMS"`, `contentType="COMM"`, `countryCode="82"`, and ordered files `mms_01_intro.jpg`, then `mms_02_details.jpg`.
- Upload both images once per approved live run and reuse the two returned file IDs for every one-recipient POST in that run.
- A POST response with `statusCode="202"` is acceptance only. Final success still requires `COMPLETED + success` for the same `requestId`, `messageId`, and recipient.
- Never resend `SENT`, `READY`, `PROCESSING`, an ambiguous POST, a transient lookup failure, an unresolved request, or any `PENDING_CONFIRMATION` row.
- A new POST is allowed only after a definite POST failure or `COMPLETED + fail`, with a maximum of three attempts under one fresh approval.
- Logs must never contain complete recipient or sender numbers, names, message content, subject, file IDs, service ID, credentials, signatures, authentication headers, or complete request/response bodies.
- `delivery_id` uses exactly 16 characters from `23456789ABCDEFGHJKLMNPQRSTUVWXYZ`, remains stable across retries in one approved run, and changes for a later separately approved resend.
- Every live run must create a writable JSONL event log before its first API call. If logging or checkpoint persistence fails, stop subsequent new POSTs and preserve accepted or uncertain requests as non-resendable.
- All result and JSONL files are retained; no automatic deletion is introduced.
- Convert log, snapshot, and approval-report timestamps explicitly with `ZoneInfo("Asia/Seoul")`; do not rely on the host's implicit local timezone.
- No live send is authorized by implementing or executing this plan. A fresh user approval is still required immediately before every live run.

---

## Planned File Structure

**Create:**

- `sens_mms/event_log.py` — allowlist-based response serialization, safe text masking, durable JSONL append, collision-safe log path creation.
- `tests/test_event_log.py` — logger schema, ordering, durability boundary, response allowlist, and privacy tests.
- `README.md` — safe operator commands, results/log locations, resend workflow, and approval boundary.

**Modify:**

- `AGENTS.md` — new results/log paths, `delivery_id` column and lifecycle, `attempts=0` reservation exception, JSONL rules, snapshot selection, and completion report path.
- `sens_mms/inputs.py` — restore the exact approved `MESSAGE_BODY` before logging work begins.
- `sens_mms/results.py` — expanded schema, root migration, result snapshots, latest snapshot selection, failed candidate loading, and delivery ID creation.
- `sens_mms/api.py` — safe response DTOs and exceptions that retain only allowlisted response metadata.
- `sens_mms/preflight.py` — normal/resend modes, snapshot selection and hashing, current-checkpoint consistency checks, and approval-token binding.
- `sens_mms/workflow.py` — delivery ID reservation, all response events, stable retry correlation, fail-closed logging, snapshot creation, and final summary event.
- `sens_mms_cli.py` — `results/` and `logs/` wiring, `--resend-failed`, migration, event-log lifecycle, and safe completion output.
- `tests/test_results.py` — schema, reservation, migration, snapshot collision, latest selection, and corruption tests.
- `tests/test_api.py` — response DTO and unsafe-field exclusion tests.
- `tests/test_preflight.py` — resend-source selection, token binding, corruption, and current-state mismatch tests.
- `tests/test_workflow.py` — ID stability, every API response, every polling response, retries, aborts, logging failures, and snapshots.
- `tests/test_cli.py` — end-to-end preflight/live/resend wiring and public-output safety.
- `tests/test_inputs.py` — exact approved body regression test.

---

### Task 0: Restore the Approved MMS Body and Clean Baseline

**Files:**

- Modify: `AGENTS.md`
- Modify: `sens_mms/inputs.py`
- Modify: `tests/test_inputs.py`

**Interfaces:**

- Preserves the existing public `MESSAGE_BODY: str` constant.
- Produces a clean pre-feature baseline before Tasks 1-7 begin.

- [ ] **Step 1: Record the reproduced baseline failure and root cause**

Run: `python -m unittest tests.test_inputs.InputTests.test_body_exact tests.test_preflight.PreflightTests.test_public_report_has_exact_body_sender_and_only_masked_recipient -v`

Expected: both tests fail because `sens_mms/inputs.py` contains a different, later-modified announcement body than the exact body already required by `AGENTS.md` and `tests/test_preflight.py`.

- [ ] **Step 2: Strengthen the focused body test while it is RED**

Replace the substring/ending assertions in `tests/test_inputs.py` with one literal equality assertion containing the exact approved body:

```python
EXPECTED_MESSAGE_BODY = """[개업소연 안내]

안녕하세요.
세무사 윤성중 입니다.
6월 말일자로 국세청에서 퇴직하고 호연회계법인에서 새로 시작하게 되었습니다.
많은 응원 부탁드립니다.

감사합니다."""


def test_body_is_exact_approved_text(self):
    self.assertEqual(MESSAGE_BODY, EXPECTED_MESSAGE_BODY)
```

Run: `python -m unittest tests.test_inputs.InputTests.test_body_is_exact_approved_text -v`

Expected: FAIL with a literal body mismatch, not an import or syntax error.

- [ ] **Step 3: Add the baseline safety rule to `AGENTS.md`**

Under 발송 본문, state that workflow implementation or modification may proceed only when an exact-equality automated test confirms `MESSAGE_BODY` matches the approved body byte-for-byte after UTF-8 decoding. This does not change the approved text.

- [ ] **Step 4: Restore only the production constant**

Set `MESSAGE_BODY` in `sens_mms/inputs.py` to the exact `EXPECTED_MESSAGE_BODY` literal above. Do not modify validation, images, API code, CLI behavior, or logging in this task.

- [ ] **Step 5: Verify focused GREEN**

Run: `python -m unittest tests.test_inputs.InputTests.test_body_is_exact_approved_text tests.test_preflight.PreflightTests.test_public_report_has_exact_body_sender_and_only_masked_recipient -v`

Expected: both tests pass with pristine output.

- [ ] **Step 6: Verify the complete baseline**

Run: `python -m unittest discover -s tests -v`

Expected: all 46 pre-feature tests pass, with zero failures and errors.

- [ ] **Step 7: Record the task checkpoint**

Verify that only `AGENTS.md`, `sens_mms/inputs.py`, `tests/test_inputs.py`, and this approved plan changed for Task 0. Do not initialize Git or commit.

---

### Task 1: Update Project Policy and Add the Durable JSONL Event Logger

**Files:**

- Modify: `AGENTS.md`
- Create: `sens_mms/event_log.py`
- Create: `tests/test_event_log.py`

**Interfaces:**

- Produces: `EventLogError`, `JsonlEventLog`, `create_event_log(logs_dir, started_at, now)`, `mask_digit_runs(text)`, and `safe_error_dict(status, message)`.
- `JsonlEventLog.write(...)` writes one complete JSON object with `schema_version=1`, a per-file monotonic `sequence`, and an ISO 8601 `logged_at` value.
- Task 3 adds serializers for its typed API response objects; workflow tasks pass only the resulting allowlisted dictionaries.

- [ ] **Step 1: Update the root policy before adding behavior**

Add exact project rules to `AGENTS.md`:

```text
results/result.csv
results/result_{YYYYMMDD_HHMMSS}.csv
logs/delivery_{YYYYMMDD_HHMMSS}.jsonl
receiving_number,delivery_id,delivery_status,is_sent,attempts,request_id,message_id,error
```

State that `PENDING_CONFIRMATION + attempts=0` is legal only for a newly assigned `delivery_id` reserved immediately before the first POST, with blank request/message IDs and `error=null`. State that logs use `delivery_id` instead of a complete recipient number and that every real list/get poll response is recorded, including repeated `READY` and `PROCESSING` results.

- [ ] **Step 2: Write failing event-schema and sequence tests**

Create `tests/test_event_log.py` with a deterministic clock and a temporary file:

```python
class FixedNow:
    def __init__(self):
        self.value = datetime(2026, 8, 14, 15, 0, tzinfo=timezone(timedelta(hours=9)))

    def __call__(self):
        current = self.value
        self.value += timedelta(milliseconds=1)
        return current


def read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


class EventLogTests(unittest.TestCase):
    def test_every_row_has_schema_version_sequence_and_timestamp(self):
        path = Path(tempfile.mkdtemp()) / "events.jsonl"
        log = JsonlEventLog(path, now=FixedNow())
        log.write("WORKFLOW_STARTED")
        log.write("DELIVERY_ID_ASSIGNED", delivery_id="7K3M9Q2X8C4N6P1R")

        rows = read_jsonl(path)
        self.assertEqual([row["schema_version"] for row in rows], [1, 1])
        self.assertEqual([row["sequence"] for row in rows], [1, 2])
        self.assertTrue(all(row["logged_at"].endswith("+09:00") for row in rows))
```

- [ ] **Step 3: Run the event-log test and verify RED**

Run: `python -m unittest tests.test_event_log.EventLogTests.test_every_row_has_schema_version_sequence_and_timestamp -v`

Expected: import failure because `sens_mms.event_log` does not exist.

- [ ] **Step 4: Implement the minimal durable writer**

Create a focused writer with a fixed signature rather than accepting arbitrary keyword dictionaries:

```python
class EventLogError(RuntimeError):
    pass


class JsonlEventLog:
    def __init__(self, path: Path, *, now):
        self.path = Path(path)
        self._now = now
        self._sequence = 0
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("x", encoding="utf-8", newline="\n")

    def write(
        self,
        event: str,
        *,
        delivery_id: str | None = None,
        attempt: int | None = None,
        api: str | None = None,
        http_status: int | None = None,
        request_id: str | None = None,
        message_id: str | None = None,
        response: dict | None = None,
        error: dict | None = None,
        state: dict | None = None,
        summary: dict | None = None,
        completed_at: str | None = None,
        result_snapshot: str | None = None,
    ) -> None:
        self._sequence += 1
        row = {
            "schema_version": 1,
            "sequence": self._sequence,
            "logged_at": self._now().astimezone(ZoneInfo("Asia/Seoul")).isoformat(timespec="milliseconds"),
            "event": event,
            "delivery_id": delivery_id,
            "attempt": attempt,
            "api": api,
            "http_status": http_status,
            "request_id": request_id,
            "message_id": message_id,
            "response": response,
            "error": error,
            "state": state,
            "summary": summary,
            "completed_at": completed_at,
            "result_snapshot": result_snapshot,
        }
        try:
            self._file.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
            self._file.flush()
            os.fsync(self._file.fileno())
        except OSError as exc:
            raise EventLogError("event log write failed") from exc
```

Implement `close()` and context-manager methods. Convert file-creation `OSError` into `EventLogError` without including the event payload in the exception.

- [ ] **Step 5: Run the schema test and verify GREEN**

Run: `python -m unittest tests.test_event_log.EventLogTests.test_every_row_has_schema_version_sequence_and_timestamp -v`

Expected: PASS and two parseable JSON lines.

- [ ] **Step 6: Add failing safe-text and fixed-shape tests**

Keep Task 1 independent of the API DTOs that are introduced in Task 3. Test masking and the logger's fixed field shape directly:

```python
def test_mask_digit_runs_keeps_only_last_four_digits(self):
    self.assertEqual(
        mask_digit_runs("recipient 01012345678 failed"),
        "recipient *******5678 failed",
    )


def test_safe_error_dict_masks_message_and_has_fixed_keys(self):
    safe = safe_error_dict("3001", "recipient 01012345678 failed")
    self.assertEqual(safe, {
        "status": "3001",
        "message": "recipient *******5678 failed",
    })
```

Use `inspect.signature(JsonlEventLog.write)` to assert there is no `**kwargs`, `headers`, `body`, `recipient`, `sender`, `content`, `files`, or `service_id` parameter. Typed API allowlist tests are added in Task 3.

- [ ] **Step 7: Implement safe error construction and text masking**

Implement:

```python
DIGIT_RUN = re.compile(r"(?<!\d)(\d{4,})(?!\d)")


def mask_digit_runs(value: str) -> str:
    return DIGIT_RUN.sub(
        lambda match: "*" * max(0, len(match.group(1)) - 4) + match.group(1)[-4:],
        str(value),
    )
```

Add `safe_error_dict(status, message)` with exactly `status` and masked `message` keys. API response serializers are deliberately deferred until the typed DTOs exist in Task 3.

- [ ] **Step 8: Add failing collision and write-error tests**

```python
def test_create_event_log_never_overwrites_same_second_file(self):
    logs = Path(tempfile.mkdtemp())
    started = datetime(2026, 8, 14, 15, 0, 0)
    first = create_event_log(logs, started, now=FixedNow())
    first.close()
    second = create_event_log(logs, started, now=FixedNow())
    second.close()
    self.assertEqual(first.path.name, "delivery_20260814_150000.jsonl")
    self.assertEqual(second.path.name, "delivery_20260814_150000_001.jsonl")


def test_fsync_failure_raises_safe_event_log_error(self):
    with mock.patch("sens_mms.event_log.os.fsync", side_effect=OSError("disk full")):
        with self.assertRaisesRegex(EventLogError, "event log write failed"):
            JsonlEventLog(temp_path(), now=FixedNow()).write("WORKFLOW_STARTED")
```

- [ ] **Step 9: Implement collision-safe creation and run the complete logger suite**

Use exclusive creation mode (`"x"`) and try the base name, then `_001` through `_999`. Do not inspect modification times and do not overwrite an existing file.

Run: `python -m unittest tests.test_event_log -v`

Expected: all event-log tests pass, and rendered JSON contains no sensitive test values.

- [ ] **Step 10: Record the task checkpoint**

Verify that only `AGENTS.md`, `sens_mms/event_log.py`, and `tests/test_event_log.py` changed for this task. Do not initialize Git or commit.

---

### Task 2: Expand Result Persistence, Snapshots, Migration, and Delivery IDs

**Files:**

- Modify: `sens_mms/results.py`
- Modify: `tests/test_results.py`

**Interfaces:**

- Produces: `COLUMNS`, expanded `ResultRow`, `ResultStore.for_root(root)`, `ResultStore.snapshot(completed_at)`, `migrate_legacy_result(root)`, `select_latest_snapshot(results_dir)`, `load_failed_candidates(path)`, and `new_delivery_id(existing_ids, chooser=secrets.choice)`.
- Produces: `collect_delivery_ids(results_dir)` so new IDs are checked against the current checkpoint and all historical snapshots.
- `ResultRow.delivery_id` is the second field and defaults to `""` only for legacy/validation rows.
- Later tasks obtain all current result paths through `ResultStore.for_root(root)` rather than constructing `root / "result.csv"`.

- [ ] **Step 1: Write failing schema and reservation tests**

Refactor test construction to keyword arguments and add:

```python
EXPECTED_COLUMNS = (
    "receiving_number", "delivery_id", "delivery_status", "is_sent",
    "attempts", "request_id", "message_id", "error",
)


def test_result_schema_includes_delivery_id_in_exact_position(self):
    store = ResultStore.for_root(temp_root())
    store.upsert(ResultRow(
        receiving_number="01012345678",
        delivery_id="7K3M9Q2X8C4N6P1R",
        delivery_status="SENT",
        is_sent="true",
        attempts=1,
        request_id="request-1",
        message_id="message-1",
        error=None,
    ))
    store.write_atomic()
    with store.path.open(encoding="utf-8", newline="") as handle:
        self.assertEqual(tuple(csv.reader(handle).__next__()), EXPECTED_COLUMNS)


def test_attempt_zero_pending_is_only_valid_as_a_reserved_delivery(self):
    reserved = ResultRow(
        receiving_number="01012345678",
        delivery_id="7K3M9Q2X8C4N6P1R",
        delivery_status="PENDING_CONFIRMATION",
        is_sent="",
        attempts=0,
        request_id="",
        message_id="",
        error=None,
    )
    reserved.validate()
    with self.assertRaises(ResultFormatError):
        replace(reserved, delivery_id="").validate()
```

- [ ] **Step 2: Run focused result tests and verify RED**

Run: `python -m unittest tests.test_results -v`

Expected: failures for the absent `delivery_id`, `for_root`, and reservation support.

- [ ] **Step 3: Implement the expanded row and root-scoped store**

Use this field order:

```python
@dataclass
class ResultRow:
    receiving_number: str
    delivery_id: str
    delivery_status: str
    is_sent: str
    attempts: int
    request_id: str = ""
    message_id: str = ""
    error: dict | None = None
```

Validate non-empty IDs with `re.fullmatch(r"[23456789ABCDEFGHJKLMNPQRSTUVWXYZ]{16}", delivery_id)`. Permit an empty ID for migrated legacy rows and validation failures. Permit `PENDING_CONFIRMATION + attempts=0` only when the ID is non-empty, `is_sent` is blank, request/message IDs are blank, and `error is None`.

Implement:

```python
@classmethod
def for_root(cls, root: Path) -> "ResultStore":
    return cls(Path(root) / "results" / "result.csv")
```

Keep same-directory temporary writes, `flush`, `fsync`, and `os.replace`.

- [ ] **Step 4: Run existing and new schema tests**

Run: `python -m unittest tests.test_results -v`

Expected: schema and invariant tests pass after all old positional row constructions are converted to keyword arguments.

- [ ] **Step 5: Write failing delivery-ID tests**

```python
def test_delivery_id_is_sixteen_allowed_characters_and_retries_on_collision(self):
    values = iter("A" * 16 + "7K3M9Q2X8C4N6P1R")
    generated = new_delivery_id({"A" * 16}, chooser=lambda alphabet: next(values))
    self.assertEqual(generated, "7K3M9Q2X8C4N6P1R")
    self.assertRegex(generated, r"^[23456789ABCDEFGHJKLMNPQRSTUVWXYZ]{16}$")
```

The injected chooser receives the exact alphabet on every character selection. Add a test that rejects lowercase, `0`, `O`, `1`, and `I` in loaded result rows.

- [ ] **Step 6: Implement delivery-ID creation**

Build 16 characters with `secrets.choice(ALPHABET)`, retry the entire ID on collision, and raise a safe `ResultFormatError` after 100 collisions. Never seed the generator from a recipient or timestamp.

- [ ] **Step 7: Write failing snapshot selection and corruption tests**

```python
def test_snapshot_uses_completion_time_and_collision_suffix(self):
    store = seeded_store()
    completed = datetime(2026, 8, 14, 15, 30, 12)
    first = store.snapshot(completed)
    second = store.snapshot(completed)
    self.assertEqual(first.name, "result_20260814_153012.csv")
    self.assertEqual(second.name, "result_20260814_153012_001.csv")
    self.assertEqual(first.read_bytes(), second.read_bytes())


def test_latest_filename_candidate_is_validated_without_fallback(self):
    results = make_results_dir(
        valid="result_20260814_120000.csv",
        corrupt="result_20260814_130000.csv",
    )
    selected = select_latest_snapshot(results)
    self.assertEqual(selected.name, "result_20260814_130000.csv")
    with self.assertRaises(ResultFormatError):
        load_failed_candidates(selected)
```

Also test that unrelated names, partial timestamps, and suffixes above `_999` are ignored, and that selection uses parsed filename timestamps rather than file modification times.

- [ ] **Step 8: Implement immutable snapshots and latest-candidate selection**

`snapshot()` must serialize the current rows to a sibling temporary file, flush and fsync it, then publish with exclusive semantics. Do not use `os.replace` on a timestamped snapshot because it must never overwrite history. `select_latest_snapshot()` returns only the filename-latest candidate; validation remains a separate mandatory step so corrupt latest files cannot be silently skipped.

Normalize `completed_at` with `completed_at.astimezone(ZoneInfo("Asia/Seoul"))` before formatting the filename.

- [ ] **Step 9: Write failing root migration tests**

```python
def test_legacy_root_result_is_copied_with_blank_delivery_id_and_preserved(self):
    root = root_with_legacy_result()
    original = (root / "result.csv").read_bytes()

    store = migrate_legacy_result(root)

    self.assertEqual((root / "result.csv").read_bytes(), original)
    row = next(iter(store.load().values()))
    self.assertEqual(row.delivery_id, "")
    self.assertEqual(store.path, root / "results" / "result.csv")


def test_migration_stops_when_both_old_and_new_current_results_exist(self):
    root = root_with_both_result_locations()
    with self.assertRaisesRegex(ResultFormatError, "both legacy and current result files exist"):
        migrate_legacy_result(root)


def test_completed_migration_marker_allows_later_runs_without_changing_legacy_file(self):
    root = root_with_legacy_result()
    first = migrate_legacy_result(root)
    second = migrate_legacy_result(root)
    self.assertEqual(first.path, second.path)
    self.assertTrue((root / "results" / ".legacy_result_migrated.json").exists())


def test_modified_legacy_file_after_migration_blocks_later_run(self):
    root = root_with_legacy_result()
    migrate_legacy_result(root)
    (root / "result.csv").write_text("changed", encoding="utf-8")
    with self.assertRaises(ResultFormatError):
        migrate_legacy_result(root)
```

- [ ] **Step 10: Implement migration without mutating the legacy file**

Read the old seven-column schema with a dedicated legacy parser. Validate every row, insert a blank `delivery_id`, then write `results/result.csv` atomically. After the copy succeeds, atomically write `results/.legacy_result_migrated.json` containing only `{"schema_version":1,"source_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`, substituting the actual 64-character lowercase SHA-256 at runtime. If both results exist later, accept them only when this marker exists and its source hash matches the unchanged root legacy file; otherwise stop without merging or overwriting. If neither file exists, return an empty root-scoped store. If only the new path exists, validate and return it.

- [ ] **Step 11: Add historical collision-set coverage**

```python
def test_collect_delivery_ids_reads_current_and_all_timestamped_snapshots(self):
    results = seeded_results_with_distinct_current_and_snapshot_ids()
    self.assertEqual(
        collect_delivery_ids(results),
        {"7K3M9Q2X8C4N6P1R", "8M4N2R7X9C5P3K6Q"},
    )
```

Implement the collector with the strict result parser. A corrupt historical result file blocks ID generation rather than being silently ignored.

- [ ] **Step 12: Run the complete results suite**

Run: `python -m unittest tests.test_results -v`

Expected: all schema, atomic write, reservation, ID, snapshot, latest selection, failed candidate, corruption, and migration tests pass.

- [ ] **Step 13: Record the task checkpoint**

Verify that Task 1 tests still pass:

Run: `python -m unittest tests.test_event_log tests.test_results -v`

Do not initialize Git or commit.

---

### Task 3: Return Typed, Allowlisted SENS Responses

**Files:**

- Modify: `sens_mms/api.py`
- Modify: `tests/test_api.py`
- Modify: `sens_mms/event_log.py`
- Modify: `tests/test_event_log.py`

**Interfaces:**

- Produces: `SendResponse`, `MessageRecord`, `MessageListResponse`, and `MessageResultResponse` immutable dataclasses.
- Changes `SensClient.send_one(...) -> SendResponse`.
- Changes `SensClient.list_by_request(...) -> MessageListResponse` and `list_by_time_and_recipient(...) -> MessageListResponse`.
- Changes `SensClient.get_message(...) -> MessageResultResponse`.
- `ExplicitApiFailure` gains safe `http_status` and `response` attributes; they must contain no headers, body, recipient, content, sender, file, or service ID.

- [ ] **Step 1: Write failing DTO parsing tests**

Add official-style responses that deliberately contain unsafe fields:

```python
def test_send_returns_allowlisted_response_metadata(self):
    transport = RecordingTransport([response(202, {
        "requestId": "request-1",
        "requestTime": "2026-08-14T15:00:00.000",
        "statusCode": "202",
        "statusName": "success",
        "unexpected": "must-not-propagate",
    })])
    result = client(transport).send_one("01012345678", ["file-1", "file-2"])
    self.assertEqual(result.request_id, "request-1")
    self.assertEqual(result.http_status, 202)
    self.assertFalse(hasattr(result, "unexpected"))


def test_get_result_parses_only_internal_matching_and_log_fields(self):
    raw = documented_message(
        to="01012345678", content="secret body", sender="0212345678",
        files=[{"fileId": "secret-file"}],
    )
    result = client(RecordingTransport([response(200, {
        "statusCode": "200", "statusName": "success", "messages": [raw],
    })])).get_message("message-1")

    self.assertEqual(result.message.to, "01012345678")
    self.assertFalse(hasattr(result.message, "content"))
    self.assertFalse(hasattr(result.message, "files"))
```

- [ ] **Step 2: Run focused API tests and verify RED**

Run: `python -m unittest tests.test_api.ApiTests.test_send_returns_allowlisted_response_metadata tests.test_api.ApiTests.test_get_result_parses_only_internal_matching_and_log_fields -v`

Expected: current methods return a string/dictionary rather than the new DTOs.

- [ ] **Step 3: Define immutable response DTOs**

Use exact fields:

```python
@dataclass(frozen=True)
class SendResponse:
    http_status: int
    request_id: str
    request_time: str
    status_code: str
    status_name: str


@dataclass(frozen=True)
class MessageRecord:
    request_id: str
    message_id: str
    to: str
    request_time: str
    complete_time: str
    telco_code: str
    status: str
    status_code: str
    status_name: str
    status_message: str


@dataclass(frozen=True)
class MessageListResponse:
    http_status: int
    status_code: str
    status_name: str
    messages: tuple[MessageRecord, ...]
    page_size: int | None
    page_index: int | None
    item_count: int | None
    has_more: bool | None


@dataclass(frozen=True)
class MessageResultResponse:
    http_status: int
    status_code: str
    status_name: str
    message: MessageRecord
```

Parse each named field explicitly, for example `str(data.get("requestId", ""))`, `str(data.get("messageId", ""))`, and `str(data.get("statusCode", ""))`. Validate that `messages` is a list and that get-result contains exactly one message. Do not store raw data on any DTO.

- [ ] **Step 4: Extend explicit failures with safe response metadata**

Use:

```python
class ExplicitApiFailure(SensApiError):
    def __init__(self, status, message, *, http_status=None, response=None):
        super().__init__(f"SENS request failed ({status}): {message}")
        self.status = str(status)
        self.message = str(message)
        self.http_status = http_status
        self.response = dict(response or {})
```

The `response` value may contain only `statusCode`, `statusName`, `requestId`, and `requestTime`. For unreadable HTTP bodies, use an empty dictionary and a generic message. Do not retain response bytes or headers.

- [ ] **Step 5: Implement typed return values and run API tests**

Run: `python -m unittest tests.test_api -v`

Expected: all existing request-construction/signing tests and new DTO tests pass.

- [ ] **Step 6: Write failing typed-serializer privacy tests**

Add tests to `tests/test_event_log.py` using DTOs whose internal `to` field and status message contain a complete number:

```python
def test_message_serializer_omits_internal_recipient_and_masks_status_message(self):
    message = MessageRecord(
        request_id="request-1",
        message_id="message-1",
        to="01012345678",
        request_time="2026-08-14 15:00:00",
        complete_time="2026-08-14 15:00:30",
        telco_code="SKT",
        status="COMPLETED",
        status_code="3001",
        status_name="fail",
        status_message="recipient 01012345678 failed",
    )
    safe = message_to_log_dict(message)
    rendered = json.dumps(safe, ensure_ascii=False)
    self.assertNotIn("01012345678", rendered)
    self.assertNotIn("to", safe)
    self.assertEqual(safe["statusMessage"], "recipient *******5678 failed")
```

Run: `python -m unittest tests.test_event_log.EventLogTests.test_message_serializer_omits_internal_recipient_and_masks_status_message -v`

Expected: import/name failure because the typed serializer has not been added.

- [ ] **Step 7: Bind logger serializers to the DTOs**

Implement explicit DTO-to-dictionary conversions:

```python
def message_to_log_dict(message: MessageRecord) -> dict:
    return {
        "requestId": message.request_id,
        "messageId": message.message_id,
        "requestTime": message.request_time,
        "completeTime": message.complete_time,
        "telcoCode": message.telco_code,
        "status": message.status,
        "statusCode": message.status_code,
        "statusName": message.status_name,
        "statusMessage": mask_digit_runs(message.status_message),
    }
```

`list_to_log_dict` includes the safe envelope and a `messages` list built with this function. `message_result_to_log_dict` includes the safe envelope and one safe `message`. `send_to_log_dict` includes only the five `SendResponse` fields.

- [ ] **Step 8: Run API and logger privacy suites together**

Run: `python -m unittest tests.test_api tests.test_event_log -v`

Expected: all tests pass and test fixtures containing full numbers/body/file IDs never appear in rendered log JSON.

- [ ] **Step 9: Record the task checkpoint**

Review the public attributes of all four DTOs against the official documentation and the design allowlist. Do not initialize Git or commit.

---

### Task 4: Add Normal and Latest-Failed Resend Preflight Modes

**Files:**

- Modify: `sens_mms/preflight.py`
- Modify: `tests/test_preflight.py`

**Interfaces:**

- Changes `build_preflight(root, config, result_store, *, resend_failed=False) -> PreflightReport`.
- Extends `PreflightReport` with `mode`, `resend_source`, `resend_source_sha256`, and `eligible_numbers` representing the exact approved candidate set.
- Consumes `select_latest_snapshot` and `load_failed_candidates` from Task 2.

- [ ] **Step 1: Move all tests to the new results path**

Replace test stores such as `ResultStore(root / "result.csv")` with `ResultStore.for_root(root)`. Keep a dedicated legacy migration fixture only in `tests/test_results.py`.

- [ ] **Step 2: Write failing latest-failed preflight tests**

```python
def test_resend_preflight_selects_every_failed_row_from_filename_latest_snapshot(self):
    root = root_with_inputs(numbers=(FIRST, SECOND, THIRD))
    current = seed_current(root, sent=(FIRST,), failed=(SECOND, THIRD))
    older = write_snapshot(root, "result_20260814_100000.csv", failed=(SECOND,))
    latest = write_snapshot(root, "result_20260814_110000.csv", failed=(SECOND, THIRD))

    report = build_preflight(root, config(), current, resend_failed=True)

    self.assertEqual(report.mode, "resend_failed")
    self.assertEqual(report.eligible_numbers, (SECOND, THIRD))
    self.assertEqual(report.resend_source, latest.name)
    self.assertNotEqual(report.resend_source, older.name)


def test_resend_preflight_public_output_masks_candidates(self):
    report = make_resend_report()
    rendered = json.dumps(report.to_public_dict(), ensure_ascii=False)
    self.assertNotIn(SECOND, rendered)
    self.assertEqual(report.to_public_dict()["eligible_count"], 2)
```

- [ ] **Step 3: Run focused resend tests and verify RED**

Run: `python -m unittest tests.test_preflight -v`

Expected: `build_preflight` does not accept `resend_failed` and the report lacks source metadata.

- [ ] **Step 4: Implement latest snapshot selection and current-checkpoint cross-checking**

For resend mode:

1. Select the filename-latest snapshot.
2. Validate it without falling back.
3. Collect every `FAILED` row in snapshot order whose recipient is still present and valid in the current `receiving_numbers.csv`.
4. Keep validation failures (`attempts=0` with `error.status="VALIDATION_ERROR"`) excluded from resend candidates and report them only in the invalid count.
5. Require each send-eligible candidate to exist in current `result.csv` with `delivery_status="FAILED"` and matching `request_id` and `message_id`.
6. If any send-eligible row differs, raise `ResultFormatError("resend snapshot does not match current checkpoint")` and make no API call.
7. Exclude all `SENT` and `PENDING_CONFIRMATION` rows.

For normal mode, retain existing input-driven eligibility and never make existing `FAILED` eligible.

- [ ] **Step 5: Bind snapshot identity into the approval token**

Add these exact values to canonical JSON:

```python
canonical.update({
    "mode": "resend_failed" if resend_failed else "normal",
    "resendSource": source_path.name if source_path else None,
    "resendSourceSha256": source_hash if source_path else None,
    "eligible": eligible_numbers,
})
```

Keep sender, exact body, content type, ordered image hashes, pending set, and current result-state snapshot in the token.

- [ ] **Step 6: Add failing token-change and corrupt-latest tests**

```python
def test_resend_token_changes_if_selected_snapshot_bytes_change(self):
    first = build_preflight(root, config(), store, resend_failed=True).approval_token
    rewrite_latest_snapshot_with_changed_failure(root)
    second = build_preflight(root, config(), store, resend_failed=True).approval_token
    self.assertNotEqual(first, second)


def test_corrupt_latest_snapshot_stops_without_using_older_valid_file(self):
    make_valid_older_and_corrupt_latest(root)
    with self.assertRaises(ResultFormatError):
        build_preflight(root, config(), store, resend_failed=True)


def test_resend_preflight_never_includes_validation_failure_rows(self):
    seed_latest_snapshot_with_validation_and_api_failures(root)
    report = build_preflight(root, config(), store, resend_failed=True)
    self.assertEqual(report.eligible_numbers, (VALID_API_FAILURE,))
```

- [ ] **Step 7: Run the complete preflight suite**

Run: `python -m unittest tests.test_preflight -v`

Expected: normal and resend modes pass, public output remains masked, and approval tokens change on every relevant source/input change.

- [ ] **Step 8: Record the task checkpoint**

Run: `python -m unittest tests.test_results tests.test_preflight -v`

Expected: both suites pass. Do not initialize Git or commit.

---

### Task 5: Add Delivery-ID Reservation and Success-Path Event Logging to the Workflow

**Files:**

- Modify: `sens_mms/workflow.py`
- Modify: `tests/test_workflow.py`

**Interfaces:**

- Changes `Workflow.__init__(root, config, store, api, clock, event_log, delivery_id_factory, *, resend_failed=False)`.
- Extends `RunSummary` with `result_snapshot: Path` and `event_log: Path` while retaining `total`, `sent`, `failed`, and `pending`.
- Consumes typed API responses from Task 3 and preflight mode from Task 4.

- [ ] **Step 1: Add a recording event sink and deterministic ID factory to workflow tests**

```python
class RecordingEventLog:
    def __init__(self):
        self.path = Path("logs/test.jsonl")
        self.events = []

    def write(self, event, **fields):
        self.events.append((event, fields))


class FixedIdFactory:
    def __init__(self, *values):
        self.values = iter(values)

    def __call__(self, existing_ids):
        value = next(self.values)
        if value in existing_ids:
            raise AssertionError("test ID collision")
        return value
```

Update `ScriptedApi` to return `SendResponse`, `MessageListResponse`, and `MessageResultResponse` objects rather than strings/dictionaries.

- [ ] **Step 2: Write the failing ID reservation success-path test**

```python
def test_delivery_id_is_reserved_before_first_post_and_kept_through_success(self):
    observed = []
    api = successful_api(on_send=lambda: observed.append(load_only_row()))
    log = RecordingEventLog()
    workflow = make_workflow(
        api=api,
        event_log=log,
        delivery_id_factory=FixedIdFactory("7K3M9Q2X8C4N6P1R"),
    )

    summary = workflow.run_live(workflow.current_token())

    before_post = observed[0]
    final = load_only_row()
    self.assertEqual(before_post.delivery_id, "7K3M9Q2X8C4N6P1R")
    self.assertEqual(before_post.delivery_status, "PENDING_CONFIRMATION")
    self.assertEqual(before_post.attempts, 1)
    self.assertEqual(final.delivery_id, before_post.delivery_id)
    self.assertEqual(final.delivery_status, "SENT")
    states = [fields["state"] for event, fields in log.events if event == "RESULT_STATE_CHANGED"]
    self.assertEqual([states[0]["attempts"], states[1]["attempts"]], [0, 1])
```

The pre-POST observation occurs inside `send_one`, proving the checkpoint was persisted before transport invocation.

- [ ] **Step 3: Run the focused workflow test and verify RED**

Run: `python -m unittest tests.test_workflow.WorkflowTests.test_delivery_id_is_reserved_before_first_post_and_kept_through_success -v`

Expected: constructor/signature and `delivery_id` failures.

- [ ] **Step 4: Implement reservation and stable row construction**

At the start of a new delivery:

```python
delivery_id = self.delivery_id_factory(collect_delivery_ids(self.store.path.parent))
reserved = ResultRow(
    receiving_number=recipient,
    delivery_id=delivery_id,
    delivery_status="PENDING_CONFIRMATION",
    is_sent="",
    attempts=0,
    request_id="",
    message_id="",
    error=None,
)
self._save(reserved)
self._log("DELIVERY_ID_ASSIGNED", row=reserved)
```

Immediately before invoking `send_one`, increment and persist `attempts`, then write `SEND_ATTEMPT_STARTED`. Keep the same ID when replacing the row after acceptance, message discovery, and completion.

- [ ] **Step 5: Write failing success-path event tests**

```python
def test_success_path_logs_send_list_every_poll_state_and_completion(self):
    log = RecordingEventLog()
    workflow = make_workflow(
        api=api_with_processing_then_success(),
        event_log=log,
        delivery_id_factory=FixedIdFactory("7K3M9Q2X8C4N6P1R"),
    )
    workflow.run_live(workflow.current_token())

    self.assertEqual([event for event, _ in log.events], [
        "WORKFLOW_STARTED",
        "DELIVERY_ID_ASSIGNED",
        "RESULT_STATE_CHANGED",
        "RESULT_STATE_CHANGED",
        "SEND_ATTEMPT_STARTED",
        "RESULT_STATE_CHANGED",
        "SEND_RESPONSE",
        "RESULT_STATE_CHANGED",
        "MESSAGE_LIST_RESPONSE",
        "DELIVERY_POLL_RESPONSE",
        "RESULT_STATE_CHANGED",
        "DELIVERY_POLL_RESPONSE",
        "RESULT_SNAPSHOT_WRITTEN",
        "WORKFLOW_COMPLETED",
    ])
```

Use exact event ordering as the observable contract. The list response and both polling responses must each appear once.

- [ ] **Step 6: Implement typed response logging on the success path**

- After `SendResponse`, checkpoint its request ID first, then log `SEND_RESPONSE` before any list lookup.
- Log every `MessageListResponse`, even when it contains no unique `messageId`; when it yields a message ID, checkpoint that ID before the response event.
- Log every `MessageResultResponse`, even when the same state repeats; checkpoint a terminal success/failure state before its response event.
- Use DTO serializers from `event_log.py`; never pass `MessageRecord.to` into event fields.
- Emit `RESULT_STATE_CHANGED` after every successful checkpoint write with only ID, state, attempt, request/message IDs, and safe error.

- [ ] **Step 7: Implement normal-completion snapshots and summary events**

After all recipients finish:

```python
snapshot = self.store.snapshot(self.clock.now())
counts = summarize_rows(ResultStore(snapshot).load().values())
self.event_log.write("RESULT_SNAPSHOT_WRITTEN", result_snapshot=str(snapshot.resolve()))
self.event_log.write(
    "WORKFLOW_COMPLETED",
    summary={
        "total": counts.total,
        "sent": counts.sent,
        "failed": counts.failed,
        "pending_confirmation": counts.pending,
    },
    result_snapshot=str(snapshot.resolve()),
)
```

Return the same counts and paths through `RunSummary`. If snapshot creation fails, do not emit `WORKFLOW_COMPLETED`.

- [ ] **Step 8: Run focused success-path workflow tests**

Run: `python -m unittest tests.test_workflow.WorkflowTests.test_delivery_id_is_reserved_before_first_post_and_kept_through_success tests.test_workflow.WorkflowTests.test_success_path_logs_send_list_every_poll_state_and_completion -v`

Expected: PASS with one snapshot and a consistent event order.

- [ ] **Step 9: Run the workflow suite and fix only compatibility breakage**

Run: `python -m unittest tests.test_workflow -v`

Expected: tests unrelated to new retry/error logging pass after fixtures use the new DTOs, keyword `ResultRow` construction, `ResultStore.for_root`, and injected event sink/ID factory. Retry-specific missing behavior remains for Task 6 tests only.

- [ ] **Step 10: Record the task checkpoint**

Run: `python -m unittest tests.test_results tests.test_api tests.test_event_log tests.test_preflight tests.test_workflow -v`

Do not initialize Git or commit.

---

### Task 6: Log Retries, Every Poll, Ambiguous Outcomes, and Fail Closed on Logging Errors

**Files:**

- Modify: `sens_mms/workflow.py`
- Modify: `tests/test_workflow.py`

**Interfaces:**

- Completes all event/error behavior behind the Task 5 `Workflow` interface.
- Produces no new public module; this task completes the workflow state-machine contract.

- [ ] **Step 1: Write the failing stable-ID retry-history test**

```python
def test_explicit_failure_then_success_uses_one_delivery_id_and_two_attempts(self):
    log = RecordingEventLog()
    workflow = make_workflow(
        api=api_with_explicit_failure_then_success(),
        event_log=log,
        delivery_id_factory=FixedIdFactory("7K3M9Q2X8C4N6P1R"),
    )
    workflow.run_live(workflow.current_token())

    send_events = [fields for event, fields in log.events if event == "SEND_ATTEMPT_STARTED"]
    self.assertEqual([item["attempt"] for item in send_events], [1, 2])
    self.assertEqual({item["delivery_id"] for item in send_events}, {"7K3M9Q2X8C4N6P1R"})
    self.assertEqual(
        [fields["attempt"] for event, fields in log.events if event == "RETRY_SCHEDULED"],
        [2],
    )
```

- [ ] **Step 2: Implement explicit-failure and completed-failure events**

- A definite POST failure emits `SEND_RESPONSE` with safe HTTP/response fields and an `error` object.
- `COMPLETED + fail` is already represented by `DELIVERY_POLL_RESPONSE`; also checkpoint the last safe failure before retry scheduling.
- Before a retry sleep, write `RETRY_SCHEDULED` with the upcoming attempt number; sleep 30 seconds; then persist/invoke the next attempt.
- Three definite failures produce `FAILED` with the same `delivery_id` and the last status/status message.

- [ ] **Step 3: Write and implement the all-polls test**

```python
def test_twenty_processing_polls_are_all_logged_before_ten_minute_pending(self):
    log = RecordingEventLog()
    workflow = make_workflow(api=api_with_twenty_processing_results(), event_log=log)
    summary = workflow.run_live(workflow.current_token())

    polls = [fields for event, fields in log.events if event == "DELIVERY_POLL_RESPONSE"]
    self.assertEqual(len(polls), 20)
    self.assertTrue(all(item["response"]["message"]["status"] == "PROCESSING" for item in polls))
    self.assertEqual(summary.pending, 1)
    self.assertEqual(len(workflow.api.sent), 1)
```

Do not deduplicate events by state. Log first, evaluate state second, then sleep at most the remaining deadline.

- [ ] **Step 4: Write and implement ambiguous and lookup-error events**

Add separate tests for:

```text
POST response lost -> SEND_AMBIGUOUS_OUTCOME -> time/recipient list response -> no automatic resend
list lookup network error -> API_LOOKUP_ERROR -> PENDING_CONFIRMATION
get-result network error -> API_LOOKUP_ERROR -> PENDING_CONFIRMATION
requestId without messageId for 10 minutes -> every list response logged -> PENDING_CONFIRMATION
```

`API_LOOKUP_ERROR.error` contains only a fixed category and `mask_digit_runs(exception.message)`. Never log exception `repr`, cause chains, URLs, request bodies, or headers.

- [ ] **Step 5: Write failing pre-existing pending ID-assignment tests**

```python
def test_legacy_pending_gets_delivery_id_before_reconciliation_lookup(self):
    seed_pending(delivery_id="", attempts=1, request_id="request-1", message_id="message-1")
    observed = []
    api = pending_success_api(on_get=lambda: observed.append(load_only_row()))
    workflow = make_workflow(
        api=api,
        delivery_id_factory=FixedIdFactory("7K3M9Q2X8C4N6P1R"),
    )

    workflow.run_live(workflow.current_token())

    self.assertEqual(observed[0].delivery_id, "7K3M9Q2X8C4N6P1R")
    self.assertEqual(load_only_row().delivery_status, "SENT")
    self.assertEqual(api.sent, [])
```

- [ ] **Step 6: Implement reconciliation ID assignment without permitting a new POST**

For a legacy pending row with no ID, assign and checkpoint a new ID, log `DELIVERY_ID_ASSIGNED`, then query the existing identifiers. A pending reservation with `attempts=0` and blank request/message IDs remains pending and is reported for manual review; do not call list, get, upload, or send.

- [ ] **Step 7: Write failing logger-start and mid-run failure tests**

```python
class RaisingEventLog(RecordingEventLog):
    def __init__(self, fail_on_event):
        super().__init__()
        self.fail_on_event = fail_on_event

    def write(self, event, **fields):
        if event == self.fail_on_event:
            raise EventLogError("event log write failed")
        super().write(event, **fields)


def test_workflow_started_log_failure_makes_zero_api_calls(self):
    api = no_calls_allowed_api()
    workflow = make_workflow(api=api, event_log=RaisingEventLog("WORKFLOW_STARTED"))
    with self.assertRaises(EventLogError):
        workflow.run_live(workflow.current_token())
    self.assertEqual((api.uploaded, api.sent), ([], []))


def test_log_failure_after_accepted_response_stops_later_recipients(self):
    api = two_recipient_api()
    workflow = make_workflow(api=api, event_log=RaisingEventLog("SEND_RESPONSE"))
    with self.assertRaises(EventLogError):
        workflow.run_live(workflow.current_token())
    self.assertEqual(len(api.sent), 1)
    self.assertEqual(load_first_row().delivery_status, "PENDING_CONFIRMATION")
```

- [ ] **Step 8: Implement fail-closed logging behavior**

- `WORKFLOW_STARTED` must succeed before image upload or any API call.
- Persist accepted request IDs or ambiguous pending state before propagating a logger failure that occurs after a response.
- Stop all later recipients after `EventLogError`.
- If possible, record `WORKFLOW_ABORTED`; if the logger itself is broken, preserve the original safe exception and do not attempt repeated writes.
- Never create a timestamped completion snapshot after an aborted workflow.

- [ ] **Step 9: Add and run final retry/error workflow scenarios**

Ensure the suite covers:

```text
202 -> PROCESSING -> COMPLETED/success
first explicit POST failure -> retry success
three explicit POST failures -> FAILED
three COMPLETED/fail responses -> FAILED
20 PROCESSING polls over 10 minutes -> pending
ambiguous POST uniquely recovered -> final result
ambiguous POST not uniquely recovered -> pending
transient list/get failure -> pending
restart reconciles pending without new POST
SENT remains excluded
duplicate input sends once
invalid recipient/image makes zero API calls
two recipients reuse one ordered uploaded file pair
```

Run: `python -m unittest tests.test_workflow -v`

Expected: all workflow scenarios pass with stable IDs and complete event histories.

- [ ] **Step 10: Record the task checkpoint**

Run: `python -m unittest discover -s tests -v`

Expected: all suites pass after fixture/interface updates. Do not initialize Git or commit.

---

### Task 7: Wire CLI Migration, Resend Mode, Per-Run Logs, Snapshots, and Safe Output

**Files:**

- Modify: `sens_mms_cli.py`
- Modify: `tests/test_cli.py`
- Create: `README.md`
- Modify: `AGENTS.md` if exact implementation names differ from the approved policy wording

**Interfaces:**

- Adds `--resend-failed` to both `preflight` and `live` commands.
- Uses `migrate_legacy_result(project_root)` before preflight.
- Creates a real event log only after live approval checks pass and before constructing/executing the workflow.
- Public completion output adds `result_snapshot` and `event_log`; it still contains only allowed counts, de-identified errors, pending follow-up, live-send flag, and approval time.

- [ ] **Step 1: Write failing CLI results/log path tests**

```python
def test_normal_live_writes_checkpoint_snapshot_and_jsonl_under_dedicated_folders(self):
    root = make_root()
    code, public = run_successful_live(root)

    self.assertEqual(code, 0)
    self.assertTrue((root / "results" / "result.csv").exists())
    self.assertEqual(len(list((root / "results").glob("result_*.csv"))), 1)
    self.assertEqual(len(list((root / "logs").glob("delivery_*.jsonl"))), 1)
    self.assertEqual(Path(public["result_csv"]), (root / "results" / "result.csv").resolve())
    self.assertTrue(Path(public["result_snapshot"]).is_absolute())
    self.assertTrue(Path(public["event_log"]).is_absolute())
```

- [ ] **Step 2: Run the focused CLI test and verify RED**

Run: `python -m unittest tests.test_cli.CliTests.test_normal_live_writes_checkpoint_snapshot_and_jsonl_under_dedicated_folders -v`

Expected: current CLI uses root `result.csv` and creates neither a log nor snapshot.

- [ ] **Step 3: Add CLI option and safe dependency wiring**

Add:

```python
parser.add_argument("--resend-failed", action="store_true")
```

Use this order:

1. Load configuration.
2. Run `migrate_legacy_result(project_root)`.
3. Build normal/resend preflight.
4. For `preflight`, print the safe report and return without creating a live JSONL file or calling any API.
5. For `live`, check sender confirmation and exact approval token.
6. Create the event log under `root / "logs"`; if creation fails, return blocked with zero API calls.
7. Construct `Workflow(project_root, config, store, client, active_clock, event_log, new_delivery_id, resend_failed=args.resend_failed)` with injected dependencies.
8. Close the event log in `finally` without hiding the original error.

- [ ] **Step 4: Write failing resend preflight/live tests**

```python
def test_resend_preflight_auto_selects_latest_snapshot_and_makes_zero_calls(self):
    root = root_with_completed_failed_snapshots()
    transport = ScriptedHttpTransport()
    code, public = run_cli(["preflight", "--resend-failed"], root, transport)
    self.assertEqual(code, 0)
    self.assertEqual(public["mode"], "resend_failed")
    self.assertEqual(public["eligible_count"], 2)
    self.assertEqual(transport.calls, [])


def test_resend_live_gives_every_candidate_a_new_id_after_fresh_approval(self):
    root = root_with_completed_failed_snapshots()
    old_ids = latest_failed_ids(root)
    token = resend_token(root)
    code, _ = run_cli([
        "live", "--resend-failed", "--approval-token", token,
        "--confirm-sender-registered",
    ], root, successful_two_recipient_transport())
    self.assertEqual(code, 0)
    new_ids = current_ids(root)
    self.assertTrue(old_ids.isdisjoint(new_ids))
```

- [ ] **Step 5: Implement resend initialization without losing other rows**

When live resend starts after token validation, replace only approved `FAILED` candidates with new reservation rows. Preserve every existing `SENT`, `PENDING_CONFIRMATION`, validation-failure, and unrelated row. Do not reset candidates before approval. The latest snapshot and current checkpoint cross-check from Task 4 must run again during live token validation.

- [ ] **Step 6: Update safe completion output**

Return:

```python
{
    "total": summary.total,
    "sent": summary.sent,
    "failed": summary.failed,
    "pending_confirmation": summary.pending,
    "failed_error_summary": deidentified_error_counts,
    "pending_follow_up_required": summary.pending > 0,
    "result_csv": str(store.path.resolve()),
    "result_snapshot": str(summary.result_snapshot.resolve()),
    "event_log": str(summary.event_log.resolve()),
    "live_send": True,
    "approved_at": approved_at,
}
```

Do not print `delivery_id` lists, full recipients, request/message IDs, names, credentials, signatures, or response bodies.

- [ ] **Step 7: Add CLI failure-boundary tests**

Cover:

```text
wrong token -> no log and zero API calls
missing sender confirmation -> no log and zero API calls
corrupt latest resend snapshot -> blocked and zero API calls
current/snapshot mismatch -> blocked and zero API calls
event-log creation failure -> zero API calls
event-log mid-run failure -> remaining recipients not sent
legacy root result migrates once and remains byte-for-byte unchanged
both legacy and new current results -> blocked and zero API calls
```

Run: `python -m unittest tests.test_cli -v`

Expected: all CLI boundaries pass and output contains no fake secret or full recipient.

- [ ] **Step 8: Write the operator README**

Document these commands without including credentials:

```powershell
python sens_mms_cli.py preflight
$approvalToken = Read-Host 'Paste the approval_token printed by this exact preflight'
python sens_mms_cli.py live --approval-token $approvalToken --confirm-sender-registered
python sens_mms_cli.py preflight --resend-failed
$resendApprovalToken = Read-Host 'Paste the approval_token printed by this exact resend preflight'
python sens_mms_cli.py live --resend-failed --approval-token $resendApprovalToken --confirm-sender-registered
```

Explain the roles of `results/result.csv`, timestamped result snapshots, and per-run JSONL logs; the same-ID retry rule; the latest-snapshot selection rule; and the requirement for a fresh approval before every live run. Do not provide a command that bypasses the approval token or sender confirmation.

- [ ] **Step 9: Run the complete mock suite**

Run: `python -m unittest discover -s tests -v`

Expected: all tests pass with no real network access and no real `.env` read by the test process.

- [ ] **Step 10: Run static privacy and policy scans**

Run:

```powershell
rg -n "print\(.*(access_key|secret_key)|authorization|x-ncp-apigw-signature-v2.*print" sens_mms sens_mms_cli.py tests
rg -n 'response\.body|dict\(headers\)|"content"|"from"|"to"|fileId|serviceId' sens_mms\event_log.py
rg -n 'ResultStore\(.*result\.csv' sens_mms sens_mms_cli.py tests
rg -n 'subject' sens_mms sens_mms_cli.py
```

Expected:

- No secret-printing match.
- The event logger contains no generic raw-body/header serialization; any `"to"` occurrence is restricted to an explicit assertion that the field is absent.
- Production code uses `ResultStore.for_root`; legacy paths appear only in migration tests.
- `subject` appears only in negative tests/assertions, never in the live request builder.

- [ ] **Step 11: Run a real-input preflight only**

Run: `python sens_mms_cli.py preflight`

Expected: a safe masked summary, exact approved body, sender, `MMS`, `COMM`, ordered validated images, and approval token. No `logs/delivery_*.jsonl` is created by preflight and no upload/send request occurs.

Do not run the live command during plan execution or implementation completion. Stop after presenting the preflight information required by `AGENTS.md` and request a new explicit approval in a later turn.

- [ ] **Step 12: Record the final implementation checkpoint**

Report only test/preflight status and changed file paths. Do not claim a live send occurred. Do not initialize Git or commit.

---

## Final Acceptance Checklist

- [ ] Exact approved `MESSAGE_BODY` baseline tests pass before logging implementation.
- [ ] Official SENS fields and status semantics were rechecked immediately before implementation verification.
- [ ] `delivery_id` is present in result rows and every related event, remains stable through retry, and changes on a later approved resend.
- [ ] Every real POST/list/get response, including every repeated 30-second poll, has one JSONL event.
- [ ] JSONL rows contain `schema_version=1` and continuous per-file `sequence` values.
- [ ] `results/result.csv` is updated atomically after every state change.
- [ ] Normal completion creates an immutable completion-timestamp snapshot and a matching final-count event.
- [ ] The filename-latest snapshot is selected for resend and a corrupt latest snapshot blocks rather than falling back.
- [ ] Every send-eligible latest-snapshot `FAILED` candidate is prepared, validation failures remain excluded, and no reset or POST happens before a fresh approval.
- [ ] Logging/checkpoint failures stop later new POSTs and never trigger uncertain resends.
- [ ] Existing root `result.csv` is preserved during one-time migration.
- [ ] Full unit suite, privacy scans, and real-input preflight pass without a live SENS call.
