# SENS MMS Sender Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a deterministic Python 3.14 command-line workflow that loads approved secrets from process environment or `.env`, validates the single-recipient MMS inputs, requires a fresh approval token, sends one recipient per request, reconciles final SENS delivery state, and atomically maintains `result.csv`.

**Architecture:** A small `sens_mms` package separates secret loading, input validation, result persistence, signed SENS transport, and the delivery state machine. `sens_mms_cli.py` exposes `preflight` and `live` commands; live mode is impossible without a token derived from the exact validated payload. All tests use the Python standard library and injected fake transports/clocks, so no test reads the project `.env` or calls SENS.

**Tech Stack:** Python 3.14 standard library (`argparse`, `base64`, `csv`, `dataclasses`, `hashlib`, `hmac`, `json`, `pathlib`, `tempfile`, `time`, `urllib`, `unittest`).

## Global Constraints

- Check the current official SENS send, list, get, attachment, overview, and Ncloud signature documentation before live execution.
- Do not initialize Git; this workspace is not a Git repository, so commit steps are intentionally omitted.
- Only the approved Python loader may read root `.env`; agents, shell commands, and tests must never open or print the real file.
- Process environment values override `.env`; only missing approved keys may be filled from `.env`.
- Never log credentials, generated signatures, authentication headers, complete API bodies, names, or unmasked recipient numbers.
- Use exactly `type="MMS"`, `contentType="COMM"`, country code `82`, no `subject`, and this exact body:

```text
[개업소연 안내]

안녕하세요.
세무사 윤성중 입니다.
6월 말일자로 국세청에서 퇴직하고 호연회계법인에서 새로 시작하게 되었습니다.
많은 응원 부탁드립니다.

감사합니다.
```

- Use exactly `mms_01_intro.jpg` then `mms_02_details.jpg`; each must be JPG/JPEG, at most 300KB, and at most 1500x1440.
- Upload each image once per approved live execution and reuse both returned `fileId` values in order.
- Send one recipient per POST and count each POST as one attempt; maximum three attempts per recipient in one approved execution.
- `202` means accepted, not delivered. Final success requires `COMPLETED + success`.
- Poll the same request every 30 seconds for at most 10 minutes. Never resend `READY`, `PROCESSING`, uncertain POST outcomes, lookup errors, or other `PENDING_CONFIRMATION` states.
- Reconcile existing `result.csv` before any new POST; always exclude `SENT`, and do not reset or resend `PENDING_CONFIRMATION`.
- Store columns exactly as `receiving_number,delivery_status,is_sent,attempts,request_id,message_id,error` using same-directory temporary output and `os.replace` after every transition.
- No live API call may occur until the preflight summary has been presented and the user gives fresh explicit approval for that exact token.

---

### Task 1: Policy exception and secret configuration loader

**Files:**
- Modify: `AGENTS.md`
- Create: `sens_mms/__init__.py`
- Create: `sens_mms/config.py`
- Create: `tests/__init__.py`
- Create: `tests/test_config.py`

**Interfaces:**
- Produces: `Config`, `ConfigError`, and `load_config(root: Path, environ: Mapping[str, str], env_path: Path | None = None) -> Config`.
- `Config` fields: `access_key`, `secret_key`, `service_id`, `from_number`, `content_type`.

- [ ] **Step 1: Update the root policy wording**

Replace the absolute `.env` prohibition with the exact limited exception approved in `docs/superpowers/specs/2026-08-13-sens-env-loader-design.md`. Keep all live-send approval and non-disclosure rules unchanged.

- [ ] **Step 2: Write failing configuration tests**

```python
class ConfigTests(unittest.TestCase):
    def test_process_environment_overrides_env_file(self):
        root, env_path = make_fake_root({"NCP_ACCESS_KEY_ID": "file-access"})
        cfg = load_config(root, complete_env(access_key="process-access"), env_path)
        self.assertEqual(cfg.access_key, "process-access")

    def test_missing_values_are_loaded_from_fake_env_file(self):
        root, env_path = make_fake_root(complete_values())
        cfg = load_config(root, {}, env_path)
        self.assertEqual(cfg.content_type, "COMM")

    def test_missing_required_key_reports_only_key_name(self):
        with self.assertRaisesRegex(ConfigError, "NCP_SECRET_KEY") as raised:
            load_config(fake_root_without_secret(), {}, fake_env_path())
        self.assertNotIn("secret-value", str(raised.exception))

    def test_non_comm_content_type_is_rejected(self):
        with self.assertRaisesRegex(ConfigError, "SENS_CONTENT_TYPE"):
            load_config(fake_root(), complete_env(content_type="AD"), fake_env_path())
```

- [ ] **Step 3: Run the tests and verify RED**

Run: `python -m unittest tests.test_config -v`

Expected: import failure because `sens_mms.config` does not exist.

- [ ] **Step 4: Implement the minimal loader**

Implement a local parser that accepts blank lines, comments, optional `export`, unquoted values, and matching single/double quotes. Restrict lookup to the five approved keys and return an immutable dataclass. Never include values in exception messages.

```python
APPROVED_KEYS = (
    "NCP_ACCESS_KEY_ID",
    "NCP_SECRET_KEY",
    "NCP_SENS_SERVICE_ID",
    "NCP_SENS_FROM_NUMBER",
    "SENS_CONTENT_TYPE",
)

@dataclass(frozen=True)
class Config:
    access_key: str
    secret_key: str
    service_id: str
    from_number: str
    content_type: str
```

- [ ] **Step 5: Run the tests and verify GREEN**

Run: `python -m unittest tests.test_config -v`

Expected: all configuration tests pass without printing any fake secret.

---

### Task 2: Recipient, message, and image validation

**Files:**
- Create: `sens_mms/inputs.py`
- Create: `tests/test_inputs.py`

**Interfaces:**
- Produces: `MESSAGE_BODY`, `RecipientSet`, `ImageInfo`, `load_recipients(path)`, `validate_images(directory)`, and `mask_number(number)`.
- `RecipientSet` carries ordered valid unique numbers and validation-failure rows without names.

- [ ] **Step 1: Write failing recipient and image tests**

```python
class InputTests(unittest.TestCase):
    def test_tab_input_preserves_leading_zero_and_deduplicates(self):
        path = write_csv("number\tname\n010-1234-5678\tA\n01012345678\tB\n")
        result = load_recipients(path)
        self.assertEqual(result.valid_numbers, ("01012345678",))

    def test_invalid_number_becomes_zero_attempt_validation_failure(self):
        path = write_csv("number\n010-ABCD-0000\n")
        result = load_recipients(path)
        self.assertEqual(result.failures[0].attempts, 0)
        self.assertEqual(result.failures[0].error_status, "VALIDATION_ERROR")

    def test_images_must_match_exact_names_order_size_and_dimensions(self):
        infos = validate_images(make_two_valid_jpegs())
        self.assertEqual([item.name for item in infos],
                         ["mms_01_intro.jpg", "mms_02_details.jpg"])

    def test_message_body_is_exact(self):
        self.assertEqual(MESSAGE_BODY, EXPECTED_MESSAGE_BODY)
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `python -m unittest tests.test_inputs -v`

Expected: import failure because `sens_mms.inputs` does not exist.

- [ ] **Step 3: Implement deterministic validation**

Use `csv.Sniffer` limited to comma and tab, always treat `number` as text, strip outer whitespace and internal hyphens/whitespace, require digits, and preserve first-seen order. Parse JPEG SOF markers using bytes so no imaging dependency is needed. Reject extra image files and enforce `<= 300 * 1024`, width `<= 1500`, height `<= 1440`.

- [ ] **Step 4: Run the tests and verify GREEN**

Run: `python -m unittest tests.test_inputs -v`

Expected: all tests pass.

---

### Task 3: Restart-safe atomic result storage

**Files:**
- Create: `sens_mms/results.py`
- Create: `tests/test_results.py`

**Interfaces:**
- Produces: `DeliveryStatus`, `ResultRow`, and `ResultStore` with `load()`, `upsert(row)`, `write_atomic()`, `archive_for_resend(timestamp)`.
- `ResultRow.error` is either `None` or `{"status": str, "message": str}`.

- [ ] **Step 1: Write failing result-store tests**

```python
class ResultStoreTests(unittest.TestCase):
    def test_atomic_write_uses_exact_columns_and_preserves_sent_rows(self):
        store = seeded_store(sent_row())
        store.upsert(pending_row())
        store.write_atomic()
        self.assertEqual(read_header(store.path), EXPECTED_COLUMNS)
        self.assertEqual(store.load()[sent_row().receiving_number].delivery_status, "SENT")

    def test_pending_has_blank_is_sent_and_null_error(self):
        row = pending_row(request_id="req", message_id="msg")
        self.assertEqual(row.to_csv()["is_sent"], "")
        self.assertEqual(row.to_csv()["error"], "null")

    def test_corrupt_existing_result_is_rejected_without_overwrite(self):
        path = write_result("wrong,header\n")
        before = path.read_bytes()
        with self.assertRaises(ResultFormatError):
            ResultStore(path).load()
        self.assertEqual(path.read_bytes(), before)
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `python -m unittest tests.test_results -v`

Expected: import failure because `sens_mms.results` does not exist.

- [ ] **Step 3: Implement state invariants and atomic replacement**

Validate exact columns, legal status/is_sent/error combinations, integer attempts, unique normalized rows, and safe JSON error parsing. Write UTF-8 to a sibling temporary file, flush and `os.fsync`, then `os.replace`.

- [ ] **Step 4: Run the tests and verify GREEN**

Run: `python -m unittest tests.test_results -v`

Expected: all tests pass and no temporary file remains.

---

### Task 4: Signed SENS API client with injectable transport

**Files:**
- Create: `sens_mms/api.py`
- Create: `tests/test_api.py`

**Interfaces:**
- Produces: `make_signature(method, uri, timestamp, access_key, secret_key)`, `SensClient`, `ApiResponse`, `ExplicitApiFailure`, `AmbiguousPostOutcome`, `TransientLookupError`.
- `SensClient` methods: `upload_file`, `send_one`, `list_by_request`, `list_by_time_and_recipient`, `get_message`.
- Transport signature: `request(method: str, url: str, headers: Mapping[str, str], body: bytes | None, timeout: float) -> ApiResponse`.

- [ ] **Step 1: Write failing signature and endpoint tests**

```python
class ApiTests(unittest.TestCase):
    def test_signature_uses_method_uri_timestamp_and_access_key(self):
        expected = reference_hmac("POST /sms/v2/services/svc/messages\n1700000000000\naccess")
        self.assertEqual(make_signature("POST", "/sms/v2/services/svc/messages",
                                        "1700000000000", "access", "secret"), expected)

    def test_send_one_has_one_message_no_subject_and_ordered_files(self):
        transport = RecordingTransport(response=accepted("req-1"))
        client = make_client(transport)
        client.send_one("01012345678", ["file-1", "file-2"])
        body = transport.last_json()
        self.assertEqual(body["messages"], [{"to": "01012345678"}])
        self.assertNotIn("subject", body)
        self.assertEqual(body["files"], [{"fileId": "file-1"}, {"fileId": "file-2"}])

    def test_network_loss_during_post_is_ambiguous_not_explicit_failure(self):
        with self.assertRaises(AmbiguousPostOutcome):
            make_client(TimeoutTransport()).send_one("01012345678", ["f1", "f2"])
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `python -m unittest tests.test_api -v`

Expected: import failure because `sens_mms.api` does not exist.

- [ ] **Step 3: Implement signing and minimal endpoints**

Build the signature input exactly as `METHOD + " " + URI_WITH_QUERY + "\n" + TIMESTAMP + "\n" + ACCESS_KEY`, HMAC-SHA256 with the secret, then Base64. Use one timestamp for both signature and header. Classify definite HTTP responses separately from timeouts, connection resets, and missing POST responses. Never include headers or bodies in exception text.

- [ ] **Step 4: Run the tests and verify GREEN**

Run: `python -m unittest tests.test_api -v`

Expected: all tests pass.

---

### Task 5: Preflight approval manifest

**Files:**
- Create: `sens_mms/preflight.py`
- Create: `tests/test_preflight.py`

**Interfaces:**
- Produces: `PreflightReport` and `build_preflight(root, config, result_store) -> PreflightReport`.
- `PreflightReport.approval_token` is a SHA-256 digest of canonical JSON containing sender, exact body, `COMM`, ordered image hashes, eligible normalized recipients, and existing result-state snapshot.

- [ ] **Step 1: Write failing preflight tests**

```python
class PreflightTests(unittest.TestCase):
    def test_report_masks_recipients_but_shows_exact_sender_and_body(self):
        report = build_preflight(valid_root(), config(), empty_store())
        safe = report.to_public_dict()
        self.assertEqual(safe["sender"], config().from_number)
        self.assertEqual(safe["body"], EXPECTED_MESSAGE_BODY)
        self.assertNotIn("01012345678", json.dumps(safe, ensure_ascii=False))

    def test_token_changes_when_any_send_input_changes(self):
        first = build_preflight(valid_root(), config(), empty_store())
        second = build_preflight(root_with_changed_image(), config(), empty_store())
        self.assertNotEqual(first.approval_token, second.approval_token)

    def test_sent_and_pending_recipients_are_not_eligible(self):
        report = build_preflight(valid_root(), config(), store_with_sent_and_pending())
        self.assertEqual(report.eligible_numbers, ())
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `python -m unittest tests.test_preflight -v`

Expected: import failure because `sens_mms.preflight` does not exist.

- [ ] **Step 3: Implement the safe public report and canonical token**

Include total/eligible/invalid/deduplicated counts, up to three masked samples, exact sender, exact body, ordered filename/size/dimensions/SHA-256 data, `type=MMS`, `contentType=COMM`, and result reconciliation summary. Do not include credentials, names, or full recipient numbers in public output.

- [ ] **Step 4: Run the tests and verify GREEN**

Run: `python -m unittest tests.test_preflight -v`

Expected: all tests pass.

---

### Task 6: Delivery state machine and reconciliation

**Files:**
- Create: `sens_mms/workflow.py`
- Create: `tests/test_workflow.py`

**Interfaces:**
- Produces: `Workflow`, `Clock`, and `RunSummary`.
- `Workflow.run_live(approval_token)` first rebuilds preflight, rejects token mismatch, reconciles existing pending rows, uploads files once, then dispatches eligible recipients.

- [ ] **Step 1: Write the first failing success-path test**

```python
def test_accepted_processing_completed_success_becomes_sent(self):
    api = ScriptedApi(send=[accepted("req")], lists=[[message("msg")]],
                      gets=[processing(), completed_success()])
    workflow = make_workflow(api)
    summary = workflow.run_live(workflow.current_token())
    self.assertEqual(summary.sent, 1)
    self.assertEqual(load_only_row().delivery_status, "SENT")
    self.assertEqual(load_only_row().attempts, 1)
```

- [ ] **Step 2: Run the test and verify RED**

Run: `python -m unittest tests.test_workflow.WorkflowTests.test_accepted_processing_completed_success_becomes_sent -v`

Expected: import failure because `sens_mms.workflow` does not exist.

- [ ] **Step 3: Implement the minimal accepted-to-success path**

Persist `PENDING_CONFIRMATION` immediately after `202`, persist `message_id` immediately when found, poll by the same `message_id`, and write `SENT` only for `COMPLETED + success`.

- [ ] **Step 4: Run the success-path test and verify GREEN**

Run the same command; expected PASS.

- [ ] **Step 5: Add failing retry, failure, ambiguity, timeout, and restart tests**

Add separate tests for:

```text
explicit failure -> 30-second wait -> retry success
three explicit failures -> FAILED with final statusCode/statusMessage
10 minutes READY/PROCESSING -> PENDING_CONFIRMATION without a new POST
lost POST response -> time/recipient list lookup; unresolved remains pending
transient result lookup error -> pending, no resend
restart reconciles SENT and PENDING before dispatch
duplicate recipient causes one send call
invalid input/image causes zero API calls
file uploads happen once and file IDs are reused in order
```

- [ ] **Step 6: Run the expanded tests and verify RED**

Run: `python -m unittest tests.test_workflow -v`

Expected: each newly added behavior fails for the missing transition.

- [ ] **Step 7: Implement the remaining state transitions**

Use an injected clock so tests advance time without sleeping. Permit a new POST only for definite POST failure or `COMPLETED + fail`; never for ambiguous or lookup errors. At each transition call `ResultStore.upsert()` followed by atomic write.

- [ ] **Step 8: Run the expanded tests and verify GREEN**

Run: `python -m unittest tests.test_workflow -v`

Expected: all workflow tests pass.

---

### Task 7: CLI, full mock suite, and pre-live handoff

**Files:**
- Create: `sens_mms_cli.py`
- Create: `tests/test_cli.py`
- Create: `README.md`

**Interfaces:**
- Command: `python sens_mms_cli.py preflight`
- Command: `python sens_mms_cli.py live --approval-token <token>`
- `preflight` may load config and validate files but performs no POST.
- `live` rejects absent/mismatched tokens before any upload or send call.

- [ ] **Step 1: Write failing CLI gate tests**

```python
class CliTests(unittest.TestCase):
    def test_preflight_never_calls_live_transport(self):
        code, output = run_cli(["preflight"], transport=FailOnNetworkTransport())
        self.assertEqual(code, 0)
        self.assertIn("approval_token", output)

    def test_live_without_matching_token_makes_zero_calls(self):
        transport = RecordingTransport()
        code, _ = run_cli(["live", "--approval-token", "wrong"], transport=transport)
        self.assertNotEqual(code, 0)
        self.assertEqual(transport.calls, [])
```

- [ ] **Step 2: Run the CLI tests and verify RED**

Run: `python -m unittest tests.test_cli -v`

Expected: import or command failure because the CLI does not exist.

- [ ] **Step 3: Implement commands and safe output**

Return nonzero for configuration/validation/result corruption/token errors. Print JSON summaries containing only fields permitted by `AGENTS.md`. Keep actual API execution exclusively behind `live` plus the matching token.

- [ ] **Step 4: Run the complete mock suite**

Run: `python -m unittest discover -s tests -v`

Expected: all tests pass, including every mock scenario required by `AGENTS.md`, with no network access and no secret output.

- [ ] **Step 5: Run static safety scans**

Run:

```powershell
rg -n "print\(.*(access_key|secret_key)|authorization|x-ncp-apigw-signature-v2.*print" sens_mms sens_mms_cli.py tests
rg -n "subject" sens_mms sens_mms_cli.py
```

Expected: no secret-printing matches; `subject` appears only in a negative test/assertion, never in a live request builder.

- [ ] **Step 6: Run real-input preflight only**

Run: `python sens_mms_cli.py preflight`

Expected: safe summary with one eligible recipient, masked sample, exact sender, exact approved body, both validated images in order, `MMS`, `COMM`, and an approval token. No upload/send POST occurs.

- [ ] **Step 7: Stop and request fresh live-send approval**

Present the preflight output required by `AGENTS.md`, explicitly ask the user to confirm the sender is registered in SENS, and request approval for that exact token. Do not run `live` in the same turn as preflight approval solicitation.

- [ ] **Step 8: After fresh approval, execute and monitor live delivery**

Run `python sens_mms_cli.py live --approval-token <approved-token>`, poll according to the state machine, and report only total/SENT/FAILED/PENDING_CONFIRMATION counts, de-identified failures, pending follow-up need, absolute `result.csv` path, and approval timestamp.
