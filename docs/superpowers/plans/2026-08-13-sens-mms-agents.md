# SENS MMS AGENTS.md Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a root-level `AGENTS.md` that safely governs the SENS MMS workflow, including input validation, explicit live-send approval, three-state delivery tracking, duplicate prevention, retries, reconciliation, and `result.csv` output.

**Architecture:** `AGENTS.md` is the single operational policy file and derives all normative rules from the approved design document. It defines preflight validation, one-recipient-per-request dispatch, a polling state machine, and restart-safe result reconciliation without implementing or executing the sender itself.

**Tech Stack:** Markdown; NAVER Cloud SENS SMS v2 REST API; PowerShell and ripgrep for document verification.

## Global Constraints

- Treat `docs/superpowers/specs/2026-08-13-sens-mms-agents-design.md` as the approved source of truth.
- Check the current official SENS send, list, result, attachment, and overview documentation before changing or running the workflow.
- Use `receiving_numbers.csv`; detect comma or tab delimiters; require `number`; treat `name` as optional.
- Use exactly two JPG/JPEG files from `mms_img`; each must be at most 300KB and 1500×1440.
- Preserve the approved Korean message body exactly, including blank lines.
- Send one recipient per POST request even though SENS supports larger `messages` arrays.
- Count the initial POST in the maximum of three attempts per user-approved send execution.
- Treat POST `202` only as request acceptance; treat `COMPLETED + success` as terminal success.
- Poll the existing request every 30 seconds and move a nonterminal request to `PENDING_CONFIRMATION` after 10 minutes.
- Never resend a `PENDING_CONFIRMATION` recipient; reconcile its existing request first.
- Use `SENT`, `FAILED`, and `PENDING_CONFIRMATION`; leave `is_sent` empty for unconfirmed delivery.
- Require explicit user approval immediately before every real-send execution.
- Do not initialize Git merely to satisfy a commit step; the current workspace is not a Git repository.

---

### Task 1: Create and verify the root workflow policy

**Files:**
- Create: `AGENTS.md`
- Read: `docs/superpowers/specs/2026-08-13-sens-mms-agents-design.md`
- Verify: `receiving_numbers.csv`
- Verify: `mms_img/mms_01_intro.jpg`
- Verify: `mms_img/mms_02_details.jpg`

**Interfaces:**
- Consumes: The approved design document and the current workspace input paths.
- Produces: A root-scoped `AGENTS.md` that future agents must obey for all files under the project root.

- [ ] **Step 1: Verify the policy file does not already exist**

Run:

```powershell
if (Test-Path -LiteralPath '.\AGENTS.md') { throw 'AGENTS.md already exists; inspect before editing.' }
```

Expected: command exits successfully because `AGENTS.md` is currently absent. If it exists at execution time, stop and inspect it so user-owned rules are not overwritten.

- [ ] **Step 2: Create `AGENTS.md` with the approved sections and exact rules**

Create the file with these headings in this order:

```markdown
# SENS MMS 프로젝트 운영 규칙

## 적용 범위와 우선순위
## 공식 API 명세 확인
## 절대 안전 규칙
## 프로젝트 입력
### 수신번호 파일
### MMS 이미지
### 발송 본문
## 인증과 요청 서명
## 발송 전 검증과 승인
## MMS 발송 요청
## 상태 모델
## 폴링과 재시도 상태 머신
## 모호한 요청 결과
## result.csv 형식
## 재실행과 별도 재발송
## 로그와 개인정보
## 테스트와 완료 보고
```

The file must state all of the following explicitly:

1. The rules apply recursively to the repository root and cannot be relaxed by implementation code.
2. Before workflow work, open the five official URLs from the design; if the API contract has changed, stop real sending and report the difference.
3. Never execute a live MMS send without a fresh, explicit user approval after presenting recipient count, masked sample numbers, sender, body, file names, image validation, and `contentType`.
4. Never expose access keys, secret keys, signatures, full recipient numbers, or names in source, logs, reports, or CSV output other than the required normalized number in `result.csv`.
5. Read `receiving_numbers.csv` as text, auto-detect comma or tab, require `number`, preserve leading zeroes, trim spaces/hyphens, reject nondigits, and deduplicate normalized numbers.
6. Mark invalid input as `FAILED`, `is_sent=false`, `attempts=0`, with `error.status="VALIDATION_ERROR"`, without an API call.
7. Require exactly two JPG/JPEG images in `mms_img`; validate 300KB and 1500×1440 limits; upload once per execution; reuse file IDs; attach in `mms_01_intro.jpg`, `mms_02_details.jpg` order.
8. State that attachment order is controllable but side-by-side rendering is not guaranteed by SENS because the receiving device and message app control presentation.
9. Include the following message verbatim in a fenced text block:

```text
[개업소연 안내]

안녕하세요.
세무사 윤성중 입니다.
6월 말일자로 국세청에서 퇴직하고 호연회계법인에서 새로 시작하게 되었습니다.
많은 응원 부탁드립니다.

감사합니다.
```

10. Use `type="MMS"`, omit a separate subject, do not silently edit the body, and stop for user confirmation if `contentType` classification is unresolved.
11. Read all credentials from environment variables or an approved secret store and generate the HMAC-SHA256 signature according to the official documentation for every request.
12. Send one recipient per POST using a single-entry `messages` array and the two shared file IDs.
13. Define POST `statusCode="202"` as accepted, not terminal delivery success; persist `requestId` immediately; resolve and persist `messageId` through the list endpoint.
14. Poll every 30 seconds; terminal success is `messages[].status="COMPLETED"` plus `messages[].statusName="success"`.
15. Permit a new POST only after an explicit POST failure or `COMPLETED + fail`; wait 30 seconds; allow at most three POST attempts including the initial one in the current approved execution.
16. Treat `READY`, `PROCESSING`, POST response loss, client timeout, transient lookup failure, or missing `messageId` as uncertain rather than failed.
17. After 10 minutes without a terminal result, store `PENDING_CONFIRMATION`, leave `is_sent` empty, retain identifiers, and prohibit any new POST for that recipient.
18. On restart, reconcile every pending request before dispatch; always skip `SENT`; do not resend prior `FAILED` rows without explicit approval.
19. Before a separately approved resend of `FAILED` recipients, archive the old `result.csv` with a timestamp and reset attempts only for the approved failed recipients. Never reset pending recipients.
20. Define the `result.csv` columns exactly as `receiving_number,delivery_status,is_sent,attempts,request_id,message_id,error`; store `error` as `null` or a JSON string with `status` and `message`.
21. Save `result.csv` as UTF-8 using a same-directory temporary file and atomic replacement after every state transition.
22. Require mock-only tests for accepted-to-success, retry success, three failures, pending-then-success, ambiguous POST, transient lookup failure, restart skipping, deduplication, validation failures, and atomic CSV replacement.
23. Require final reporting of counts by `SENT`, `FAILED`, and `PENDING_CONFIRMATION`, plus the absolute path to `result.csv`, without exposing credentials or unmasked logs.

- [ ] **Step 3: Run structural validation**

Run:

```powershell
$policy = Get-Content -Raw -Encoding UTF8 -LiteralPath '.\AGENTS.md'
$required = @(
  'https://api.ncloud-docs.com/docs/sens-sms-send',
  'https://api.ncloud-docs.com/docs/sens-sms-list',
  'https://api.ncloud-docs.com/docs/sens-sms-get',
  'https://api.ncloud-docs.com/docs/sens-sms-attachment-create',
  'https://api.ncloud-docs.com/docs/sens-overview',
  'receiving_numbers.csv',
  'mms_01_intro.jpg',
  'mms_02_details.jpg',
  'PENDING_CONFIRMATION',
  'COMPLETED',
  'statusName',
  '30초',
  '10분',
  '최대 3회',
  'result.csv',
  'receiving_number,delivery_status,is_sent,attempts,request_id,message_id,error'
)
$missing = @($required | Where-Object { -not $policy.Contains($_) })
if ($missing.Count -gt 0) { throw "Missing policy tokens: $($missing -join ', ')" }
```

Expected: exit code 0 and no missing token error.

- [ ] **Step 4: Verify exact message preservation and no placeholder text**

Run:

```powershell
$policy = Get-Content -Raw -Encoding UTF8 -LiteralPath '.\AGENTS.md'
$message = "[개업소연 안내]`n`n안녕하세요.`n세무사 윤성중 입니다.`n6월 말일자로 국세청에서 퇴직하고 호연회계법인에서 새로 시작하게 되었습니다.`n많은 응원 부탁드립니다.`n`n감사합니다."
if (-not $policy.Contains($message)) { throw 'Approved message body changed.' }
$placeholderPattern = @(('T' + 'BD'), ('T' + 'ODO'), ('FIX' + 'ME')) -join '|'
if ($policy -match $placeholderPattern) { throw 'Placeholder text remains.' }
```

Expected: exit code 0.

- [ ] **Step 5: Verify the duplicate-prevention invariants**

Run:

```powershell
$policy = Get-Content -Raw -Encoding UTF8 -LiteralPath '.\AGENTS.md'
$invariants = @(
  'PENDING_CONFIRMATION은 실패가 아니다',
  'PENDING_CONFIRMATION 상태에서는 신규 발송 요청을 금지',
  'COMPLETED',
  'success',
  'is_sent`는 빈 값'
)
$missing = @($invariants | Where-Object { -not $policy.Contains($_) })
if ($missing.Count -gt 0) { throw "Missing duplicate-prevention invariants: $($missing -join ', ')" }
```

Expected: exit code 0, proving the policy distinguishes terminal failure from uncertain delivery and prohibits resend while pending.

- [ ] **Step 6: Confirm workspace status without initializing Git**

Run:

```powershell
git rev-parse --is-inside-work-tree
```

Expected: command reports that the current directory is not a Git repository. Do not run `git init`; report that commit creation was skipped because version control is not configured.
