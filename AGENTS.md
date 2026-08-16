# SENS MMS 프로젝트 운영 규칙

## Structured result and event-log policy

This policy supersedes earlier result-file location and column definitions.

- Results live in `results/result.csv`; immutable snapshots live in `results/result_{YYYYMMDD_HHMMSS}.csv`.
- Logs live in `logs/delivery_{YYYYMMDD_HHMMSS}.jsonl`; same-second collisions use `_001` through `_999` and never overwrite an existing file.
- Result columns and order are exactly `receiving_number,delivery_id,pipeline,delivery_status,is_sent,lms_status,lms_attempts,lms_request_id,lms_message_id,mms_status,mms_attempts,mms_request_id,mms_message_id,error`.
- `pipeline` is exactly `LMS_THEN_MMS` or `LEGACY_COMBINED_MMS`. Stage status values are exactly `NOT_STARTED`, `RESERVED`, `PENDING_CONFIRMATION`, `SENT`, `FAILED`, or `NOT_APPLICABLE`; `NOT_APPLICABLE` is legal only for the LMS stage of `LEGACY_COMBINED_MMS`.
- A stage in `RESERVED` means no POST for that reserved attempt has started. Its request and message IDs are blank, its attempts value counts only earlier actual POST starts, and a fresh approval may continue that same stage. Immediately before an actual POST, atomically change that stage to `PENDING_CONFIRMATION` and increment its attempts. A `PENDING_CONFIRMATION` stage with a positive attempts value and blank correlation IDs is ambiguous and must never be automatically reposted.
- Log records use `delivery_id` and the allowlisted `stage=LMS|MMS`, never a complete recipient number. Record every actual send/list/get response, including repeated `READY` and `PROCESSING` polls.
- Never log full recipient or sender numbers, names, content, subject, file IDs, service ID, credentials, signatures, authentication headers, or raw request/response bodies.
- API-controlled `statusMessage` and error `message` text is never stored or logged verbatim: an empty value stays empty and every non-empty value becomes the fixed literal `redacted`. Status/error codes keep only approved internal constants or one-to-four ASCII digits; every other value becomes `UNKNOWN`.
- API-value sanitizers are total and never invoke caller-controlled `__str__`, equality, truthiness, length, iteration, or similar methods. Only exact built-in types are inspected: status codes accept exact strings or non-boolean exact integers from 0 through 9999; human messages treat only `None`, an exact empty string, exact empty list, or exact empty dict as empty. Every other message type, including subclasses and hostile objects, becomes `redacted`; every other status type becomes `UNKNOWN`.
- Request/message correlation IDs are retained only when they are exact built-in strings. Subclasses and other types become empty before durable JSON encoding.
- API response correlation IDs are accepted only as non-empty exact built-in strings. A 2xx message POST establishes a definite result only when body `statusCode` is an exact built-in string of exactly three ASCII digits. Exact `202` additionally requires a non-empty exact built-in string `requestId`. Missing, empty, whitespace, non-string, non-ASCII-digit, or wrong-length status values and invalid accepted-request IDs are ambiguous outcomes and must never enter the explicit-failure retry path. A malformed list/get envelope or message-record correlation ID is a fixed transient lookup error for the whole lookup.
- Durable API response fields are sanitized by meaning as well as by key. Envelope `statusName` keeps only empty, `success`, `reserved`, or `fail`; message `statusName` keeps only empty, `success`, or `fail`; message `status` keeps only empty, `READY`, `PROCESSING`, or `COMPLETED`. `telcoCode` keeps only the project-approved fixed values empty, `SKT`, and the official example `ETC`; no broader uppercase pattern is trusted because the official documents do not publish a complete enum.
- Send `requestTime` is retained only as `YYYY-MM-DDTHH:mm:ss.sss`; list/get message `requestTime` and `completeTime` are retained only as `YYYY-MM-DD HH:mm:ss`. Pagination fields retain only their documented integer, boolean, or null types. Invalid API-controlled values become empty strings or null and are never copied verbatim.
- Naive datetimes at event-log, snapshot, or Workflow boundaries mean Asia/Seoul wall time; aware datetimes are converted to Asia/Seoul. Host-local timezone inference is forbidden.
- A cooperating live Workflow must hold the project live-execution lock for the entire live run before writing `WORKFLOW_STARTED` or making any API call. The lock combines a canonical-path process-local nonblocking lock with a Windows/POSIX OS file lock and fails closed with a fixed safe error.
- A logging or checkpoint failure stops all later new POSTs and never causes an uncertain resend.
- Final image preflight reads each required JPEG once and retains those immutable bytes internally. Metadata, SHA-256, approval token, and Base64 upload must all derive from that same byte copy; Workflow must not reopen an image path after final token validation.
- Do not automatically delete results, snapshots, or event logs.

## 적용 범위와 우선순위

- 이 파일은 프로젝트 루트와 모든 하위 경로에 적용한다.
- 구현 코드, 스크립트, 테스트, 실행 절차는 이 규칙을 완화하거나 우회할 수 없다.
- 사용자 지시와 최신 공식 API 명세가 충돌하면 실발송을 중단하고 차이를 사용자에게 보고한다.
- 이 프로젝트의 기본 작업은 워크플로우 구현과 검증이다. 실제 LMS/MMS 발송은 별도의 명시적 승인이 필요한 외부 작업이다.

## 공식 API 명세 확인

워크플로우를 변경하거나 실행하기 전에 다음 최신 공식 문서를 확인한다.

- 메시지 발송: <https://api.ncloud-docs.com/docs/sens-sms-send>
- 메시지 발송 목록 조회: <https://api.ncloud-docs.com/docs/sens-sms-list>
- 메시지 발송 결과 조회: <https://api.ncloud-docs.com/docs/sens-sms-get>
- 첨부 파일 업로드: <https://api.ncloud-docs.com/docs/sens-sms-attachment-create>
- SENS 공통 인증 및 응답: <https://api.ncloud-docs.com/docs/sens-overview>

명세에서 요청 필드, 인증, 파일 제한, 상태 코드 또는 결과 필드가 이 문서와 달라졌다면 값을 추정하지 않는다. 실발송을 중단하고 확인된 변경 사항과 필요한 설계 변경을 사용자에게 보고한다.

## 절대 안전 규칙

- 승인된 Python 발송 프로그램만 필요한 자격증명을 프로세스 메모리에 적재하기 위한 목적으로 프로젝트 루트의 `.env`를 읽을 수 있다. 에이전트, 셸, 테스트는 `.env`를 직접 열거나 내용을 출력하지 않는다. 로더는 허용된 키의 존재 여부만 검증하며 비밀값을 로그, 오류, 문서 또는 `result.csv`에 기록하지 않는다.
- 매 실발송 실행 직전에 사용자의 새롭고 명시적인 승인을 받아야 한다.
- 승인 전에 수신 건수, 마스킹된 번호 표본, 발신번호, 정확한 본문, 첨부 파일명, 이미지 검증 결과, `contentType`을 제시한다.
- 구현, 테스트, 드라이런 또는 문서 검증을 실발송 승인으로 간주하지 않는다.
- 테스트에서는 실제 SENS 발송 API와 실제 수신번호를 사용하지 않는다.
- 실제 POST 가능성이 있는 stage `PENDING_CONFIRMATION` 또는 접수 여부가 불확실한 번호에는 어떤 이유로도 새 발송 요청을 보내지 않는다. stage `RESERVED`만 새 승인 후 같은 단계를 계속할 수 있다.
- 기존 `result.csv`를 읽고 조정하기 전에는 신규 발송을 시작하지 않는다.

## 프로젝트 입력

### 수신번호 파일

- 기준 파일은 프로젝트 루트의 `receiving_numbers.csv`다.
- 파일을 문자열 데이터로 읽고 쉼표 또는 탭 구분자를 자동 감지한다.
- `number` 열은 필수이며 `name` 열은 선택이다.
- 전화번호를 문자열로 처리하여 선행 `0`을 보존한다.
- 앞뒤 공백과 하이픈을 제거한 후 숫자만 허용한다.
- 정규화된 번호가 중복되면 한 번만 발송한다.
- 빈 값이나 잘못된 형식은 API를 호출하지 않고 다음과 같이 기록한다.

```text
delivery_id=
pipeline=LMS_THEN_MMS
delivery_status=FAILED
is_sent=false
lms_status=NOT_STARTED
lms_attempts=0
lms_request_id=
lms_message_id=
mms_status=NOT_STARTED
mms_attempts=0
mms_request_id=
mms_message_id=
error={"status":"VALIDATION_ERROR","message":"수신번호 검증 실패 사유"}
```

### MMS 이미지

- 이미지 경로는 프로젝트 루트의 `mms_img`다.
- JPG 또는 JPEG 파일이 정확히 두 개 있어야 한다.
- 각 파일은 300KB 이하, 해상도 1500×1440 이하여야 한다.
- 현재 첨부 순서는 `mms_01_intro.jpg`, `mms_02_details.jpg`다.
- 이미지는 승인된 발송 실행당 한 번씩 업로드하고 반환된 두 `fileId`를 같은 실행의 모든 단건 요청에 재사용한다.
- SENS 요청의 `files` 배열에서도 위 순서를 유지한다.
- 워크플로우가 보장할 수 있는 것은 첨부 순서뿐이다. 두 이미지의 좌우 배치는 수신 단말과 메시지 앱이 결정하므로 SENS API로 보장하지 않는다.

### 발송 본문

아래 본문을 줄바꿈까지 정확히 유지한다.

```text
[개업소연 안내]

안녕하세요.
국세청에서의 오랜 경험을 바탕으로 호연회계법인에서 새로운 출발을 하게 된 윤성중 세무사입니다.

그동안 보내주신 관심과 성원에 감사드리며, 앞으로도 많은 관심과 응원 부탁드립니다.

새로운 시작을 기쁜 마음으로 함께해 주시면 감사하겠습니다.
```

- 본문 단계의 메시지 `type`은 `LMS`이며 이미지나 `files`를 넣지 않는다.
- LMS 성공 확인 뒤 보내는 이미지 단계의 메시지 `type`은 `MMS`이며 `content=""`와 이미지 두 개만 포함한다.
- 두 단계 모두 별도 `subject`는 넣지 않는다.
- 본문을 임의로 수정하거나 문구를 자동 삽입하지 않는다.
- 워크플로우 구현 또는 수정은 `MESSAGE_BODY`가 승인된 UTF-8 본문과 바이트 단위로 일치함을 정확한 동등성 자동 테스트가 확인한 경우에만 진행할 수 있다.
- `contentType`의 광고성 여부가 확정되지 않았다면 실발송 전에 중단하고 사용자에게 확인한다.
- 광고성 메시지로 분류된다면 관련 법령과 SENS 정책을 확인하고, 필요한 표시를 갖춘 별도 승인 문안 없이는 발송하지 않는다.

## 인증과 요청 서명

- Access Key, Secret Key, SENS 서비스 ID, 등록된 발신번호는 환경변수 또는 승인된 비밀 저장소에서만 읽는다.
- 비밀값이나 생성된 서명을 코드, 문서, 테스트 데이터, 로그, 보고서 또는 `result.csv`에 기록하지 않는다.
- 공식 명세에 따라 모든 API 요청마다 현재 타임스탬프와 요청 경로를 사용해 HMAC-SHA256 서명을 생성한다.
- 시스템 시간과 API Gateway 시간 차가 인증 허용 범위를 넘지 않는지 확인한다.

## 발송 전 검증과 승인

실발송 승인 요청 전에 다음을 모두 검증한다.

1. 최신 공식 명세와 현재 요청 구조가 일치한다.
2. 인증 정보가 존재하지만 출력이나 로그에 노출되지 않는다.
3. 발신번호가 SENS에 등록된 번호다.
4. 수신번호 파일, 필수 열, 정규화, 검증, 중복 제거가 완료됐다.
5. 이미지가 정확히 두 개이며 형식, 크기, 해상도 제한을 만족한다.
6. 본문이 승인된 내용과 정확히 일치한다.
7. `contentType`이 사용자에게 확인됐다.
8. 기존 `result.csv`의 overall 및 LMS/MMS 단계별 `RESERVED`, `SENT`, `FAILED`, `PENDING_CONFIRMATION` 상태를 조정했다.

이 결과와 실제 발송 대상을 사용자에게 제시하고 해당 실행에 대한 명시적 승인을 받은 뒤에만 POST 요청을 허용한다.

## LMS 선행·이미지 MMS 후행 요청

- SENS가 한 요청에 여러 수신번호를 허용하더라도 배치 발송하지 않는다.
- 수신번호별로 독립된 POST 요청을 만든다.
- 각 요청의 `messages` 배열에는 `to` 한 건만 넣는다.
- 먼저 승인 본문을 `type=LMS`, `contentType=COMM`, `countryCode=82`, subject/files 없음으로 보낸다.
- LMS가 상관관계가 확인된 `COMPLETED + success`일 때만 `type=MMS`, `content=""`, subject 없음으로 이미지 MMS를 보낸다.
- MMS에는 두 이미지의 업로드된 `fileId`를 `files` 배열에 지정된 순서로 첨부한다.
- POST API 호출 한 번을 해당 단계의 발송 시도 한 번으로 계산한다.
- POST 응답의 `statusCode="202"`는 요청 접수 성공일 뿐 최종 전송 성공이 아니다.
- `202` 응답의 `requestId`를 해당 단계 결과에 즉시 저장한다.
- 발송 목록 조회에 단계별 `requestId`를 사용하여 수신번호의 `messageId`를 찾고 즉시 저장한다.

## 상태 모델

수신번호별 상태는 다음 세 가지를 사용한다.

| `delivery_status` | 의미 | `is_sent` | 신규 발송 |
| --- | --- | --- | --- |
| `SENT` | LMS와 MMS 모두 `COMPLETED + success` 확인 | `true` | 금지 |
| `FAILED` | 현재 승인 범위에서 어느 단계든 세 번째 명시적 실패 | `false` | 별도 사용자 승인 전 금지 |
| `PENDING_CONFIRMATION` | 접수 또는 최종 결과 미확정 | 빈 값 | 금지 |

PENDING_CONFIRMATION은 실패가 아니다. `PENDING_CONFIRMATION`인 동안 `is_sent`는 빈 값으로 유지한다. `RESERVED` 단계만 새 승인 후 같은 단계를 시작할 수 있으며, 실제 POST 가능성이 있는 `PENDING_CONFIRMATION` 단계에는 신규 발송 요청을 금지한다.

## 폴링과 재시도 상태 머신

1. 접수된 요청은 동일한 단계의 `requestId`와 `messageId`를 사용해 1초마다 결과 조회 API로 확인한다.
2. `messages[].status="READY"` 또는 `PROCESSING`이면 실패로 판단하지 않고 같은 요청을 계속 조회한다.
3. `messages[].status="COMPLETED"`이고 `messages[].statusName="success"`이면 해당 단계를 `SENT`로 체크포인트한다. LMS 성공 뒤에만 MMS를 시작하고, 두 단계 성공 뒤에만 전체 `SENT`, `is_sent=true`, `error=null`로 확정한다.
4. `COMPLETED`이고 `statusName="fail"`이면 명시적 실패로 처리한다. `statusCode`는 승인된 내부 상수 또는 ASCII 숫자 1~4자리만 보존하고 그 외에는 `UNKNOWN`으로 저장하며, 비어 있지 않은 `statusMessage`는 `redacted`로 저장한다.
5. 명시적인 POST 실패 또는 `COMPLETED + fail`일 때만 다음 POST를 허용한다.
6. 재시도 전에 10초를 기다린다.
7. 사용자가 승인한 범위에서 최초 발송을 포함해 단계별 최대 3회까지만 POST한다.
8. 한 단계의 세 번째 명시적 실패 후 해당 단계와 전체를 `FAILED`, `is_sent=false`로 확정하고 정제된 최종 상태 코드와 고정된 비식별 오류 문구를 `error`에 기록한다. MMS 실패 시 LMS 성공 기록은 보존한다.
9. 한 접수된 단계 요청이 120초 동안 종결되지 않으면 해당 단계를 `PENDING_CONFIRMATION`으로 저장하고 해당 번호의 처리를 보류한다.
10. 첫 HTTP 429는 실행 전체의 새 API 호출을 10초, 같은 실행의 두 번째 이후 429는 매번 최대 20초 멈춘다. GET 429는 transient lookup이고, 메시지 POST가 명확한 HTTP 429 응답을 받은 경우만 비접수 명시적 실패로 센다.

## 모호한 요청 결과

다음 상황은 발송 실패가 아니라 결과 미확정이다.

- POST 중 클라이언트 네트워크 타임아웃
- 연결 종료로 HTTP 응답을 받지 못함
- POST 접수 여부를 판별할 응답이 없음
- 결과 조회 API의 일시적 오류
- `requestId`는 있으나 `messageId`를 아직 찾지 못함
- `READY` 또는 `PROCESSING` 상태가 단계별 120초 확인 창을 초과함

이 경우 `PENDING_CONFIRMATION`으로 저장하고 재발송하지 않는다. `requestId` 또는 `messageId`가 있으면 이후 실행에서 같은 요청을 우선 조회한다. 식별자가 없는 모호한 POST는 요청 시각과 수신번호로 발송 목록을 조회하되 기존 요청을 유일하게 식별할 수 없으면 자동 재발송하지 않고 수동 확인 대상으로 보고한다.

## result.csv 형식

- 결과 경로는 프로젝트 루트의 `results/result.csv`다.
- UTF-8로 저장하고 정규화된 수신번호당 한 행을 유지한다.
- 열 이름과 순서는 정확히 다음과 같다.

```text
receiving_number,delivery_id,pipeline,delivery_status,is_sent,lms_status,lms_attempts,lms_request_id,lms_message_id,mms_status,mms_attempts,mms_request_id,mms_message_id,error
```

- `lms_attempts`와 `mms_attempts`는 현재 승인 범위에서 해당 단계의 실제 시작된 POST 횟수다.
- `pipeline=LMS_THEN_MMS`에서는 LMS와 MMS의 상태와 correlation ID를 각 단계 열에 독립적으로 보존한다.
- `pipeline=LEGACY_COMBINED_MMS`에서는 `lms_status=NOT_APPLICABLE`이며 과거 통합 MMS의 attempts와 correlation ID를 MMS 단계 열에 보존한다.
- `error`는 `null` 또는 `{"status":"...","message":"..."}` 형태의 JSON 문자열이다. API 제어 원문은 저장하지 않고 위 structured policy의 상태 코드 allowlist와 고정 `redacted` 문구를 적용한다.
- 전체 `SENT`는 `is_sent=true`, `error=null`이며 새 파이프라인에서는 두 단계가 모두 `SENT`다.
- 전체 `FAILED`는 `is_sent=false`이며 최종 명시적 실패의 정제된 코드와 비식별 고정 문구를 `error`에 기록한다. MMS 실패 시 LMS의 `SENT` 상태와 식별자는 보존한다.
- 전체 `PENDING_CONFIRMATION`은 `is_sent`가 빈 값이고 `error=null`이며 단계별 상태와 가능한 요청 식별자를 보존한다.
- `RESERVED` 단계는 새 승인 후 계속할 수 있지만, positive attempts와 blank correlation IDs를 가진 `PENDING_CONFIRMATION` 단계는 자동 재POST하지 않는다.
- 모든 상태 전이 직후 결과를 갱신한다.
- 기존 파일 손상을 막기 위해 같은 디렉터리의 임시 파일에 전체 내용을 쓴 뒤 원자적으로 `result.csv`를 교체한다.
- 기존 `result.csv`를 무조건 초기화하거나 성공 행을 삭제하지 않는다.

## 재실행과 별도 재발송

모든 재실행은 신규 발송보다 기존 상태 조정을 먼저 수행한다.

1. 기존 `result.csv`를 읽는다.
2. 모든 LMS/MMS `PENDING_CONFIRMATION` 요청을 해당 단계의 저장된 식별자로 다시 조회한다. `RESERVED`는 조회하지 않는다.
3. 나중에 성공한 요청은 해당 단계를 `SENT`로 갱신하고, LMS 성공 뒤 MMS 시작이 승인된 경우에만 MMS `RESERVED`로 진행한다.
4. 나중에 명시적 실패한 요청만 조건부 재시도가 사전 승인된 경우 현재 단계의 남은 횟수에 따라 재시도하거나 `FAILED`로 전환한다.
5. 여전히 미확정인 번호는 보류하고 새 POST를 금지한다.
6. `SENT` 번호는 항상 발송 대상에서 제외한다.
7. 이전 실행의 `FAILED` 번호는 사용자가 대상과 새 실행을 명시적으로 승인한 경우에만 다시 보낸다.
8. 별도 재발송 실행 전에 기존 `result.csv`를 타임스탬프가 포함된 보관 파일로 복사한다.
9. 별도 실행 대상으로 승인된 `FAILED` 번호만 새로 시작하는 단계의 attempts를 0부터 다시 계산한다. LMS 성공·MMS 실패 재발송은 LMS 기록을 유지하고 `mms_attempts`만 0부터 시작한다.
10. `PENDING_CONFIRMATION` 번호의 횟수와 상태는 초기화하지 않는다.

## 로그와 개인정보

- 수신번호와 이름을 일반 로그, 콘솔 출력, 오류 메시지 또는 완료 보고에 평문으로 노출하지 않는다.
- 번호 표시가 필요하면 마지막 네 자리만 남기고 마스킹한다.
- `result.csv`에는 결과 식별을 위해 정규화된 `receiving_number`를 저장할 수 있다.
- API 요청 본문 전체, 인증 헤더, 서명, Access Key, Secret Key는 로그에 남기지 않는다.
- 사용자에게 보고하는 API 오류에서도 비밀값과 개인정보를 제거한다.

## 테스트와 완료 보고

실발송 없이 모의 응답으로 최소한 다음 시나리오를 검증한다.

- LMS `202 → PROCESSING → COMPLETED/success` 뒤에만 MMS를 POST하고 MMS 성공 뒤 전체 `SENT`
- LMS와 MMS 각각 첫 번째 명시적 실패 후 재시도 성공
- LMS 또는 MMS에서 최초 발송 포함 세 번 모두 명시적 실패 후 전체 `FAILED`
- 단계별 120초 초과 후 `PENDING_CONFIRMATION`, 이후 재조회 성공
- POST 응답 유실 또는 네트워크 타임아웃
- 결과 조회 API의 일시적 오류
- 재실행 시 `SENT`와 실제 POST 가능성이 있는 `PENDING_CONFIRMATION`의 신규 발송 차단, `RESERVED`의 새 승인 후 재개
- worker 최대 5개, 번호별 LMS→MMS 순서, 전역 429 백오프와 공통 안전 실패 후 신규 POST 차단
- 중복 수신번호 단일 발송
- 입력 번호와 이미지 검증 실패의 사전 차단
- 8열 legacy 결과의 14열 마이그레이션, `result.csv` 복구와 원자적 교체

작업 완료 보고에는 다음만 포함한다.

- 총 대상 건수와 `SENT`, `FAILED`, `PENDING_CONFIRMATION` 건수
- `FAILED`의 비식별화된 오류 요약
- `PENDING_CONFIRMATION`의 후속 조회 필요 여부
- `result.csv`의 절대 경로
- 실발송 여부와 사용자 승인 시점

자격증명, 서명, 전체 수신번호 목록 또는 이름은 완료 보고에 포함하지 않는다.
