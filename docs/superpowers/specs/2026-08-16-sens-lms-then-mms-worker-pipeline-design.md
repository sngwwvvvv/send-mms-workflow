# SENS LMS 선행·이미지 MMS 후행 파이프라인 설계

작성일: 2026-08-16
상태: 대체됨 — `2026-08-17-sens-mms-worker-pipeline-design.md`로 대체

2026-08-16 구현 계획 사전 검토에서 결과 정책, `RESERVED` 재개 의미, 모듈 경계, 429 처리와 테스트 경계를 재확정했다.

## 1. 목적과 범위

현재 수신번호별 `MMS(텍스트 + 이미지 2개)` 단건 발송 파이프라인을 다음의 2단계 파이프라인으로 변경한다.

1. 승인된 본문을 LMS로 발송한다.
2. SENS가 LMS 수신 성공을 보고할 때까지 1초 간격으로 조회한다.
3. LMS 성공 확인 후에만 빈 본문과 이미지 2개로 구성된 MMS를 발송한다.
4. SENS가 MMS 수신 성공을 보고할 때까지 1초 간격으로 조회한다.
5. LMS와 MMS가 모두 성공한 경우에만 해당 수신번호의 업무를 성공으로 확정한다.

목적은 서버 측에서 MMS 요청을 LMS 수신 성공 이후로 제한하여 텍스트가 이미지 메시지보다 먼저 수신되도록 가능한 가장 강한 순서를 보장하는 것이다. SENS가 보고하는 `COMPLETED + statusName=success`를 수신 성공으로 판단한다. 단말 메시지 앱의 최종 표시 순서 자체는 SENS API로 통제할 수 없다.

이 문서는 설계만 확정한다. 구현 계획과 구현은 후속 세션의 범위다. 실제 발송은 포함하지 않는다.

## 2. 공식 API 계약

설계 기준 문서는 2026-07-23 갱신본이다.

- 메시지 발송: <https://api.ncloud-docs.com/docs/sens-sms-send>
- 메시지 발송 목록 조회: <https://api.ncloud-docs.com/docs/sens-sms-list>
- 메시지 발송 결과 조회: <https://api.ncloud-docs.com/docs/sens-sms-get>
- 첨부 파일 업로드: <https://api.ncloud-docs.com/docs/sens-sms-attachment-create>
- SENS 공통 인증 및 응답: <https://api.ncloud-docs.com/docs/sens-overview>

구현 및 매 실발송 실행 전에 최신 공식 문서를 다시 확인한다. 요청 필드, 인증, 파일 제한, 상태 코드 또는 결과 필드가 달라졌다면 값을 추정하지 않고 실발송을 중단한다.

### 2.1 LMS 요청

- `type="LMS"`
- `contentType="COMM"`
- `countryCode="82"`
- 등록된 발신번호 사용
- 프로젝트가 승인한 기존 본문을 줄바꿈과 UTF-8 바이트까지 정확히 유지
- `subject` 없음
- `files` 없음
- `messages`에는 수신번호 한 건만 포함

### 2.2 이미지 MMS 요청

- `type="MMS"`
- `contentType="COMM"`
- `countryCode="82"`
- 등록된 발신번호 사용
- `content=""`를 명시적으로 포함
- `subject` 없음
- `files`에는 `mms_01_intro.jpg`, `mms_02_details.jpg`에서 얻은 두 `fileId`를 이 순서로 포함
- `messages`에는 수신번호 한 건만 포함

공식 명세상 `content`는 필수 필드이지만 LMS/MMS에서 0~2000바이트를 허용하므로 이미지 전용 MMS도 필드를 생략하지 않고 빈 문자열로 보낸다.

## 3. 확정된 실행 설정

| 설정 | 값 |
| --- | --- |
| worker 방식 | 수신번호 전담 고정 worker pool |
| worker 수 | 5 |
| 폴링 간격 | 1초 |
| 폴링 제한 | 접수된 POST 요청당 120초 |
| 단계별 POST 최대 횟수 | 최초 시도 포함 3회 |
| 명시적 실패 재시도 간격 | 10초 |
| 이미지 업로드 | 승인된 실행당 각 이미지 1회 |

worker 하나는 수신번호 하나를 성공, 최종 실패 또는 pending 상태까지 전담한다. 한 번호 안에서 LMS와 MMS를 병렬화하지 않는다. 동시에 진행 중인 수신번호는 최대 5개다.

120초는 각 정상 접수된 POST 요청의 결과 확인 창이다. 명시적 실패 후 새 POST가 허용되면 새로 접수된 요청은 자체 120초 확인 창을 갖는다. 제한시간 초과는 실패나 재시도 허가가 아니다.

## 4. 결과 스키마

수신번호당 하나의 행을 유지한다. 새 결과 열과 순서는 다음과 같다.

```text
receiving_number,delivery_id,pipeline,delivery_status,is_sent,lms_status,lms_attempts,lms_request_id,lms_message_id,mms_status,mms_attempts,mms_request_id,mms_message_id,error
```

이 14열 스키마는 `AGENTS.md` 최상위 structured result policy와 동일한 프로젝트 표준이다. 단계별 상태와 correlation ID를 별도 보조 파일에 분리하지 않으며 한 행을 단일 원자적 체크포인트로 사용한다.

### 4.1 파이프라인 구분

`pipeline`은 다음 값만 사용한다.

- `LMS_THEN_MMS`: 새 2단계 파이프라인
- `LEGACY_COMBINED_MMS`: 기존 텍스트·이미지 통합 MMS 기록

### 4.2 단계 상태

`lms_status`와 `mms_status`는 다음 값만 사용한다.

- `NOT_STARTED`: 해당 단계가 시작되지 않음
- `RESERVED`: POST 직전 안전 체크포인트가 만들어짐
- `PENDING_CONFIRMATION`: POST 접수 또는 최종 수신 결과가 미확정
- `SENT`: `COMPLETED + success` 확인
- `FAILED`: 허용된 실행에서 세 번째 명시적 실패 확인
- `NOT_APPLICABLE`: 기존 통합 MMS 행의 LMS 단계에만 사용

`RESERVED`는 해당 예약 이후의 POST가 아직 시작되지 않았음을 뜻한다. 해당 단계의 request/message ID는 비어 있고 attempts는 이전에 실제 시작된 POST 횟수만 센다. 재실행 시 `RESERVED`는 조회 대상이 아니며, 새 승인에 같은 단계의 진행이 포함되면 안전하게 POST를 계속할 수 있다.

실제 POST 직전에는 전역 백오프와 중단 상태를 확인한 후 해당 단계를 `PENDING_CONFIRMATION`으로 바꾸고 attempts를 증가시켜 원자적으로 저장한다. 이 체크포인트 이후 프로세스가 중단되어 positive attempts와 blank correlation IDs가 남으면 실제 POST 여부가 모호하므로 자동 재POST하지 않는다.

### 4.3 전체 상태

`delivery_status`는 다음 세 값을 유지한다.

- `SENT`: 새 파이프라인에서는 LMS와 MMS 모두 `SENT`; 기존 파이프라인에서는 통합 MMS 성공
- `FAILED`: 현재 실행 단계의 세 번째 명시적 실패 또는 입력 검증 실패
- `PENDING_CONFIRMATION`: 실제 결과가 미확정이거나 다음 단계 POST 전 안전 체크포인트에 있음

`is_sent`는 업무 전체 성공 여부를 뜻한다.

- 전체 성공: `true`
- 최종 실패: `false`
- 결과 미확정: 빈 값

MMS 실패 시에도 `lms_status=SENT`가 LMS의 부분 성공을 보존한다. `error`는 `null` 또는 정제된 `{"status":"...","message":"..."}`만 저장하며 실패 단계는 LMS/MMS 상태로 식별한다.

### 4.4 대표 상태 조합

| 상황 | delivery_status | lms_status | mms_status |
| --- | --- | --- | --- |
| 새 LMS POST 전 | `PENDING_CONFIRMATION` | `RESERVED` | `NOT_STARTED` |
| LMS 결과 미확정 | `PENDING_CONFIRMATION` | `PENDING_CONFIRMATION` | `NOT_STARTED` |
| LMS 성공, MMS POST 전 | `PENDING_CONFIRMATION` | `SENT` | `RESERVED` |
| LMS 성공, MMS 결과 미확정 | `PENDING_CONFIRMATION` | `SENT` | `PENDING_CONFIRMATION` |
| LMS 세 번째 명시적 실패 | `FAILED` | `FAILED` | `NOT_STARTED` |
| MMS 세 번째 명시적 실패 | `FAILED` | `SENT` | `FAILED` |
| 전체 성공 | `SENT` | `SENT` | `SENT` |

## 5. 상태 전이

### 5.1 신규 번호

1. LMS 전송 직전 새 `delivery_id`와 LMS `RESERVED` 상태를 원자적으로 저장한다.
2. 전역 백오프 종료와 중단 상태를 확인한다.
3. POST 시도 직전에 `lms_status=PENDING_CONFIRMATION`으로 바꾸고 `lms_attempts`를 증가시켜 체크포인트한 뒤 시작 이벤트를 기록한다.
4. 전역 중단 상태를 다시 확인한 뒤 LMS POST를 수행한다.
5. LMS POST가 접수되면 `lms_request_id`를 즉시 저장한다.
6. 목록 조회로 `lms_message_id`를 찾는 즉시 저장한다.
7. LMS가 `COMPLETED + success`이면 `lms_status=SENT`로 체크포인트한다.
8. MMS 전송 직전 `mms_status=RESERVED`를 체크포인트한다.
9. MMS도 같은 규칙으로 status, attempts, request/message ID를 저장한다.
10. 두 단계가 모두 `SENT`일 때만 전체 `SENT`, `is_sent=true`, `error=null`로 확정한다.

### 5.2 LMS 실패 또는 pending

- 명시적 POST 실패 또는 상관관계가 확인된 `COMPLETED + fail`만 LMS 재POST를 허용한다.
- 10초 후 재시도하며 최초 시도 포함 최대 3회다.
- 세 번째 명시적 실패 후 전체 `FAILED`로 확정하고 MMS는 보내지 않는다.
- POST 응답 유실, 모호한 2xx, 조회 미확정 또는 120초 초과는 LMS `PENDING_CONFIRMATION`으로 보존하며 MMS를 보내지 않는다.

### 5.3 MMS 실패 또는 pending

- LMS 성공 기록과 식별자는 변경하지 않는다.
- 명시적 실패만 10초 후 MMS 재POST를 허용하며 최대 3회다.
- 세 번째 명시적 실패 후 `delivery_status=FAILED`, `lms_status=SENT`, `mms_status=FAILED`로 확정한다.
- 모호한 결과 또는 120초 초과는 `delivery_status=PENDING_CONFIRMATION`, `lms_status=SENT`, `mms_status=PENDING_CONFIRMATION`으로 보존한다.

## 6. 재실행과 단계별 재개

모든 재실행은 신규 발송보다 기존 상태 조정을 먼저 수행한다.

1. 기존 결과를 읽고 검증한다.
2. 모든 LMS/MMS pending 요청을 저장된 식별자로 최대 5개씩 병렬 조회한다.
3. 조회 결과와 현재 결과를 원자적으로 반영한다.
4. 승인된 범위의 새 POST 작업만 시작한다.

재개 규칙은 다음과 같다.

- LMS/MMS reserved: 해당 단계의 POST가 아직 시작되지 않았으므로 새 승인 범위 안에서 같은 단계부터 계속한다.
- LMS pending: 기존 LMS만 조회하고 MMS는 보내지 않는다.
- MMS pending: 기존 MMS만 조회하고 LMS와 MMS 모두 재POST하지 않는다.
- positive attempts와 blank correlation IDs를 가진 pending: 모호한 POST로 보존하고 자동 재POST하지 않는다.
- LMS 실패 재발송: 별도 승인과 새 `delivery_id` 후 LMS부터 시작한다.
- LMS 성공·MMS 실패 재발송: 별도 승인과 새 `delivery_id` 후 LMS 기록은 유지하고 `mms_attempts`만 0부터 시작한다.
- pending 조회 결과가 명시적 실패가 되고 남은 시도가 있는 경우, 사전 승인 화면이 동일 단계의 조건부 재시도를 명시적으로 포함했을 때만 재POST한다.
- `SENT`는 항상 모든 신규 발송 대상에서 제외한다.

## 7. 기존 결과 마이그레이션

기존 열 형식은 원본을 훼손하지 않고 새 형식으로 한 번만 이관한다.

legacy 발송 행의 기존 `attempts`, `request_id`, `message_id`는 각각 `mms_attempts`, `mms_request_id`, `mms_message_id`로 옮기고 기존 `delivery_id`와 정제된 `error`는 그대로 보존한다. LMS 관련 attempts와 식별자는 0과 빈 문자열로 초기화한다. 이 매핑은 과거 단일 요청이 통합 MMS였다는 사실을 조작하지 않고 유지하기 위한 것이다.

- 기존 `SENT`: `pipeline=LEGACY_COMBINED_MMS`, `lms_status=NOT_APPLICABLE`, `mms_status=SENT`로 보존하고 재발송하지 않는다.
- 기존 `PENDING_CONFIRMATION`: `pipeline=LEGACY_COMBINED_MMS`, `lms_status=NOT_APPLICABLE`, `mms_status=PENDING_CONFIRMATION`으로 이관하고 기존 식별자로만 조회한다.
- 기존 발송 `FAILED`: 승인 전에는 legacy 실패로 보존한다. 새 재발송 승인을 받을 때 `LMS_THEN_MMS`로 전환하고 LMS부터 시작한다.
- 기존 번호 검증 실패: 양 단계 `NOT_STARTED`, attempts 0으로 보존하고 API를 호출하지 않는다.
- 기존 legacy pending이 나중에 실패로 확정되면, 별도 승인된 새 파이프라인 재발송에서 LMS부터 시작한다.

이관은 원자적으로 수행하고 기존 결과 및 스냅샷을 삭제하거나 덮어쓰지 않는다. 기존 데이터가 새 상태로 유일하게 해석되지 않으면 추정하지 않고 실행을 차단한다.

## 8. 동시성 구조

### 8.1 실행 순서

```text
실행 잠금 및 승인 토큰 검증
  -> 기존 pending 병렬 재조회
  -> 승인된 불변 이미지 바이트를 각 1회 업로드
  -> 수신번호 작업 큐 생성
  -> 전담 worker 5개 실행
       -> LMS 발송 및 조회
       -> LMS 성공 체크포인트
       -> 이미지 MMS 발송 및 조회
  -> 결과 스냅샷 및 완료 보고
```

이미지 업로드는 단말 전달이 아니므로 LMS보다 먼저 수행해도 수신 순서에 영향을 주지 않는다. 선행 업로드가 실패하면 새 LMS POST도 시작하지 않아 완료 불가능한 부분 실행을 방지한다. 최종 이미지 사전 검증에서 읽은 동일한 불변 바이트만 업로드한다.

### 8.2 구성 요소

- `sens_mms/results.py`: 14열 행 검증, 원자적 저장, 8열 legacy 마이그레이션
- `sens_mms/api.py`: 별도 `send_lms`/`send_mms` 요청과 조회 응답 분류
- `sens_mms/coordination.py`: 아래 네 조정자를 제공
  - `CheckpointCoordinator.commit(row)`: worker의 상태 변경을 직렬화하고 CSV를 원자적으로 교체
  - `EventCoordinator.write(...)`: JSONL 순번과 기록을 직렬화
  - `RunSafetyCoordinator.stop()`/`raise_if_stopped()`: 공통 안전 실패를 모든 worker에 전파
  - `GlobalBackoff.before_call()`/`record_429()`: 실행 전체 API 호출 백오프를 조정
- `sens_mms/pipeline.py`: `RecipientPipeline.run(row, approved_action, file_ids)`로 한 수신번호의 LMS→MMS 상태 전이를 전담
- `sens_mms/workflow.py`: 승인 검증, pending 선조회, 조건부 이미지 업로드, 고정 worker pool과 완료 집계
- 고정 worker pool: 동시에 최대 5개 수신번호만 처리

SENS 클라이언트와 업로드된 두 `fileId`는 실행 동안 읽기 전용으로 공유한다.

각 worker 입력은 다음 필드를 가진 불변 승인 작업으로 구성한다.

- `initial_action=RECONCILE_LMS|RECONCILE_MMS|START_LMS|START_MMS`
- `allow_followup_mms: bool`: LMS 성공 뒤 같은 승인으로 MMS 예약과 POST를 시작할 수 있는지 여부
- `allow_retry_on_explicit_failure: bool`: 조회 또는 POST의 명시적 실패 뒤 같은 단계를 남은 횟수 안에서 재POST할 수 있는지 여부

`initial_action`만으로 후속 POST 권한을 추정하지 않는다. `allow_followup_mms=false`인 LMS 조회/발송은 LMS 성공을 체크포인트한 뒤 멈추며, `allow_retry_on_explicit_failure=false`인 명시적 실패는 재POST 없이 보존한다. LMS 성공 전에는 어떤 승인 조합에서도 MMS 실행 경로에 진입하지 않는다. 이 세 필드는 단계별 대상과 함께 승인 토큰에 포함한다.

### 8.3 전역 중단 규칙

- 각 POST 직전에 전역 중단 상태와 최신 체크포인트를 확인한다.
- 어느 worker에서든 로그 또는 체크포인트 실패가 발생하면 전역 중단 신호를 설정한다.
- 중단 신호 이후에는 다른 worker도 새로운 POST를 시작하지 않는다.
- 이미 진행 중인 요청은 불확실한 재발송을 만들지 않도록 가능한 상태를 보존한다.
- 한 수신번호의 정상적인 명시적 실패는 다른 번호의 실행을 중단하지 않는다.
- 실행 잠금, 로그, 결과 저장과 같은 공통 안전 기반 실패는 전체 실행을 중단한다.

## 9. 폴링, 오류 및 백오프

### 9.1 폴링

- request ID로 message ID를 찾는 목록 조회와 message ID 결과 조회 모두 1초 간격이다.
- 모든 실제 send/list/get 응답은 반복된 `READY`와 `PROCESSING`을 포함하여 기록한다.
- 상관관계는 정확한 수신번호, request ID, message ID로 확인한다.
- API가 제어하는 잘못된 상관관계 값이나 malformed envelope는 해당 조회의 고정된 일시 오류로 처리한다.

### 9.2 결과 분류

| API 결과 | 분류 |
| --- | --- |
| 정확한 202와 유효한 request ID | 접수 성공 |
| POST 타임아웃 또는 응답 유실 | 모호한 결과, pending |
| 2xx지만 status/request ID가 잘못됨 | 모호한 결과, pending |
| 명시적인 비접수 POST 응답 | 명시적 실패, 재시도 가능 |
| 조회 네트워크 오류, 5xx 또는 malformed 응답 | 실패 횟수에 포함하지 않고 확인 창 안에서 재조회 |
| READY 또는 PROCESSING | 1초 후 재조회 |
| 상관관계가 확인된 COMPLETED + success | 단계 성공 |
| 상관관계가 확인된 COMPLETED + fail | 명시적 실패, 재시도 가능 |
| 120초 초과 | pending |

### 9.3 429 전역 백오프

HTTP 429가 발생하면 worker별 독립 재시도로 요청이 몰리지 않도록 실행 전체에 백오프를 적용한다.

- 같은 실행의 첫 제한: 10초
- 같은 실행의 두 번째 이후 제한: 매번 최대 20초
- 백오프 중에는 모든 worker의 새 POST/GET 시작을 잠시 멈춘다.
- GET의 429는 확인 창 안의 일시 조회 오류로 처리한다.
- 메시지 POST가 HTTP 429 응답을 명확히 받은 경우에는 공식 공통 응답 정의에 따라 비접수로 분류하고 명시적 실패 시도에 포함한다. HTTP 응답 자체가 없는 타임아웃은 429로 추정하지 않고 모호한 결과로 처리한다.
- API 제어 메시지 원문은 저장하지 않고 `redacted`로 정제한다.

## 10. 사전 승인

사전 승인 화면은 다음을 표시한다.

- 전체 대상 수
- LMS부터 시작할 신규/실패 재발송 수
- LMS pending 재조회 수
- LMS 성공 후 MMS만 전송할 수
- MMS pending 재조회 수
- 각 범주의 마스킹된 번호 표본
- 등록 확인할 발신번호
- 정확한 LMS 본문
- MMS 본문이 빈 문자열이고 subject가 없다는 사실
- 두 이미지의 파일명, 크기, 해상도, SHA-256 검증 결과와 순서
- `LMS -> 성공 확인 -> MMS` 순서
- worker 5개, 폴링 1초, 요청당 제한 120초, 재시도 10초, 단계별 최대 3회
- `contentType`
- pending이 명시적 실패로 확인될 경우 동일 단계 조건부 재시도 여부

승인 토큰은 위 메시지 내용, 이미지 불변 바이트 해시, 실행 설정, 단계별 대상과 현재 결과 상태를 포함한다. 입력, 설정 또는 결과 상태가 바뀌면 토큰은 무효다. 매 실발송 실행 직전에 새 승인을 받아야 한다.

## 11. 로그와 개인정보

- 로그는 전체 수신번호 대신 `delivery_id`와 allowlist 값인 `stage=LMS|MMS`를 사용한다.
- 발신/수신번호, 이름, 본문, subject, 파일 ID, 서비스 ID, 자격증명, 서명, 인증 헤더와 원시 요청/응답 바디를 기록하지 않는다.
- 모든 실제 send/list/get 응답을 의미별 allowlist로 정제하여 기록한다.
- 상태 메시지와 오류 메시지는 비어 있지 않으면 `redacted`로 바꾼다.
- correlation ID는 exact built-in string만 보존한다.
- API 값 sanitization과 이벤트 로그의 기존 total-sanitizer 정책을 유지한다.
- 결과 및 이벤트 기록은 worker 간 직렬화하며 기존 파일을 덮어쓰지 않는다.

## 12. 테스트 전략

실제 SENS 발송 API나 실제 수신번호를 사용하지 않고 모의 응답과 제어 가능한 clock으로 검증한다.

### 12.1 API 요청 계약

- LMS 요청이 `type=LMS`, 승인 본문, subject/files 없음, 단일 수신번호를 정확히 사용
- MMS 요청이 `type=MMS`, `content=""`, subject 없음, 파일 두 개의 순서, 단일 수신번호를 정확히 사용
- LMS 본문 UTF-8 바이트 단위 동등성
- 두 API의 서명 URI와 응답 형식 검증

### 12.2 상태 머신

- LMS `202 -> PROCESSING -> COMPLETED/success` 후 MMS 성공
- LMS 성공 전 MMS POST가 절대 발생하지 않음
- LMS 첫 명시적 실패 후 10초 뒤 재시도 성공
- LMS 세 번 실패 후 MMS 미발송 및 전체 실패
- LMS 120초 초과 후 pending 및 MMS 미발송
- LMS pending 후속 조회 성공 시 승인 범위 안에서 MMS 진행
- MMS 첫 명시적 실패 후 재시도 성공
- MMS 세 번 실패 시 LMS 성공 보존
- MMS pending 후속 실행에서 LMS/MMS 재POST 없이 기존 MMS 조회
- MMS 실패 별도 재발송에서 LMS를 건너뛰고 MMS attempts만 0부터 시작
- 각 단계의 POST 응답 유실, 네트워크 타임아웃, 모호한 2xx
- 조회 일시 오류 후 확인 창 안에서 복구
- 정확한 1초 폴링과 120초 경계

### 12.3 동시성과 장애

- 활성 수신번호가 5개를 넘지 않음
- worker별 LMS→MMS 순서가 교차 실행 중에도 유지됨
- 동시 체크포인트에서 결과 행이 유실되지 않음
- JSONL 순번 및 레코드가 섞이거나 손상되지 않음
- 한 worker의 로그/체크포인트 실패 후 모든 신규 POST 차단
- 일반 수신 실패는 다른 번호 처리를 중단하지 않음
- 429의 10초/20초 전역 백오프와 요청 억제
- 동일 초 로그·스냅샷 충돌 시 기존 파일 미덮어쓰기

### 12.4 입력, 이미지, 재실행

- 중복 번호는 한 파이프라인만 실행
- 입력 번호 및 이미지 검증 실패의 사전 차단
- 이미지를 사전 검증에서 한 번만 읽고 동일 바이트로 각 1회 업로드
- 모든 worker가 동일한 두 file ID를 순서대로 공유
- 기존 결과 스키마의 안전하고 반복 가능한 마이그레이션
- legacy SENT 및 모든 pending의 중복 발송 차단
- 결과 CSV 원자적 교체와 손상 복구
- 승인 토큰 이후 입력, 이미지, 설정 또는 결과 변경 시 실행 차단

### 12.5 테스트 모듈과 완료 기준

- `tests/test_results.py`: 14열 불변식, 모든 대표 상태 조합, 8열 legacy 마이그레이션과 원자적 저장
- `tests/test_coordination.py`: 동시 체크포인트, JSONL sequence, 전역 중단, 10초/20초 429 백오프
- `tests/test_pipeline.py`: LMS→MMS 순서, 단계별 재시도/pending, `RESERVED` 재개와 승인 범위 차단
- `tests/test_api.py`: LMS/MMS 요청 바디, UTF-8 본문 동등성, 빈 MMS content와 파일 순서, total sanitizer 회귀
- `tests/test_workflow.py`, `tests/test_cli.py`: worker 최대 5개, pending 선조회, 조건부 업로드, 승인 토큰과 비식별 완료 보고

동시성 테스트는 실제 대기 대신 thread-safe 가짜 clock, barrier와 scripted transport를 사용한다. 실제 SENS API, 실제 수신번호와 `.env`를 사용하지 않는다. 완료 기준은 `python -m unittest discover -s tests -v` 전체 통과와 모의 transport 기록을 통한 POST 순서 및 최대 동시 실행 수 검증이다.

## 13. 완료 보고

완료 보고는 다음 정보만 포함한다.

- 총 대상 건수
- 전체 `SENT`, `FAILED`, `PENDING_CONFIRMATION` 건수
- `FAILED`의 비식별 오류 요약
- pending 후속 조회 필요 여부
- `results/result.csv` 절대 경로
- 결과 스냅샷과 이벤트 로그 경로
- 실발송 여부와 사용자 승인 시점

자격증명, 서명, 전체 수신번호 목록, 이름, request/message ID 또는 메시지 본문은 완료 보고에 포함하지 않는다.

## 14. 범위 밖 항목

- 실제 MMS/LMS 발송
- 단말 메시지 앱의 표시 순서 보장
- worker 수의 동적 자동 조절
- 5개를 초과하는 활성 수신번호 스케줄링
- 광고성 여부 또는 승인 본문 자체의 변경
- 구현 계획과 코드 변경
