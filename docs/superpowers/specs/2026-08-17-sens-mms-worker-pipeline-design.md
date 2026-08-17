# SENS 통합 MMS worker 파이프라인 설계

작성일: 2026-08-17  
상태: 확정  
대체 문서: `2026-08-16-sens-lms-then-mms-worker-pipeline-design.md`

## 1. 목적과 범위

기존 수신번호별 `MMS(텍스트 + 이미지 2개)` 단건 발송을 유지하면서 직렬 처리만 최대 5개의 수신번호 전담 worker pool로 바꾼다. 앞서 설계한 LMS 선행·이미지 MMS 후행 2단계 파이프라인은 구현하지 않는다.

이번 변경에서 유지하는 실험 설정은 다음과 같다.

| 설정 | 값 |
| --- | --- |
| worker 방식 | 수신번호 전담 고정 worker pool |
| worker 수 | 5 |
| 폴링 간격 | 1초 |
| 폴링 제한 | 접수된 POST 요청당 120초 |
| POST 최대 횟수 | 최초 시도 포함 3회 |
| 명시적 실패 재시도 간격 | 10초 |
| 전역 HTTP 429 백오프 | 첫 429는 10초, 두 번째 이후는 매번 고정 20초 |
| 이미지 업로드 | 승인된 실행당 각 이미지 1회 |

실제 SENS 발송은 범위에 포함하지 않는다. 구현, 모의 테스트, main 병합은 실발송 승인이 아니다.

## 2. 공식 API 계약

2026-08-17 확인한 공식 문서는 `release/20260723`이며 통합 MMS 요청 계약과 충돌하는 변경은 확인되지 않았다.

- 메시지 발송: <https://api.ncloud-docs.com/docs/sens-sms-send>
- 메시지 발송 목록 조회: <https://api.ncloud-docs.com/docs/sens-sms-list>
- 메시지 발송 결과 조회: <https://api.ncloud-docs.com/docs/sens-sms-get>
- 첨부 파일 업로드: <https://api.ncloud-docs.com/docs/sens-sms-attachment-create>
- SENS 공통 인증 및 응답: <https://api.ncloud-docs.com/docs/sens-overview>

각 수신번호의 발송 요청은 다음 계약을 사용한다.

- `type="MMS"`
- 사전 승인된 `contentType`
- `countryCode="82"`
- SENS에 등록된 발신번호
- 승인된 `MESSAGE_BODY`를 기본 `content`로 그대로 사용
- `subject` 없음
- `messages=[{"to": "<한 수신번호>"}]`
- `files`에 `mms_01_intro.jpg`, `mms_02_details.jpg`에서 얻은 두 `fileId`를 이 순서로 포함

SENS가 한 요청에 최대 100개의 메시지를 허용하더라도 배치 발송하지 않는다. 한 POST는 한 수신번호만 포함한다. 구현 및 매 실발송 실행 직전에 공식 문서를 다시 확인하고 계약 차이가 있으면 값을 추정하지 않고 실발송을 중단한다.

## 3. 결과 스키마와 상태 모델

결과는 기존 8열 스키마를 유지한다.

```text
receiving_number,delivery_id,delivery_status,is_sent,attempts,request_id,message_id,error
```

LMS 단계, `pipeline`, 단계별 상태 또는 단계별 correlation ID 열은 추가하지 않는다. 구현되지 않은 14열 LMS 실험 스키마의 마이그레이션도 만들지 않는다.

전체 상태는 다음 세 값만 사용한다.

- `SENT`: 상관관계가 확인된 `COMPLETED + success`; `is_sent=true`, `error=null`
- `FAILED`: 현재 승인 실행에서 세 번째 명시적 실패; `is_sent=false`, 정제된 `error`
- `PENDING_CONFIRMATION`: 접수 여부 또는 최종 결과가 미확정; `is_sent`는 빈 값, `error=null`

새 번호는 POST 전에 새 `delivery_id`, `delivery_status=PENDING_CONFIRMATION`, `attempts=0`, 빈 request/message ID를 원자적으로 저장해 예약한다. 이 조합은 아직 POST가 시작되지 않은 예약에만 허용된다.

프로세스가 이 예약 체크포인트 뒤 실제 POST 시도 체크포인트 전에 종료되면, 다음 `live` 실행의 새로운 승인 토큰이 동일한 행과 실행 설정을 다시 결합한 경우에만 기존 `delivery_id`로 재개한다. 이것은 `PENDING_CONFIRMATION`에 새 POST를 허용하는 유일한 예외다. `attempts>0`이고 correlation ID가 비어 있는 행은 POST 시작 여부가 모호하므로 자동 재개하지 않는다.

실제 POST 직전에는 `attempts`를 증가시키고 같은 pending 행을 원자적으로 저장한다. 이 체크포인트 뒤 프로세스가 중단되어 positive attempts와 빈 correlation ID가 남으면 POST 여부가 모호하므로 자동 재POST하지 않는다.

모든 상태 전이와 correlation ID 확보 직후 결과를 갱신한다. 결과 파일은 같은 디렉터리의 임시 파일을 거쳐 원자적으로 교체하고 기존 결과, 스냅샷 또는 이벤트 로그를 자동 삭제하지 않는다.

## 4. 구성 요소와 경계

기존 모듈 책임을 유지하면서 동시성 조정만 분리한다.

- `sens_mms/results.py`: 8열 행 검증, 원자적 저장, 복구와 스냅샷
- `sens_mms/api.py`: 통합 MMS 발송, 첨부 파일 업로드, 목록/결과 조회, 의미별 응답 정제
- `sens_mms/coordination.py`: worker가 공유하는 결과/로그 직렬화, 전역 안전 중단, 모든 API 호출의 전역 429 게이트를 담당하는 조정자
- `sens_mms/pipeline.py`: 한 수신번호의 `attempts=0` 예약 재개, MMS POST, 상관관계 확인, 폴링과 재시도 상태 전이
- `sens_mms/workflow.py`: 승인 검증, pending-first 단계, 조건부 이미지 업로드, 원자적 일괄 예약, 최대 5 worker 실행, 완료 집계
- `sens_mms/preflight.py`: 8열 현재 상태를 승인 작업으로 고정하고 승인 토큰에 대상, 이미지 해시, 본문과 고정 실행 설정을 결합

`RecipientPipeline`은 한 번에 한 수신번호만 담당한다. 결과 CSV와 JSONL 로그를 직접 동시에 쓰지 않고 조정자를 통해 직렬화한다. SENS 클라이언트와 업로드된 두 `fileId`는 실행 동안 읽기 전용으로 공유한다.

## 5. 실행 데이터 흐름

실행 순서는 다음과 같다.

1. 프로젝트 live-execution lock을 획득하고 최신 입력, 결과, 불변 이미지 바이트와 고정 실행 설정으로 승인 토큰을 다시 검증한다.
2. 기존 8열 결과를 읽고 `PENDING_CONFIRMATION`을 분류한다. correlation ID가 있는 행은 새 발송보다 먼저 최대 5개씩 병렬 조회하고, `attempts=0`과 빈 ID인 행은 승인된 재개 대상으로, positive attempts와 빈 ID인 행은 모호한 보류 대상으로 둔다.
3. pending 조회 future를 모두 합류시키고 최신 결과를 다시 읽어 검증한다. 체크포인트, 로그 또는 예상하지 못한 worker 실패가 있었다면 이후 모든 새 API 호출을 중단한다.
4. 최신 결과에 실제 새 MMS POST 대상이 하나라도 있을 때만 사전 검증에서 보존한 불변 이미지 바이트를 각각 한 번 업로드한다.
5. 신규 및 승인된 실패 재발송 행은 새 `delivery_id`로, 승인된 `attempts=0` 재개 행은 기존 `delivery_id`로 worker 시작 전에 한 번에 원자적으로 예약한다. 전체 예약이 성공하지 않으면 어떤 worker도 시작하지 않는다.
6. 최대 5개의 worker가 각 수신번호의 통합 MMS를 성공, 최종 실패 또는 pending까지 전담한다.
7. 모든 future를 합류시킨 뒤 불변 스냅샷에서 완료 집계를 계산한다. 전역 안전 실패가 발생한 실행은 정상 완료 스냅샷이나 `WORKFLOW_COMPLETED`를 만들지 않는다.

pending-only 실행에서는 이미지를 업로드하지 않는다. pending 조회가 명시적 실패로 끝나 승인된 조건부 재POST 대상이 생긴 경우에만 이미지 업로드 단계로 진행한다. 이미지 업로드가 실패하거나 429를 받으면 전역 백오프 상태를 기록하되 그 업로드를 같은 실행에서 자동 재시도하지 않고, 새 MMS POST를 시작하지 않는다. 최종 이미지 승인 토큰 검증 뒤에는 경로를 다시 열지 않으며 메타데이터, 해시, 업로드 Base64는 모두 같은 불변 바이트에서 파생한다.

## 6. 수신번호별 상태 머신

### 6.1 POST와 상관관계

worker는 전역 백오프 종료와 중단 상태를 확인하고, attempts 증가 체크포인트와 `SEND_ATTEMPT_STARTED` 이벤트를 기록한 뒤 전역 중단 상태를 다시 확인하고 POST한다.

- HTTP 2xx이면서 body `statusCode`가 정확한 ASCII 세 자리 exact string일 때만 확정적으로 분류한다.
- 정확한 `statusCode="202"`는 non-empty exact string `requestId`도 있어야 접수 성공이다.
- 접수된 `requestId`는 즉시 저장한다.
- 목록 조회에서 요청 ID, 수신번호, `type=MMS`가 정확히 일치하는 유일한 메시지의 non-empty exact string `messageId`만 채택하고 즉시 저장한다.
- 목록/결과 envelope 또는 correlation ID가 잘못되면 원문을 보존하지 않고 고정된 일시 조회 오류로 처리한다.

### 6.2 폴링과 종료

접수 성공 시 즉시 목록/결과 조회를 시작하고, 이후 1초 간격으로 요청별 120초 확인 창 안에서 반복한다.

- `READY` 또는 `PROCESSING`: 같은 요청을 계속 조회하고 모든 실제 응답을 안전하게 기록
- `COMPLETED + success`: `SENT` 확정
- 상관관계가 확인된 `COMPLETED + fail`: 명시적 실패
- 조회 네트워크 오류, 5xx, GET 429 또는 malformed 응답: 시도 횟수를 늘리지 않고 확인 창 안에서 재조회
- 120초 초과: `PENDING_CONFIRMATION` 보존, 재POST 금지

120초 확인 창은 전역 429 대기 중에도 계속 흐른다. 백오프로 조회 기회가 줄어 확인 창이 끝난 경우에도 실패로 바꾸지 않고 다음 실행에서 같은 correlation ID를 조회한다.

### 6.3 명시적 실패와 모호한 결과

명시적인 비접수 POST 실패 또는 상관관계가 확인된 `COMPLETED + fail`만 재POST를 허용한다. 재시도 가능 시각은 실패 후 10초로 정하고, 전역 429 게이트가 더 늦게 열리면 두 시간을 더하지 않고 더 늦은 시각까지 한 번만 기다린다. 최초 시도 포함 최대 3회까지만 POST하고 세 번째 명시적 실패 후 `FAILED`로 확정한다.

POST 타임아웃, 연결 종료, 응답 유실, malformed 2xx, 유효한 request ID 없는 `202`는 모호한 결과다. 이를 `PENDING_CONFIRMATION`으로 보존하고 자동 재발송하지 않는다.

`attempts=0`과 빈 correlation ID를 가진 사전 POST 예약은 모호한 결과가 아니다. 동일한 행과 고정 실행 설정이 새 승인 토큰에 포함된 경우에만 기존 `delivery_id`로 재개한다.

## 7. 동시성, 전역 안전과 429

고정 `ThreadPoolExecutor(max_workers=5)`를 pending-first 조회 단계와 새 발송 단계에 각각 사용한다. 한 수신번호의 POST와 폴링은 같은 worker 안에서 순차 실행한다. 동시에 진행되는 수신번호는 최대 5개이며 worker 수를 live CLI 옵션으로 변경할 수 없다.

공유 조정자는 다음 규칙을 강제한다.

- 결과 변경 전체를 잠근 뒤 최신 행을 재검증하고 원자적으로 저장
- JSONL sequence와 쓰기를 직렬화
- 체크포인트 또는 로그 실패 시 전역 중단 상태 설정
- 각 신규 POST 직전에 전역 중단 상태 재확인
- 모든 upload/send/list/get 호출이 같은 전역 429 게이트를 통과
- 체크포인트 또는 로그가 실패하면 이미 접수된 요청도 새로운 조회를 시작하지 않고 마지막 내구 체크포인트를 다음 실행에서 이어서 확인
- 한 번호의 정상적인 명시적 실패는 다른 번호를 중단하지 않음

HTTP 429는 실행 전체에 적용한다.

- 첫 429 후 upload/send/list/get을 포함한 모든 새 API 호출을 10초 보류
- 같은 실행의 두 번째 이후 429는 매번 고정 20초 보류
- 새 429를 관찰할 때마다 전역 게이트 시각을 현재 게이트와 새 대기 만료 시각 중 더 늦은 값으로 갱신
- GET 429는 확인 창 안의 일시 조회 오류
- MMS POST가 명확한 HTTP 429 응답을 받은 경우만 비접수 명시적 실패
- 응답이 없는 타임아웃은 429로 추정하지 않고 모호한 결과
- MMS POST의 10초 재시도 대기와 429 전역 대기는 합산하지 않고 더 늦은 만료 시각만 적용

## 8. 승인과 개인정보

preflight는 전체 대상 수, pending 조회 수, `attempts=0` 안전 재개 수, 모호한 보류 수, 신규/실패 재발송 수, 마스킹된 번호 표본, 등록 확인할 발신번호, 정확한 본문, 이미지 파일명·크기·해상도·SHA-256과 순서, `MMS`, `contentType`과 다음 고정 실행 설정을 표시한다.

```text
worker_count=5
poll_interval_seconds=1
confirmation_timeout_seconds=120
retry_delay_seconds=10
max_attempts=3
rate_limit_delays_seconds=[10,20]
```

승인 토큰은 본문 UTF-8 바이트, 이미지 불변 바이트 해시, 파일 순서, 실행 설정, 대상과 현재 결과 상태를 포함한다. 입력이나 결과 상태가 바뀌면 토큰은 무효다. 광고성 여부가 확정되지 않았거나 최신 공식 계약과 차이가 있으면 실발송을 중단한다.

이벤트 로그는 `delivery_id`만 사용하며 전체 수신번호, 이름, 발신번호, 본문, subject, file ID, service ID, 자격증명, 서명, 인증 헤더 또는 원시 요청/응답을 기록하지 않는다. API 메시지는 기존 total-sanitizer 정책에 따라 빈 값 또는 고정 `redacted`만 보존한다.

## 9. 테스트 전략

실제 SENS API나 실제 수신번호를 사용하지 않고 모의 transport, thread-safe 가상 clock과 명시적인 barrier/event로 검증한다. 실제 wall-clock sleep이나 우연한 thread scheduling에 의존하는 동시성 테스트를 만들지 않는다.

테스트 책임은 다음처럼 분리한다.

- `tests/test_coordination.py`: 결과/로그 직렬화, sequence, 전역 안전 중단, 공유 429 게이트
- `tests/test_pipeline.py`: 한 수신번호의 예약 재개, POST, 상관관계, 폴링, 재시도와 최종 상태
- `tests/test_workflow.py`: pending-first barrier, 조건부 업로드, 일괄 예약, 최대 5 worker와 완료 합류
- 기존 API, 결과, preflight, CLI, 이벤트 로그 테스트: 요청 계약, 승인, 개인정보와 내구성 회귀

- 통합 MMS 요청의 정확한 type, 본문, subject 부재, 단일 수신번호, 두 file ID 순서
- `MESSAGE_BODY`와 승인된 본문의 UTF-8 바이트 단위 동등성
- `202 -> PROCESSING -> COMPLETED/success`
- 첫 명시적 실패 뒤 10초 후 재시도 성공
- 세 번의 명시적 실패 뒤 `FAILED`
- 120초 초과 pending과 이후 재조회 성공
- `attempts=0` 예약의 새 승인 후 기존 delivery ID 재개와 positive attempts/빈 ID의 재POST 차단
- POST 응답 유실, malformed 2xx와 correlation ID 없음의 무재발송
- 조회 transient 오류와 반복 READY/PROCESSING의 전체 이벤트 기록
- pending-first 완료 전 이미지 업로드 및 신규 POST 금지
- pending-only 실행의 이미지 업로드 생략, 조건부 재POST가 생길 때만 업로드, 업로드 실패·429 시 모든 새 POST 차단
- worker 최대 동시성 5, 수신번호별 전담 처리와 결과/로그 직렬화
- 전역 429 첫 10초·이후 고정 20초, 최신 만료 시각 병합, 120초 창 포함과 GET/POST 분류
- 체크포인트·이벤트 실패 후 모든 later API 호출 차단
- 예약, attempts 체크포인트, request ID, message ID 및 최종 상태의 crash boundary
- 8열 결과 검증, 원자적 교체, 복구, 스냅샷과 개인정보 회귀

격리 worktree에서 전체 테스트를 두 번 실행해 순서 의존 전역 상태가 없음을 확인한다.

## 10. 문서 전환과 main 병합

기존 LMS→MMS 설계와 구현 계획은 삭제하지 않고 새 설계로 대체되었음을 표시한다. 실험 worktree와 branch 이름은 의사결정 이력과 기존 격리를 보존하기 위해 유지한다. 현재 실험 worktree의 14열 LMS 정책을 담은 `AGENTS.md`는 구현 전에 통합 MMS, 8열 결과, `attempts=0` 예약 재개 예외 및 고정 worker/timing 설정으로 교체한다. 운영 정책, README와 새 구현 계획도 같은 계약에 맞춘다.

구현과 검증은 `C:\workflows\send-mms-workflow-lms-then-mms`의 `experiment/lms-then-mms-worker-pipeline`에서 수행한다. 전체 모의 테스트 두 번, diff 검사와 코드 리뷰가 통과하면 사용자가 2026-08-17 명시한 승격 지시에 따라 main에 `--no-ff`로 병합한다. main에서도 전체 테스트를 두 번 다시 실행한다.

병합 뒤에도 실험 branch/worktree, 결과, 스냅샷 또는 로그를 자동 삭제하지 않는다. main 병합은 실제 MMS 발송 승인이 아니며 실발송에는 실행 직전 별도 승인이 필요하다.

## 11. 완료 조건

- LMS POST와 LMS 단계 상태가 실행 경로, 결과, 승인 또는 로그에 존재하지 않는다.
- 각 대상은 승인 본문과 이미지 두 개를 포함한 MMS POST 한 건 단위로 처리된다.
- 결과는 정확한 8열 형식을 유지한다.
- worker 5개와 1초/120초/10초/3회/429 10·20초 설정이 자동 테스트로 고정된다.
- 새 승인을 받은 `attempts=0` 예약만 안전하게 재개하고 positive attempts/빈 correlation ID는 자동 재POST하지 않는다.
- pending-first, ambiguous no-resend, 전역 중단과 개인정보 정책이 유지된다.
- 격리 worktree와 main에서 전체 모의 테스트가 각각 두 번 통과한다.
- main 병합 후에도 실제 SENS 발송은 수행되지 않는다.
