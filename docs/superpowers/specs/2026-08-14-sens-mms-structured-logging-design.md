# SENS MMS 구조화 로깅 및 실행별 결과 보관 설계

## 목적

수신번호별 SENS 발송 요청 응답, 통신사·단말 발송 결과 응답, 재시도 이력과 실행 최종 집계를 감사 가능한 형태로 보존한다. 로그에서는 수신번호와 비밀값을 노출하지 않고, 16자리 `delivery_id`를 이용해 `result.csv`의 수신번호 행과 연결한다.

이 설계는 로깅과 결과 보관 방식만 정의한다. 실제 MMS 발송은 구현·테스트·사전 검증과 별개이며, 매 실발송 직전에 사용자의 새롭고 명시적인 승인이 필요하다.

## 공식 API 기준

설계 시 2026-08-14 기준 최신 공식 문서를 확인했다.

- [메시지 발송](https://api.ncloud-docs.com/docs/sens-sms-send)
- [메시지 발송 목록 조회](https://api.ncloud-docs.com/docs/sens-sms-list)
- [메시지 발송 결과 조회](https://api.ncloud-docs.com/docs/sens-sms-get)
- [첨부 파일 업로드](https://api.ncloud-docs.com/docs/sens-sms-attachment-create)
- [SENS 개요](https://api.ncloud-docs.com/docs/sens-overview)

발송 POST의 `202`는 요청 접수 성공이다. 최종 성공은 결과 조회 응답의 `messages[].status="COMPLETED"`와 `messages[].statusName="success"`가 함께 확인된 경우에만 인정한다. 결과 조회 응답에는 본문, 발신번호, 수신번호와 첨부 정보가 포함될 수 있으므로 원본 응답 전체를 로그에 기록하지 않는다.

## 디렉터리와 파일 책임

```text
results/
├── result.csv
├── .legacy_result_migrated.json
├── result_20260814_153012.csv
└── result_20260815_091455.csv

logs/
├── delivery_20260814_151820.jsonl
└── delivery_20260815_090030.jsonl
```

- `results/result.csv`는 실행 중 복구를 위한 권위 있는 현재 상태 체크포인트다. 모든 상태 전이 직후 같은 디렉터리의 임시 파일을 거쳐 원자적으로 교체한다.
- `results/result_{timestamp}.csv`는 정상 종료된 실행의 최종 스냅샷이다. `timestamp`는 Asia/Seoul 기준 워크플로우 완료 시각이며 형식은 `YYYYMMDD_HHMMSS`다.
- 같은 초에 스냅샷 이름이 충돌하면 `_001`, `_002` 순번을 붙이고 기존 파일을 덮어쓰지 않는다.
- `logs/delivery_{timestamp}.jsonl`은 실행별 추가 전용 이벤트 로그다. 로그 파일의 `timestamp`는 실행 시작 시각이다.
- 같은 초에 로그 이름이 충돌하면 결과 스냅샷과 동일하게 `_001`, `_002` 순번을 붙이고 기존 로그를 덮어쓰지 않는다.
- 결과와 로그 파일은 자동 삭제하지 않는다.

## 결과 CSV 스키마

열 이름과 순서는 다음과 같이 확장한다.

```text
receiving_number,delivery_id,delivery_status,is_sent,attempts,request_id,message_id,error
```

`receiving_number`은 다음 실행에서 정확한 수신번호와 상태를 식별하기 위해 결과 CSV에만 평문으로 저장한다. JSONL 로그에는 기록하지 않는다.

### `delivery_id`

- 암호학적으로 안전한 난수 생성기를 이용해 생성한다.
- 정확히 16자리이며 `23456789ABCDEFGHJKLMNPQRSTUVWXYZ` 문자 집합을 사용한다.
- 수신번호, 이름, 현재 시각 또는 다른 개인정보로부터 파생하지 않는다.
- 현재 체크포인트와 기존 결과 스냅샷에 존재하는 값과 충돌하면 다시 생성한다.
- 하나의 승인된 발송 작업에서 최초 POST와 최대 세 번의 재시도는 같은 ID를 사용한다.
- 이전 실행에서 최종 `FAILED`가 된 번호를 별도 승인으로 다시 발송할 때는 새 ID를 생성한다.
- 기존 `SENT` 행은 마이그레이션 호환성을 위해 빈 ID를 허용한다.
- 기존 `PENDING_CONFIRMATION` 행을 다시 조회할 때는 API 조회 전에 새 ID를 부여한다.
- 기존 `FAILED` 행은 별도 재발송이 승인될 때 새 ID를 부여한다.

### 첫 POST 전 안전 예약

최초 POST 전에 다음 체크포인트를 저장한다.

```text
delivery_status=PENDING_CONFIRMATION
attempts=0
delivery_id=<생성된 ID>
request_id=
message_id=
error=null
```

POST 호출을 시작할 때 `attempts`를 1로 갱신한다. 예약 저장 후 프로세스가 중단되면 자동 재발송하지 않고 수동 확인 대상으로 남긴다. 이는 중복 발송 방지를 위해 가용성보다 안전을 우선하는 처리다. 기존 상태 규칙은 이 예약 상태에 한해 `PENDING_CONFIRMATION + attempts=0`을 허용하도록 함께 개정해야 한다.

## JSONL 이벤트 스키마

모든 이벤트는 한 줄에 하나의 JSON 객체로 기록한다. 모든 행에는 다음 공통 필드가 존재한다.

| 필드 | 의미 |
| --- | --- |
| `schema_version` | 로그 구조 버전. 최초 버전은 정수 `1`이다. |
| `sequence` | 해당 로그 파일 안의 기록 순서. 실행마다 1부터 시작하고 1씩 증가한다. |
| `logged_at` | Asia/Seoul 오프셋이 포함된 ISO 8601 기록 시각이다. |
| `event` | 이벤트 종류다. |
| `delivery_id` | 수신번호별 승인 발송 작업 ID다. 실행 수준 이벤트에서는 `null`이다. |
| `attempt` | 현재 POST 시도 번호다. 적용되지 않으면 `null`이다. |

API 관련 이벤트는 필요에 따라 `api`, `http_status`, `request_id`, `message_id`, `response`, `error`를 추가한다. 빈 값은 누락하지 않고 `null`로 기록해 파싱을 단순화한다.

`RESULT_STATE_CHANGED`에는 `delivery_id`, 변경된 상태, `is_sent`, `attempts`, `request_id`, `message_id`와 비식별화된 오류만 기록한다. 결과 CSV 행 전체를 직렬화하지 않으며 `receiving_number`는 포함하지 않는다.

### 이벤트 종류

```text
WORKFLOW_STARTED
DELIVERY_ID_ASSIGNED
SEND_ATTEMPT_STARTED
SEND_RESPONSE
SEND_AMBIGUOUS_OUTCOME
MESSAGE_LIST_RESPONSE
DELIVERY_POLL_RESPONSE
API_LOOKUP_ERROR
RETRY_SCHEDULED
RESULT_STATE_CHANGED
RESULT_SNAPSHOT_WRITTEN
WORKFLOW_COMPLETED
WORKFLOW_ABORTED
```

`DELIVERY_POLL_RESPONSE`는 이전 응답과 같은 `READY` 또는 `PROCESSING` 상태가 반복되더라도 실제로 받은 모든 30초 폴링 응답을 기록한다.

### API별 응답 허용 목록

원본 응답을 로거에 직접 넘기거나 직렬화하지 않는다. 중앙의 정제 함수가 다음 허용 필드만 새 객체에 복사한다.

- 발송 POST: HTTP 상태, `requestId`, `requestTime`, `statusCode`, `statusName`
- 발송 목록 조회 응답 본문: `statusCode`, `statusName`, `pageSize`, `pageIndex`, `itemCount`, `hasMore`
- 목록의 메시지 또는 최종 결과 메시지: `requestId`, `messageId`, `requestTime`, `completeTime`, `telcoCode`, `status`, `statusCode`, `statusName`, `statusMessage`
- 네트워크 및 파싱 오류: 내부 오류 분류와 비식별화된 메시지

다음 값은 기록하지 않는다.

- 전체 수신번호와 이름
- 발신번호
- 제목과 메시지 본문
- `fileId`, 첨부 파일 응답 상세와 파일 내용
- Access Key, Secret Key, 생성된 서명과 인증 헤더
- 서비스 ID가 포함된 실제 API URI
- 전체 API 요청 또는 응답 바디

`statusMessage`와 오류 메시지에 4자리 이상의 연속 숫자가 있으면 마지막 네 자리만 남기고 마스킹한다.

### 이벤트 예시

```json
{"schema_version":1,"sequence":12,"logged_at":"2026-08-14T15:21:30.428+09:00","event":"DELIVERY_POLL_RESPONSE","delivery_id":"7K3M9Q2X8C4N6P1R","attempt":2,"api":"GET_MESSAGE_RESULT","http_status":200,"request_id":"RSMA-...","message_id":"f574...","response":{"statusCode":"200","statusName":"success","message":{"completeTime":"2026-08-14 15:21:30","telcoCode":"SKT","status":"COMPLETED","statusCode":"0","statusName":"success","statusMessage":"성공"}},"error":null}
```

## 실행 데이터 흐름

### 신규 발송

1. 기존 `results/result.csv`를 읽고 `PENDING_CONFIRMATION`을 먼저 조정한다.
2. 입력, 이미지, 본문, `contentType`, 공식 API 요청 구조와 승인 토큰을 검증한다.
3. JSONL 파일을 만들고 `WORKFLOW_STARTED`를 안전하게 기록한다.
4. 수신번호별 `delivery_id`를 생성하고 첫 POST 전 안전 예약 체크포인트를 기록한다.
5. `DELIVERY_ID_ASSIGNED`와 `SEND_ATTEMPT_STARTED`를 기록한다.
6. SENS 발송 POST의 모든 응답 또는 모호한 결과를 기록한다.
7. `requestId`로 목록을 조회한 모든 응답을 기록하고 `messageId`를 얻는 즉시 결과 CSV에 저장한다.
8. 같은 `messageId`로 30초마다 조회하며 모든 응답을 기록한다.
9. 상태가 바뀔 때마다 결과 체크포인트와 `RESULT_STATE_CHANGED`를 기록한다.
10. 명시적 실패일 때만 30초 후 같은 ID의 다음 `attempt`를 허용한다.

### 재시도와 미확정 결과

- 같은 승인 실행 안의 재시도는 같은 `delivery_id`와 증가한 `attempt`를 사용한다.
- `READY`, `PROCESSING`, 조회 오류, 응답 없는 POST와 접수 여부가 모호한 결과는 재시도하지 않는다.
- `COMPLETED + fail` 또는 명시적 POST 실패만 다음 POST를 허용한다.
- 10분을 초과하면 `PENDING_CONFIRMATION`으로 남기고 해당 번호의 처리를 종료한다.
- 세 번째 명시적 실패는 `FAILED`로 확정한다.

### 별도 승인 재발송

1. `results/result_*.csv` 이름을 파싱해 완료 타임스탬프와 충돌 순번이 가장 큰 파일을 선택한다.
2. 파일명, 정확한 헤더, 각 행의 상태 불변식, 중복 번호와 인코딩을 검증한다.
3. 최신 파일이 손상됐으면 이전 파일로 자동 후퇴하지 않고 중단해 사용자에게 보고한다.
4. 최신 파일의 모든 발송 이력상 `FAILED` 행을 재발송 후보로 구성한다. `attempts=0`이고 `error.status="VALIDATION_ERROR"`인 입력 검증 실패 행은 API 재발송 후보가 아니며 계속 제외한다.
5. 대상 건수와 마스킹 표본 등 필수 사전 검증 결과를 제시하고 새로운 명시적 승인을 받는다.
6. 승인된 각 후보에 새 `delivery_id`를 부여하고 `attempts=0`부터 새 실행으로 처리한다.
7. `PENDING_CONFIRMATION`과 `SENT` 행은 대상에서 제외한다.

### 정상 종료

1. `results/result.csv`의 최종 상태를 원자적으로 저장한다.
2. 완료 시각으로 불변 결과 스냅샷을 생성하고 기존 파일을 덮어쓰지 않는다.
3. `RESULT_SNAPSHOT_WRITTEN`을 기록한다.
4. 전체 대상과 `SENT`, `FAILED`, `PENDING_CONFIRMATION` 건수를 `WORKFLOW_COMPLETED`에 기록한다.

`WORKFLOW_COMPLETED.summary`의 수치는 생성된 결과 스냅샷을 다시 읽어 계산한 값과 일치해야 한다.

## 로그 및 저장 실패 처리

- 실행 로그 파일을 생성하거나 `WORKFLOW_STARTED`를 기록하지 못하면 API 호출 전에 중단한다.
- 각 JSONL 행은 append 후 `flush`와 `fsync`로 즉시 디스크에 반영한다.
- 결과 체크포인트 저장에 실패하면 신규 POST를 중단한다.
- API 응답 후 로그 기록에 실패하면 가능한 결과 상태를 먼저 체크포인트에 보존하고 남은 신규 발송을 중단한다.
- 이미 접수됐거나 접수 여부가 모호한 요청은 로깅 실패를 이유로 재발송하지 않는다.
- 예상하지 못한 오류가 발생하면 가능한 경우 `WORKFLOW_ABORTED`를 기록한다. 완료 스냅샷은 생성하지 않고 현재 체크포인트를 유지한다.
- `result.csv`와 JSONL 사이의 다중 파일 쓰기는 원자적일 수 없으므로, 안전 판단에서는 결과 체크포인트를 권위 있는 데이터로 사용하고 로그는 감사 이력으로 사용한다.

## 기존 `result.csv` 마이그레이션

현재 루트의 `result.csv`는 다음 절차로 이전한다.

1. 에이전트나 일반 셸이 내용을 출력하지 않고 승인된 프로그램이 파일 구조와 행 불변식을 검증한다.
2. `results/result.csv`가 없을 때만 확장된 스키마로 원자적 복사한다.
3. 기존 행에는 빈 `delivery_id`를 추가한다.
4. 원본 루트 파일은 마이그레이션 보관본으로 그대로 유지한다.
5. 복사가 성공하면 `results/.legacy_result_migrated.json`에 원본 파일의 SHA-256과 마이그레이션 스키마 버전만 원자적으로 기록한다. 수신번호나 결과 행은 표식 파일에 기록하지 않는다.
6. 이후 실행에서 두 결과 파일과 유효한 표식이 함께 있고 루트 원본 해시가 표식과 일치하면 이미 완료된 마이그레이션으로 간주해 `results/result.csv`를 사용한다.
7. 두 결과 파일이 있지만 표식이 없거나 원본 해시가 달라졌다면 자동 병합하거나 덮어쓰지 않고 중단한다.

이전이 끝난 뒤 모든 실행은 `results/result.csv`만 현재 체크포인트로 사용한다.

## 테스트 설계

실제 SENS API와 실제 수신번호를 사용하지 않고 주입된 모의 API, 시계, 난수 생성기와 파일 시스템을 사용한다.

### 식별자

- ID가 정확히 16자리 허용 문자로 생성된다.
- ID가 개인정보로부터 파생되지 않는다.
- 같은 실행의 모든 재시도에서 ID가 유지된다.
- 별도 승인 재발송에서 새 ID가 생성된다.
- 결과 CSV와 모든 관련 JSONL 행의 ID가 일치한다.

### API 응답 로그

- 발송 POST 응답, 목록 조회 응답과 결과 조회 응답이 각각 올바른 이벤트로 기록된다.
- 동일한 `READY`와 `PROCESSING` 폴링 응답도 모두 기록된다.
- 명시적 실패 후 재시도가 동일 ID와 증가한 `attempt`로 연결된다.
- 네트워크 타임아웃, 응답 파싱 실패와 조회 오류가 안전한 오류 이벤트로 기록된다.
- 모든 행에 `schema_version=1`과 1부터 연속 증가하는 `sequence`가 있다.

### 개인정보와 비밀값

- 로그에 전체 수신번호, 이름, 본문, 발신번호, 파일 ID와 서비스 ID가 없다.
- 로그와 예외에 자격증명, 서명과 인증 헤더가 없다.
- 상태 메시지 안의 번호 형태 문자열이 마스킹된다.
- 알 수 없는 응답 필드는 허용 목록에 없으므로 기록되지 않는다.

### 결과 파일과 재발송 선택

- 모든 상태 전이가 `results/result.csv`에 원자적으로 반영된다.
- 정상 종료 시 완료 시각 스냅샷이 생성된다.
- 같은 초의 파일명 충돌은 순번으로 구분된다.
- 파일명상 가장 최근인 스냅샷 후보가 선택되고, 검증을 통과한 경우에만 사용된다.
- 최신 스냅샷이 손상됐을 때 이전 파일로 후퇴하지 않는다.
- 최신 스냅샷의 모든 발송 실패 `FAILED`가 후보가 되지만 입력 검증 실패 행은 제외되고, 새 승인 전 POST는 0회다.
- 최종 JSONL 집계가 결과 스냅샷 집계와 일치한다.

### 장애 복구와 마이그레이션

- 로그 생성 실패 시 POST는 0회다.
- 로그 기록이 실행 도중 실패하면 이후 신규 POST를 막는다.
- 첫 POST 전 `attempts=0` 안전 예약이 저장된다.
- 중단 후 재실행은 예약 또는 미확정 번호에 새 POST를 보내지 않는다.
- 기존 루트 결과 파일이 새 스키마로 복사되고 원본이 보존된다.
- 완료 표식이 있는 정상 마이그레이션은 다음 실행에서도 새 체크포인트를 사용한다.
- 표식이 없거나 원본 해시가 다른 두 결과 파일은 자동 병합하지 않는다.

## 구현 범위와 정책 변경

구현 시 최소한 다음 파일을 변경하거나 추가한다.

- `AGENTS.md`: 결과 경로, 확장된 열, 안전 예약 상태, JSONL 개인정보 정책과 완료 보고 경로를 반영한다.
- `sens_mms/results.py`: 새 열, 결과 디렉터리, 스냅샷, 최신 파일 선택과 마이그레이션을 구현한다.
- `sens_mms/api.py`: API별 안전한 구조화 응답을 반환하도록 인터페이스를 확장한다.
- `sens_mms/workflow.py`: ID 수명, 모든 응답 이벤트, 재시도와 최종 집계를 연결한다.
- 새 로깅 모듈: 허용 목록 기반 정제와 내구성 있는 JSONL 기록을 전담한다.
- CLI 및 테스트: 신규·재발송 모드, 공개 출력, 장애와 개인정보 검증을 확장한다.

구현 완료 후 전체 모의 테스트와 안전성 검사를 통과하고 실제 입력에 대해 preflight만 수행한다. 실제 발송은 새 승인 전에는 수행하지 않는다.
