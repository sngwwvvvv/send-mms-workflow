# SENS 통합 MMS 워커 운영 안내

이 프로젝트는 승인된 본문과 이미지 두 장을 수신번호별 단건 MMS 요청으로 처리한다. 구현 완료, 테스트 통과, main 브랜치 병합, `preflight` 실행은 모두 실발송 승인이 아니다. `live` 실행마다 직전에 생성된 승인 토큰과 발신번호 등록 확인에 대해 별도의 명시적 승인을 받아야 한다.

## 일반 발송

```powershell
python sens_mms_cli.py preflight
$approvalToken = Read-Host 'Paste the approval_token printed by this exact preflight'
python sens_mms_cli.py live --approval-token $approvalToken --confirm-sender-registered
```

`preflight`는 네트워크 요청과 이벤트 로그 생성을 하지 않는다. 출력에서 총 대상 건수, 마스킹된 번호 표본, 등록된 발신번호, 정확한 본문, 이미지 검증 결과, `type=MMS`, `subject=null`, `contentType=COMM`, 아래 고정 설정을 확인한다. 입력, 이미지, 기존 결과 상태, 재발송 스냅샷 또는 승인 대상이 바뀌면 이전 토큰은 사용할 수 없다.

고정 실행 설정은 다음과 같으며 CLI로 변경할 수 없다.

- 워커 수: 최대 5개
- 폴링 간격: 1초
- 결과 확인 제한: 120초
- 명시적 실패 뒤 재시도 대기: 10초
- 수신번호별 최대 POST 시도: 3회
- 전역 429 대기: 첫 번째 10초, 이후 20초

`--workers`, `--poll-interval`, `--confirmation-timeout`, `--retry-delay`, `--rate-limit-delay` 같은 런타임 조정 옵션은 지원하지 않는다. 알 수 없는 옵션은 고정된 안전 차단 출력과 종료 코드 2로 종료하며 API를 호출하지 않는다.

## 통합 MMS 처리 순서

1. `live`는 프로젝트 실행 잠금을 잡고 승인 토큰과 현재 체크포인트가 일치하는지 다시 확인한다.
2. 기존 `PENDING_CONFIRMATION`을 신규 발송보다 먼저 모두 처리하고 해당 워커가 종료될 때까지 기다린다.
3. `attempts>0`이고 상관 식별자가 있는 행은 저장된 식별자로 조회한다. 명시적 실패가 확인되고 승인 실행의 시도 횟수가 남은 경우에만 재POST 대상이 된다. 양수 시도 횟수인데 식별자가 비어 있는 모호한 행은 보류하며 API를 호출하지 않는다.
4. 예외적으로 `attempts=0`, 비어 있지 않은 `delivery_id`, 빈 요청/메시지 식별자를 가진 사전 POST 예약은 같은 `delivery_id`로 이어서 최초 POST한다. 새 식별자를 부여하거나 중복 발송하지 않는다.
5. pending-first 단계 뒤 실제 POST 대상이 하나라도 남아 있을 때만 승인된 이미지 바이트를 업로드한다. `mms_01_intro.jpg`, `mms_02_details.jpg` 순서로 실행당 한 번씩 업로드하고, 두 `fileId`를 같은 순서로 모든 단건 요청에 재사용한다. pending-only 실행에는 업로드가 없다.
6. 각 수신번호는 독립된 MMS POST 하나를 사용한다. `messages`에는 수신번호 한 건만 있고, 승인된 UTF-8 본문과 `contentType=COMM`을 그대로 사용하며 `subject`를 넣지 않는다. 동시에 실행되는 수신번호 워커는 최대 5개다.

첨부 업로드가 실패하면 업로드를 재시도하지 않고 메시지 POST도 시작하지 않는다. 업로드 429도 동일하다. 모든 API 호출은 한 실행에서 공유되는 전역 429 게이트를 통과하며, 첫 429 뒤 10초, 이후 각 429 뒤 20초 동안 새 API 호출을 막는다. 명시적 전송 실패에 대한 10초 재시도 대기는 이 전역 게이트와 중복해서 더하지 않는다.

로그나 체크포인트 기록에 실패하면 이후 신규 POST를 중단한다. 접수 여부가 불확실한 요청은 `PENDING_CONFIRMATION`으로 보존하고 자동 재발송하지 않는다. `READY`와 `PROCESSING`은 실패가 아니며 같은 식별자로 계속 확인한다.

## 실패 건 별도 재발송

```powershell
python sens_mms_cli.py preflight --resend-failed
$resendApprovalToken = Read-Host 'Paste the approval_token printed by this exact resend preflight'
python sens_mms_cli.py live --resend-failed --approval-token $resendApprovalToken --confirm-sender-registered
```

재발송 사전 검증은 `results` 폴더에서 파일명 기준으로 가장 최신인 완료 스냅샷 하나를 선택한다. 선택된 스냅샷이 손상됐거나 현재 `result.csv`의 대상 행과 정확히 일치하지 않으면 이전 스냅샷으로 되돌아가지 않고 차단한다. 승인된 `FAILED` 후보만 새 `delivery_id`와 `attempts=0`으로 시작한다. 기존 `SENT`, `PENDING_CONFIRMATION`, 입력 검증 실패, 승인 대상이 아닌 행은 초기화하지 않는다. 기존 attempts-zero 예약은 재발송 모드에서도 같은 ID로 먼저 이어서 처리한다.

## 결과와 로그

- `results/result.csv`: 현재 상태 체크포인트. 상태 전이마다 원자적으로 갱신한다.
- `results/result_YYYYMMDD_HHMMSS.csv`: 정상 완료 시점의 불변 스냅샷. 같은 초 충돌 시 `_001`부터 접미사를 사용하며 덮어쓰지 않는다.
- `logs/delivery_YYYYMMDD_HHMMSS.jsonl`: 승인된 `live` 실행별 비식별 이벤트 로그. 같은 초 충돌 시 접미사를 사용하며 덮어쓰지 않는다. `preflight`는 만들지 않는다.

완료 출력은 총 대상 수와 `SENT`, `FAILED`, `PENDING_CONFIRMATION` 건수, 비식별 실패 요약, pending 후속 조회 필요 여부, `results/result.csv`의 절대 경로, 실발송 여부, 승인 시점만 포함한다. 발신번호, 전체 수신번호, 이름, 본문, delivery/request/message 식별자, 자격증명, 서명, 원시 오류, 스냅샷 경로, 로그 경로는 완료 출력에 포함하지 않는다.
