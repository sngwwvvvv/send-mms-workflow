# SENS MMS 운영 안내

이 프로젝트는 승인된 MMS 워크플로우를 사전 검증하고, 각 실발송 실행을 독립적으로 기록한다. 구현·테스트·`preflight` 수행은 실발송 승인이 아니다. `live`를 실행할 때마다 그 실행 직전에 출력된 승인 토큰과 발신번호 등록 확인을 다시 받아야 한다.

## 일반 발송

```powershell
python sens_mms_cli.py preflight
$approvalToken = Read-Host 'Paste the approval_token printed by this exact preflight'
python sens_mms_cli.py live --approval-token $approvalToken --confirm-sender-registered
```

`preflight` 출력에서 대상 건수, 마스킹된 번호 표본, 발신번호, 정확한 본문, 이미지 검증 결과, `MMS`/`COMM` 설정을 확인한다. 입력이나 승인 대상이 바뀌면 이전 토큰은 사용할 수 없다.

## 실패 건 별도 재발송

```powershell
python sens_mms_cli.py preflight --resend-failed
$resendApprovalToken = Read-Host 'Paste the approval_token printed by this exact resend preflight'
python sens_mms_cli.py live --resend-failed --approval-token $resendApprovalToken --confirm-sender-registered
```

재발송 사전 검증은 유효성 여부와 무관하게 `results` 폴더에서 파일명 기준으로 가장 최신인 완료 스냅샷 하나를 선택한다. 선택된 최신 파일이 손상됐거나 현재 체크포인트와 일치하지 않으면 더 오래된 파일로 되돌아가지 않고 즉시 차단한다. 재발송도 해당 사전 검증에서 새로 발급된 토큰으로 별도 승인해야 한다.

## 결과와 로그

- `results/result.csv`는 현재 상태 체크포인트다. 상태 전이마다 원자적으로 갱신된다.
- `results/result_YYYYMMDD_HHMMSS.csv`는 정상 완료 시점의 불변 스냅샷이다. 같은 초에 이름이 겹치면 접미사가 붙고 기존 파일을 덮어쓰지 않는다.
- `logs/delivery_YYYYMMDD_HHMMSS.jsonl`은 승인된 `live` 실행 한 번에 대응하는 이벤트 로그다. 같은 초 충돌 시 접미사가 붙고 기존 로그를 덮어쓰지 않는다. `preflight`는 이 로그를 만들지 않는다.
- 같은 프로젝트에서 두 `live` 실행이 겹치지 않도록 전체 실행 동안 OS 기반 잠금을 유지한다. 이미 실행 중이면 두 번째 실행은 이벤트 기록이나 파일 업로드·발송 API 전에 고정된 안전 오류로 차단된다.
- 한 승인 실행 안에서 명시적 실패를 재시도할 때는 같은 `delivery_id`를 유지한다.
- 나중에 별도로 승인한 실패 건 재발송은 새 `delivery_id`를 부여한다. `SENT`, `PENDING_CONFIRMATION`, 입력 검증 실패 및 승인 대상이 아닌 행은 초기화하지 않는다.
- 로그 또는 체크포인트 기록에 실패하면 이후 신규 POST를 중단한다. 접수 여부가 불확실한 요청은 자동으로 다시 보내지 않는다.
- 발송 POST의 2xx 응답이라도 `statusCode`가 정확히 3자리 ASCII 숫자 문자열이 아니면 접수 여부 불확정으로 보존하며 자동 재시도하지 않는다. 정확한 `202`는 비어 있지 않은 문자열 `requestId`도 필요하다. 조회 응답의 `requestId`/`messageId` 형식이 잘못된 경우에도 해당 조회 전체를 일시 오류로 처리하고 현재 상태를 보존한다.
- 최종 사전 검증은 각 이미지를 한 번 읽어 크기·해상도·해시와 승인 토큰을 만들고 그 불변 바이트 사본을 업로드한다. 승인 토큰 확인 뒤 이미지 경로를 다시 열지 않으므로 디스크 파일이 바뀌어도 승인되지 않은 바이트가 업로드되지 않는다.

실발송 완료 출력은 건수와 비식별화된 오류 범주, 후속 조회 필요 여부, 결과·스냅샷·로그 경로 및 승인 시점만 제공한다. 전체 번호, 이름, 요청 식별자, 메시지 식별자, 인증정보와 본문은 완료 출력에 포함하지 않는다.
