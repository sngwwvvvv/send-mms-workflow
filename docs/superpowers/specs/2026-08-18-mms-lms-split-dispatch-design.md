# Design Spec: MMS 2단계 분리 발송 (MMS 사진 -> 통신사 확인 -> LMS 본문)

- **Date**: 2026-08-18
- **Topic**: MMS/LMS Split Dispatch Refactoring
- **Status**: Draft for Review

---

## 1. 개요 및 배경 (Background & Motivation)

### 1.1 배경 및 문제 정의
현재 시스템은 수신번호별로 단일 MMS 요청(이미지 2장 + 안내 본문 텍스트)을 발송합니다.
그러나 특정 이동통신사 및 단말기 환경에서 MMS 수신 시 다음과 같은 UX 저하 문제가 발생하고 있습니다:
- **수신 순서 뒤바뀜 현상**: `[사진 1건 수신] -> [텍스트 본문 수신] -> [나머지 사진 1건 수신]`의 형태로 메시지 블록이 쪼개져 수신되는 현상 발생.

### 1.2 목표 (Goals)
수신자별 발송 로직을 2단계로 강제 분리하여 사진과 본문이 항상 올바른 순서로 수신되도록 보장합니다:
1. **1단계 (MMS)**: 첨부 이미지 2장만(본문 빈값) 발송하고 통신사 완료(`COMPLETED + success`)를 확인.
2. **2단계 (LMS)**: 1단계 성공이 최종 확인된 직후, 기존 승인된 본문 전문(`MESSAGE_BODY`)을 LMS로 즉시 발송하고 최종 완료(`COMPLETED + success`) 확인.

---

## 2. 핵심 설계 원칙 및 규칙 준수

1. **`AGENTS.md` 규약 완벽 준수**:
   - `results/result.csv` 스키마(`receiving_number,delivery_id,delivery_status,is_sent,attempts,request_id,message_id,error`) 유지.
   - 상태 모델: `SENT`, `FAILED`, `PENDING_CONFIRMATION` 3가지를 엄격히 유지.
   - 개인정보 및 민감정보(전체 전화번호, 인증키, 본문 원문 등) 로그/콘솔 노출 절대 금지.
2. **단계별 독립 재시도 및 타임아웃**:
   - 각 단계(MMS, LMS)는 독립적으로 `max_attempts=3`, `retry_delay=10s`, `confirmation_timeout=120s`, `poll_interval=1s` 적용.
3. **엄격한 단계 의존성(Fail-Safe)**:
   - 1단계(MMS)가 `COMPLETED + success`로 확정되지 않으면(실패, 타임아웃, 모호한 상태 등), 2단계(LMS)는 절대 발송하지 않음.
4. **멱등적 재조정(Reconciliation)**:
   - 시스템 중단 후 재실행 시, SENS API의 `message_type`(`MMS` 또는 `LMS`)을 확인하여 이미 MMS가 성공한 경우 MMS 재발송 없이 LMS 단계부터 안전하게 이어받음.

---

## 3. 데이터 흐름 및 상태 머신 (Workflow & State Machine)

```mermaid
flowchart TD
    Start([수신자 작업 시작]) --> CheckReconcile{조정 작업인가?}
    
    %% 복구 경로
    CheckReconcile -- 예 (미확정 복구) --> QueryMsg[SENS 메시지 조회]
    QueryMsg --> CheckMsgType{조회된 message_type?}
    CheckMsgType -- MMS 성공 --> StageLMS[2단계: LMS 발송 시작]
    CheckMsgType -- MMS 실패/진행중 --> HandleMMSRetry[MMS 재시도 또는 보류]
    CheckMsgType -- LMS 성공 --> MarkSent[최종 SENT 완료]
    CheckMsgType -- LMS 실패/진행중 --> HandleLMSRetry[LMS 재시도 또는 보류]
    
    %% 신규 발송 경로
    CheckReconcile -- 아니오 (신규/재발송) --> StageMMS[1단계: MMS 발송 (사진 2장, content=" ")]
    
    %% Stage 1: MMS
    subgraph Stage 1: MMS Stage (최대 3회 시도)
        StageMMS --> PostMMS[POST MMS 요청]
        PostMMS --> PollMMS[1초 간격 get_message 폴링]
        PollMMS --> MMSResult{MMS 결과}
        MMSResult -- COMPLETED + success --> StageMMSDone[MMS 성공 확정]
        MMSResult -- COMPLETED + fail --> MMSRetryCheck{MMS 시도 < 3?}
        MMSRetryCheck -- 예 --> WaitMMS[10초 대기] --> PostMMS
        MMSRetryCheck -- 아니오 --> FailMMS[FAILED 확정, 종료]
        MMSResult -- 120초 타임아웃/모호 --> PendingMMS[PENDING_CONFIRMATION, 종료]
    end
    
    StageMMSDone --> StageLMS
    
    %% Stage 2: LMS
    subgraph Stage 2: LMS Stage (최대 3회 시도)
        StageLMS --> PostLMS[POST LMS 요청]
        PostLMS --> PollLMS[1초 간격 get_message 폴링]
        PollLMS --> LMSResult{LMS 결과}
        LMSResult -- COMPLETED + success --> FinalSENT[SENT / is_sent=true 확정]
        LMSResult -- COMPLETED + fail --> LMSRetryCheck{LMS 시도 < 3?}
        LMSRetryCheck -- 예 --> WaitLMS[10초 대기] --> PostLMS
        LMSRetryCheck -- 아니오 --> FailLMS[FAILED 확정, 종료]
        LMSResult -- 120초 타임아웃/모호 --> PendingLMS[PENDING_CONFIRMATION, 종료]
    end
```

---

## 4. 컴포넌트별 상세 변경 사항

### 4.1 `sens_mms/api.py` (API 클라이언트)
- **`send_mms(to, file_ids, *, content_type)`**:
  - `type="MMS"`, `files=[{"fileId": ...}, ...]`, `content=" "` 구성으로 POST 호출.
- **`send_lms(to, *, content_type)`**:
  - `type="LMS"`, `files=[]`, `content=MESSAGE_BODY` 구성으로 POST 호출.
- **`send_one`**: 하위 호환성을 유지하거나 내부적으로 `send_mms` / `send_lms`를 호출하도록 리팩토링.
- **`MessageRecord`**: SENS API가 반환하는 `type`(`MMS` 또는 `LMS`) 필드 파싱 및 정제 보장.

### 4.2 `sens_mms/pipeline.py` (수신자 파이프라인)
- **`_execute_stage(stage: Literal["MMS", "LMS"], ...)` 상태 머신**:
  - 단일 스테이지 실행 루프 추상화.
  - POST 전 `PENDING_CONFIRMATION` 체크포인트 갱신.
  - SENS `requestId` 획득 -> 목록 조회로 `messageId` 획득 -> 1초 간격 `get_message` 폴링.
  - `COMPLETED + success`: 스테이지 성공 반환.
  - `COMPLETED + fail`: 10초 대기 후 최대 3회 재시도, 3회 초과 시 `FAILED`.
  - 120초 초과: `PENDING_CONFIRMATION` 반환.
- **`run(row, work, file_ids)` 메인 오케스트레이션**:
  - 1단계 `MMS` 스테이지 실행.
  - 1단계 성공 시, `attempts`를 리셋하고 2단계 `LMS` 스테이지 실행.
  - 2단계 성공 시 `SENT`, `is_sent=true`, `error=null`로 최종 체크포인트 커밋.
- **`_reconcile(row, work)` 조정 로직**:
  - 기존의 `PENDING_CONFIRMATION` 항목을 `message_id` 또는 `request_id`로 조회.
  - 조회된 메시지의 `message_type == "MMS"`이고 `COMPLETED + success`인 경우: MMS 완료 처리 후 곧바로 LMS 스테이지 실행.
  - 조회된 메시지의 `message_type == "LMS"`인 경우: LMS 상태에 따라 완료/재시도 처리.

### 4.3 `sens_mms/event_log.py` (JSONL 이벤트 로그)
- **`stage` 필드 추가**:
  - 모든 수신자 관련 이벤트(`SEND_ATTEMPT_STARTED`, `SEND_RESPONSE`, `DELIVERY_POLL_RESPONSE` 등)에 `stage` (`"MMS"` 또는 `"LMS"`) 속성 기록.
- **단계 완료 이벤트(`STAGE_COMPLETED`) 추가**:
  - 1단계 MMS 성공 시 `STAGE_COMPLETED` (`stage="MMS"`)를 기록하여 추적성 확보.
- **개인정보 및 보안 정제**:
  - 기존의 총체적 마스킹 및 화이트리스트 검증 규칙 100% 유지.

### 4.4 `sens_mms/cli.py` (CLI 및 실시간 터미널 출력)
- 수신번호 뒷 4자리만 표시하는 마스킹 규칙(`010-****-1234`)을 적용하여 터미널에 단계별 진행 상황을 실시간 출력:
  - `[MMS 1단계] 010-****-1234: 사진 발송 요청 (시도 1/3)`
  - `[MMS 1단계] 010-****-1234: 통신사 성공 확인 (COMPLETED)`
  - `[LMS 2단계] 010-****-1234: 본문 발송 요청 (시도 1/3)`
  - `[LMS 2단계] 010-****-1234: 최종 전송 완료 [SENT]`
  - 실패 시: `[MMS 1단계] 010-****-1234: 실패 (LMS 취소) [FAILED]`

---

## 5. 엣지 케이스 및 오류 처리 (Edge Cases & Safety)

| 상황 | 동작 방식 | 최종 상태 | 비고 |
| --- | --- | --- | --- |
| MMS 1회 성공 + LMS 1회 성공 | 정상 완료 | `SENT` (`is_sent=true`) | 최적 정상 경로 |
| MMS 1회 실패 후 재시도 성공 + LMS 성공 | MMS 재시도 후 LMS 정상 진행 | `SENT` (`is_sent=true`) | 자가 복구 |
| MMS 3회 연속 명시적 실패 | LMS 발송 즉시 차단 | `FAILED` (`is_sent=false`) | MMS 실패 에러 기록 |
| MMS 120초 타임아웃 / 모호한 응답 | LMS 발송 차단, 보류 | `PENDING_CONFIRMATION` | 수동 또는 후속 재조회 |
| MMS 성공 + LMS 3회 연속 실패 | MMS는 발송됨, LMS 최종 실패 | `FAILED` (`is_sent=false`) | LMS 실패 에러 기록 |
| MMS 성공 + LMS 120초 타임아웃 | 보류 처리 | `PENDING_CONFIRMATION` | 후속 실행 시 LMS부터 재조회 |
| 발송 중 프로세스 강제 종료 (MMS 성공 직후) | 재실행 시 SENS 조회 결과가 `MMS` 성공임을 확인 | LMS부터 이어서 발송 | MMS 중복 발송 방지 |

---

## 6. 테스트 및 검증 계획 (Verification Plan)

- **단위 테스트 (`tests/test_pipeline.py`, `tests/test_api.py`)**:
  - `send_mms`와 `send_lms` 요청 페이로드 구조 검증 (MMS는 `files` 포함 & `content=""`, LMS는 `files` 없음 & `content=MESSAGE_BODY`).
  - MMS 성공 후 즉시 LMS 호출 및 최종 `SENT` 전이 검증.
  - MMS 실패 시 LMS 호출 차단 검증.
  - LMS 실패 시 LMS만 재시도(최대 3회) 검증.
  - `MessageRecord.message_type` 기반 중단 복구(Reconciliation) 시나리오 검증.
- **워크플로우 통합 테스트 (`tests/test_workflow.py`)**:
  - 복수 수신자 대상 동시 실행 및 스레드 풀 격리 검증.
  - 이미지 1회 업로드 및 fileId 재사용 검증.
  - 이벤트 로그(`stage="MMS"`, `stage="LMS"`) 완전성 및 마스킹 검증.
- **CLI 및 안전성 테스트 (`tests/test_cli.py`)**:
  - 터미널 진행 상황 로그 출력 포맷 검증.
  - 사전 승인 토큰 불일치 시 차단 검증.
