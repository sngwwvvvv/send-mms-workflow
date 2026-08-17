# SENS 통합 MMS worker 실험 worktree 운영 정책

작성일: 2026-08-17
상태: 적용

## 작업 영역

| 구분 | 브랜치 | 작업 경로 | 책임 |
| --- | --- | --- | --- |
| 기존 운영 | `main` | `C:\workflows\send-mms-workflow` | 현재 통합 MMS 운영 코드와 운영 데이터 |
| worker 구현 | `experiment/lms-then-mms-worker-pipeline` | `C:\workflows\send-mms-workflow-lms-then-mms` | 8열 통합 MMS worker 구현·모의 테스트·승격 준비 |

## 기준 문서

- 활성 계약: `docs/superpowers/specs/2026-08-17-sens-mms-worker-pipeline-design.md`
- 활성 실행 계획: `docs/superpowers/plans/2026-08-17-sens-mms-worker-pipeline.md`

## 운영 원칙

- 실험 worktree는 8열 통합 MMS worker 구현과 모의 검증만 담당한다.
- main worktree의 운영 데이터, 결과, 로그, 이미지, 비밀값을 가져오거나 공유하지 않는다.
- 실발송은 별도 명시적 승인 없이는 수행하지 않는다.
- 실험 branch와 worktree는 결정 이력을 위해 유지하며, 별도 지시 없이 삭제하지 않는다.
