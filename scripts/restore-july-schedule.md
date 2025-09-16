# 7월 스케줄 복원 가이드

## 2026년 7월 1일 이전에 다음 파일들을 수정해야 합니다:

### 1. src/config.py
```python
# 99번째 줄 수정
'collection_month': 7,  # 수집 월 (7월)
```

### 2. .github/workflows/crawler.yml
```yaml
# 58-59번째 줄 수정
- name: Collect grade evaluations (July only)
  if: github.event.schedule == '0 2 1 7 *' || github.event_name == 'workflow_dispatch'
```

### 3. .github/workflows/grade-crawler.yml
이 파일은 삭제하거나 비활성화할 수 있습니다.

## 자동화된 복원 스크립트
```bash
# 2026년 6월 30일에 실행
python scripts/restore-july-schedule.py
```
