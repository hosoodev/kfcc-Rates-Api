# MG이자 (mgija.com) V2 크롤링 및 정적 API 아키텍처 개편 보고서

## 1. 개요 (Overview)
* **목적:** 기존 무거운 Raw 데이터 배열을 프론트엔드(Next.js) 렌더링에 최적화된 초경량 해시맵(Hash-Map) 구조로 개편.
* **핵심 기능:** 1일 1회 웹 크롤링(Base)에 더해, 주간 매시간 모바일 API(mBank TYPE A)를 호출하여 실시간 금리 변동을 덮어쓰기(Upsert) 하고 출처 뱃지를 제공.

## 2. 하이브리드 수집 파이프라인 (Hybrid Crawling Pipeline)
* **Base Action (웹 크롤러 - `crawler.py`)**
  * **주기:** 매일 02:00 (1회)
  * **동작:** 전국 1,200개 금고 전수 조사 -> `data/raw/YYYY-MM-DD.json` 생성 -> V2 API 스키마 변환기 실행.
  * **출처 마커:** 모든 금리 데이터의 초기 출처는 `"s": "w"` (Web)로 기록.
* **Patch Action (모바일 크롤러 - `mbank_crawler.py`)**
  * **주기:** 06:00 ~ 18:00 (매시간)
  * **동작:** `Mg_mobile_test.py`의 TYPE A 로직을 활용하여 주력 상품(12개월) 스캔 -> V2 API JSON 로드 -> 변동분만 덮어쓰기(Upsert).
  * **출처 마커:** 갱신된 금리 데이터의 출처는 `"s": "m"` (mBank)로 변경.

## 3. 브랜치 전략 (Git Branch Strategy)
* **`main` 브랜치:** 파이썬 크롤러 소스 코드와 `.yml` 워크플로우 파일만 보관 (데이터 푸시 금지).
* **`api-data` 브랜치:** 크롤러의 결과물(Raw Data & V2 API JSON)만 보관. GitHub Pages(`api.mgija.com`)와 연결되어 정적 API 서버로 동작.

## 4. V2 정적 API 스키마 (V2 Static API Schema)
* **파일 분할:** `deposit.json`(예금), `saving.json`(적금), `demand.json`(파킹통장)
* **특징:** 경영실태평가(Grade, BIS) 사전 결합(Pre-join) 및 배열 탈피(해시맵 압축).
* **BIS는 데이터가 없을 수 있음**
```json
{
  "updated_at": "2026-03-18T12:00:00+09:00",
  "data": [
    {
      "gmgoCd": "01011",
      "name": "강동(본점)",
      "region": "서울",
      "grade": "1",
      "bis_ratio": 10.45,
      "products": {
        "MG더뱅킹정기예금": {
          "12": { "r": 5.50, "s": "m" }, // 모바일 실시간 업데이트분
          "6": { "r": 4.50, "s": "w" }   // 기존 웹 베이스 데이터
        }
      }
    }
  ]
}
```
## 5. 단계별 구현 마일스톤 (Milestones)
* Step 1: storage.py에 원본 데이터를 V2 API 스키마로 변환하여 생성하는 build_v2_api() 로직 추가.
* Step 2: Mg_mobile_test.py를 리팩토링하여 mbank_crawler.py (TYPE A 전용) 모듈 생성.
* Step 3: storage.py에 모바일 수집 데이터를 기존 V2 API에 덮어쓰는 upsert_mbank_patch() 로직 추가.
* Step 4: main.py에 --mode base와 --mode patch 실행 분기 추가.
* Step 5: GitHub Actions .yml 파일 수정 (main / api-data 브랜치 푸시 분리 및 스케줄링).