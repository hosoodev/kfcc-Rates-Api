# 🏦 새마을금고 금리 API v2 (KFCC Rates API)

새마을금고(KFCC) 웹사이트 및 모바일 앱에서 전국 금리 정보와 경영실태평가 데이터를 수집하여 고성능 정적 JSON API로 제공하는 Python 크롤러 시스템입니다.

> [!NOTE]
> **v2 업데이트**: 데이터 저장 구조를 브랜치 별로 분리하고, 모바일 앱 기반의 실시간 금리 패치(`--mode patch`) 기능이 추가되었습니다.

---

## ✨ 주요 기능

- **📊 전국 금리 수집**: 17개 시/도 전역의 요구불/거치식/적립식 예탁금 금리 일별 수집.
- **📱 실시간 패치 (v2)**: 모바일 앱 연동을 통해 특정 지역의 최신 실시간 금리를 즉시 업데이트.
- **🏆 경영실태평가 통합**: 전국 금고의 경영 등급 및 BIS 비율 데이터를 금리 정보와 결합.
- **🤖 완전 자동화**: GitHub Actions를 통한 정기 크롤링 및 자동 배포 시스템 구축.
- **🚀 Static API**: 수집된 데이터를 가공하여 별도의 백엔드 서버 없이도 즉시 사용 가능한 정적 JSON API 제공.

---

## 🏗️ 시스템 아키텍처

이 프로젝트는 코드와 데이터를 분리하여 관리하는 이중 브랜치 구조를 사용합니다.

- **`main` 브랜치**: Python 크롤러 엔진 및 자동화 워크플로우 소스 코드 관리.
- **`api-data` 브랜치**: 크롤링된 모든 JSON 파일이 저장되며, GitHub Pages를 통해 [api.mgija.com](https://api.mgija.com)으로 호스팅됩니다.

---

## 🚀 시작하기 (로컬 실행)

### 1. 환경 설정
```bash
git clone https://github.com/hosoodev/kfcc-Rates-Api.git
cd kfcc-Rates-Api/backend
pip install -r requirements.txt
```

### 2. 실행 모드
```bash
# [Base 모드] 전체 금고 전수 조사 및 V2 API 빌드
python src/main.py --mode base

# [Patch 모드] 특정 지역(서울, 경기 등)의 실시간 금리 패치
python src/main.py --mode patch --regions 서울,경기

# [Grades 모드] 경영실태평가 수집 (정기 업데이트용)
python src/main.py --grades
```

---

## 📁 프로젝트 구조

```
backend/
├── src/
│   ├── main.py              # 메인 실행 엔트리포인트
│   ├── crawler.py           # 웹 기반 금리 크롤러
│   ├── mbank_crawler.py      # 모바일 앱 기반 패치 크롤러 (v2)
│   ├── grade_crawler.py     # 경영실태평가 수집기
│   ├── storage.py           # 데이터 저장 및 v2 API 빌더
│   └── parser.py            # 데이터 추출 및 가공
├── data/                    # [Legacy/Raw] 원본 데이터 저장소
├── v2/                      # [V2 Static API] 정적 API 결과물
│   ├── meta/                # 은행 목록 등 메타 데이터
│   └── rates/               # 금리 데이터 (분류별/지역별)
└── .github/workflows/       # GitHub Actions 자동화 스크립트
```

---

## 🌐 API 엔드포인트 (v2)

모든 데이터는 `api.mgija.com/v2/` 경로를 통해 서비스됩니다.

- **금고 목록**: `/v2/meta/banks.json`
- **전체 예금 금리**: `/v2/rates/deposit/all.json`
- **전체 적금 금리**: `/v2/rates/saving/all.json`
- **전체 입출금 금리**: `/v2/rates/demand/all.json`
- **지역별 상세**: `/v2/rates/{type}/regions/{province_slug}/{district_slug}.json`

---

## 📅 자동화 스케줄

- **금리 업데이트**: 매일 오전 2시 (KST) 전체 전수 조사.
- **실시간 패치**: 평일 주간(08~17시) 2시간 간격 정기 패치 실행.
- **배포 프로세스**: 데이터 업데이트 즉시 `api-data` 브랜치 배포 및 프론트엔드 캐시 재생성(Revalidation) 트리거.

---

## 📝 라이선스
MIT License

---
**⭐ 도움이 되셨다면 Star를 눌러 응원해 주세요!**
