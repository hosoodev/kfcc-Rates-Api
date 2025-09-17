# 🏦 새마을금고 금리 크롤러

새마을금고(KFCC) 웹사이트에서 금리 정보와 경영실태평가 데이터를 자동으로 수집하여 JSON API로 제공하는 Python 크롤러입니다.

## ✨ 주요 기능

### 📊 금리 정보 수집
- **3가지 상품 유형** 수집
  - 요구불예탁금 (온라인자립예탁금, 상상모바일통장)
  - 거치식예탁금 (MG더뱅킹정기예금)
  - 적립식예탁금 (MG더뱅킹정기적금, MG더뱅킹자유적금)
- **전국 17개 시/도** 모든 지역 수집
- **실시간 금리 데이터** 일별 수집
- **통계 정보** 제공 (평균, 최대, 최소 금리)

### 🏆 경영실태평가 수집
- **연 2회 수집** (1월 15일, 7월 15일)
- **5단계 등급** 평가 (우수, 양호, 보통, 취약, 위험)
- **지역별 분류** 포함 (시/구 정보)
- **자동화된 스케줄링**

### 🤖 자동화
- **GitHub Actions** 기반 자동 실행
- **일별 크롤링** (매일 오전 2시 KST)
- **자동 데이터 정리** (365일 이상 된 데이터)
- **압축 저장** (대용량 파일 최적화)

## 🚀 빠른 시작

### 1. 저장소 클론
```bash
git clone https://github.com/hosoodev/kfcc-Rates-Api.git
cd kfcc-Rates-Api/backend
```

### 2. 의존성 설치
```bash
pip install -r requirements.txt
```

### 3. 크롤링 실행
```bash
# 기본 크롤링 (제주도만)
python src/main.py

# 전체 지역 크롤링 (config.py에서 주석 해제)
python src/main.py

# 30일 이상 된 데이터 정리하며 크롤링
python src/main.py --cleanup 30

# 통계 정보만 출력
python src/main.py --stats

# 경영실태평가 수집 (1월, 7월에만)
python src/main.py --grades
```

## 📁 프로젝트 구조

```
backend/
├── src/
│   ├── main.py              # 메인 실행 파일
│   ├── config.py            # 설정 파일
│   ├── crawler.py           # 금리 크롤러
│   ├── grade_crawler.py     # 경영실태평가 크롤러
│   ├── parser.py            # HTML 파서
│   └── storage.py           # 데이터 저장 관리
├── data/
│   ├── banks.json           # 은행 목록
│   ├── summary.json         # 요약 통계
│   ├── rates/               # 일별 금리 데이터
│   │   ├── 2025-09-15.json
│   │   └── ...
│   └── grades/              # 경영실태평가 데이터
│       ├── grades_2025_06.json  # 6월 평가 (7월 15일 수집)
│       └── grades_2025_12.json  # 12월 평가 (1월 15일 수집)
├── .github/workflows/
│   ├── crawler.yml          # 메인 크롤링 워크플로우
│   └── grade-crawler.yml    # 경영실태평가 워크플로우
├── scripts/
│   ├── restore-july-schedule.py  # 7월 스케줄 복원
│   └── restore-july-schedule.md  # 복원 가이드
└── requirements.txt         # Python 의존성
```

## 📊 데이터 형식

### 금리 데이터 예시
```json
{
  "metadata": {
    "date": "2025-09-15",
    "total_banks": 74,
    "successful_banks": 74,
    "crawled_at": "2025-09-15T21:33:04.455000",
    "version": "1.1"
  },
  "summary": {
    "total_banks": 74,
    "total_products": 292,
    "average_rate": 1.33,
    "min_rate": 0.01,
    "max_rate": 3.5,
    "rate_range": {
      "min": 0.01,
      "max": 3.5
    },
    "duration_stats": {
      "0": {
        "count": 135,
        "average_rate": 0.11,
        "min_rate": 0.01,
        "max_rate": 1.0
      },
      "12": {
        "count": 116,
        "average_rate": 2.5,
        "min_rate": 1.6,
        "max_rate": 3.1
      }
    },
    "product_type_stats": {
      "요구불예탁금": {
        "count": 135,
        "average_rate": 0.11,
        "min_rate": 0.01,
        "max_rate": 1.0
      }
    }
  },
  "rates": [
    {
      "bank": {
        "gmgoCd": "6705",
        "name": "법환(본점)",
        "city": "제주",
        "district": "서귀포시",
        "phone": "064-739-1234",
        "address": "제주 서귀포시 중앙로 123"
      },
      "base_date": "2025-09-15",
      "products": [
        {
          "product_name": "온라인자립예탁금",
          "duration_months": 0,
          "interest_rate": 0.1,
          "duration_text": "",
          "rate_text": "0.10%",
          "product_type": "요구불예탁금"
        }
      ],
      "crawled_at": "2025-09-15T21:33:08.829000",
      "total_products": 2
    }
  ]
}
```

### 경영실태평가 데이터 예시
```json
{
  "collection_info": {
    "collected_at": "2025-07-15T02:00:00.000000",
    "total_banks": 41,
    "evaluation_year": 2025,
    "evaluation_month": 6
  },
  "grades": [
    {
      "gmgo_cd": "6705",
      "bank_name": "법환(본점)",
      "city": "제주",
      "district": "서귀포시",
      "evaluation_agency": "법환새마을금고",
      "evaluation_date": "20250630",
      "grade_code": "2",
      "grade_name": "양호",
      "grade_description": "경영상태가 양호한 상태",
      "collected_at": "2025-07-15T02:00:00.000000",
      "evaluation_year": 2025,
      "evaluation_month": 6
    }
  ]
}
```

### 은행 목록 데이터 예시
```json
{
  "collection_info": {
    "collected_at": "2025-09-15T21:30:00.000000",
    "total_banks": 3000,
    "regions": 17,
    "version": "1.1"
  },
  "banks": [
    {
      "gmgoCd": "6705",
      "name": "법환(본점)",
      "city": "제주",
      "district": "서귀포시",
      "phone": "064-739-1234",
      "address": "제주 서귀포시 중앙로 123",
      "region": "제주"
    }
  ]
}
```

### 요약 통계 데이터 예시
```json
{
  "total_banks": 3000,
  "total_products": 12000,
  "average_rate": 2.15,
  "min_rate": 0.01,
  "max_rate": 4.5,
  "rate_range": {
    "min": 0.01,
    "max": 4.5
  },
  "duration_stats": {
    "0": {
      "count": 5000,
      "average_rate": 0.15,
      "min_rate": 0.01,
      "max_rate": 1.2
    },
    "12": {
      "count": 4000,
      "average_rate": 2.8,
      "min_rate": 1.5,
      "max_rate": 4.0
    },
    "24": {
      "count": 3000,
      "average_rate": 3.2,
      "min_rate": 2.0,
      "max_rate": 4.5
    }
  },
  "product_type_stats": {
    "요구불예탁금": {
      "count": 5000,
      "average_rate": 0.15,
      "min_rate": 0.01,
      "max_rate": 1.2
    },
    "거치식예탁금": {
      "count": 4000,
      "average_rate": 2.8,
      "min_rate": 1.5,
      "max_rate": 4.0
    },
    "적립식예탁금": {
      "count": 3000,
      "average_rate": 2.5,
      "min_rate": 1.0,
      "max_rate": 3.8
    }
  },
  "crawled_at": "2025-09-15T21:30:00.000000"
}
```

## ⚙️ 설정

### 지역 설정 (config.py)
```python
REGIONS = {
    "서울": ["도봉구", "마포구", "관악구", ...],
    "인천": ["강화군", "서구", "동구", ...],
    "경기": ["김포시", "파주시", "연천군", ...],
    # ... 모든 시/도
}
```

### 크롤링 설정
```python
CRAWLER_CONFIG = {
    'timeout': 10,              # 요청 타임아웃 (초)
    'max_workers_list': 5,      # 은행 목록 수집 워커 수
    'max_workers_rate': 10,     # 금리 수집 워커 수
    'retry_count': 3,           # 재시도 횟수
    'retry_delay': 1            # 재시도 간격 (초)
}
```

### 경영실태평가 설정
```python
GRADE_CONFIG = {
    'enabled': True,            # 수집 활성화
    'collection_month': [1, 7], # 수집 월 (1월 15일, 7월 15일)
    'evaluation_year': 2025,    # 평가 연도 (당해년도 자동)
    'evaluation_month': [6, 12], # 평가 기준 월 (6월, 12월)
    'retry_count': 3,
    'retry_delay': 1,
    'timeout': 10
}
```

## 🔧 사용법

### 명령행 옵션
```bash
python src/main.py [옵션]

옵션:
  --cleanup DAYS     지정된 일수 이상 된 데이터 정리
  --stats            저장소 통계 정보만 출력
  --grades           경영실태평가 데이터 수집 (1월, 7월에만)
  --help             도움말 출력
  --version          버전 정보 출력
```

### 환경변수
```bash
# 경영실태평가 강제 실행 (월 제한 무시)
export FORCE_GRADE_COLLECTION=true
python src/main.py --grades
```

## 📅 스케줄

### 자동 실행
- **금리 크롤링**: 매일 오전 2시 (KST)
- **경영실태평가**: 매년 1월 15일, 7월 15일 (12월 31일, 6월 30일 공시 후)
- **데이터 정리**: 매일 (365일 이상 된 데이터)

### 수동 실행
- **GitHub Actions**: 워크플로우 수동 실행 가능
- **로컬 실행**: 언제든지 `python src/main.py` 실행

## 🛠️ 개발

### 의존성
```
requests>=2.31.0
beautifulsoup4>=4.12.0
python-dotenv>=1.0.0
```

### 개발 환경 설정
```bash
# 가상환경 생성
python -m venv venv

# 가상환경 활성화 (Windows)
venv\Scripts\activate

# 가상환경 활성화 (Linux/Mac)
source venv/bin/activate

# 의존성 설치
pip install -r requirements.txt
```

### 테스트
```bash
# 기본 크롤링 테스트
python src/main.py --stats

# 경영실태평가 테스트 (환경변수 설정)
export FORCE_GRADE_COLLECTION=true
python src/main.py --grades
```

## 📈 성능

### 수집 성능
- **은행 목록**: ~5초 (전국 3,000+ 금고)
- **금리 데이터**: ~30초 (병렬 처리)
- **경영실태평가**: ~10초 (41개 금고)

### 저장소 크기
- **일별 금리 파일**: ~100KB (압축 시 ~20KB)
- **월간 데이터**: ~3MB
- **연간 데이터**: ~36MB

## 🔒 보안

- **User-Agent**: 실제 브라우저 모방
- **요청 제한**: 적절한 딜레이 적용
- **에러 처리**: 재시도 메커니즘
- **데이터 검증**: 수집된 데이터 유효성 검사

## 📝 라이선스

MIT License

## 🤝 기여

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📞 지원

- **이슈**: [GitHub Issues](https://github.com/hosoodev/kfcc-Rates-Api/issues)
- **문서**: [Wiki](https://github.com/hosoodev/kfcc-Rates-Api/wiki)
- **토론**: [Discussions](https://github.com/hosoodev/kfcc-Rates-Api/discussions)

## 📋 체크리스트

- [x] 금리 정보 수집
- [x] 경영실태평가 수집
- [x] 자동화된 스케줄링
- [x] 데이터 압축 저장
- [x] 통계 정보 생성
- [x] 에러 처리 및 재시도
- [x] 로깅 시스템
- [x] 설정 관리
- [x] 문서화

## 🔄 업데이트 로그

### v1.2.0 (2025-09-16)
- 🔄 경영실태평가 수집 주기 변경 (연 2회: 1월, 7월)
- 📁 파일명 형식 개선 (grades_YYYY_MM.json)
- ⏰ 크롤링 시간 변경 (오전 2시 KST)
- 🗂️ 데이터 보관 기간 연장 (365일)
- 📊 통계 정보에 최대/최소 금리 추가
- 🏢 지역별 상세 정보 포함 (시/구)

### v1.1.0 (2025-09-15)
- 🏆 경영실태평가 수집 기능 추가
- 📊 통계 정보 생성 기능
- 🤖 GitHub Actions 자동화

### v1.0.0 (2025-09-14)
- ✨ 초기 릴리스
- 🏦 금리 정보 수집 기능
- 🗜️ 데이터 압축 저장

---

**⭐ 이 프로젝트가 도움이 되었다면 Star를 눌러주세요!**
