import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data')
BANK_LIST_FILE = os.path.join(DATA_DIR, 'banks.json')

# 통합 지역 정보 및 SEO/라우팅용 슬러그 매핑
# - 한글명: { "slug": "영문슬러그", "districts": { "한글 구/군/시": "영문슬러그" } }
REGIONS = {
    "서울": {
        "slug": "seoul",
        "districts": {
            "강남구": "gangnam", "서초구": "seocho", "송파구": "songpa", "강동구": "gangdong",
            "동작구": "dongjak", "관악구": "gwanak", "영등포구": "yeongdeungpo", "금천구": "geumcheon",
            "구로구": "guro", "양천구": "yangcheon", "강서구": "gangseo", "마포구": "mapo",
            "서대문구": "seodaemun", "은평구": "eunpyeong", "노원구": "nowon", "도봉구": "dobong",
            "강북구": "gangbuk", "성북구": "seongbuk", "중랑구": "jungnang", "동대문구": "dongdaemun",
            "광진구": "gwangjin", "성동구": "seongdong", "용산구": "yongsan", "종로구": "jongno", "중구": "jung"
        }
    },
    "부산": {
        "slug": "busan",
        "districts": {
            "해운대구": "haeundae", "수영구": "suyeong", "연제구": "yeonje", "기장군": "gijang",
            "남구": "nam", "강서구": "gangseo", "사상구": "sasang", "사하구": "saha",
            "북구": "buk", "금정구": "geumjeong", "동래구": "dongnae", "부산진구": "busanjin",
            "영도구": "yeongdo", "서구": "seo", "동구": "dong", "중구": "jung"
        }
    },
    "인천": {
        "slug": "incheon",
        "districts": {
            "강화군": "ganghwa", "계양구": "gyeyang", "남동구": "namdong", "미추홀구": "michuhol",
            "부평구": "bupyeong", "서구": "seo", "연수구": "yeonsu", "옹진군": "ongjin",
            "중구": "jung", "동구": "dong"
        }
    },
    "대구": {
        "slug": "daegu",
        "districts": {
            "남구": "nam", "달서구": "dalseo", "달성군": "dalseong", "동구": "dong",
            "북구": "buk", "서구": "seo", "수성구": "suseong", "중구": "jung", "군위군": "gunwi"
        }
    },
    "광주": {
        "slug": "gwangju",
        "districts": {
            "광산구": "gwangsan", "남구": "nam", "동구": "dong", "북구": "buk", "서구": "seo"
        }
    },
    "대전": {
        "slug": "daejeon",
        "districts": {
            "대덕구": "daedeok", "동구": "dong", "서구": "seo", "유성구": "yuseong", "중구": "jung"
        }
    },
    "울산": {
        "slug": "ulsan",
        "districts": {
            "남구": "nam", "동구": "dong", "북구": "buk", "울주군": "ulju", "중구": "jung"
        }
    },
    "세종": {
        "slug": "sejong",
        "districts": { "세종": "sejong" }
    },
    "경기": {
        "slug": "gyeonggi",
        "districts": {
            "가평군": "gapyeong", "고양시": "goyang", "덕양구": "deogyang", "일산동구": "ilsandong", "일산서구": "ilsanseo",
            "과천시": "gwacheon", "광명시": "gwangmyeong", "광주시": "gwangju", "구리시": "guri", "군포시": "gunpo",
            "김포시": "gimpo", "남양주시": "namyangju", "동두천시": "dongducheon", "부천시": "bucheon", "원미구": "wonmi", 
            "소사구": "sosa", "오정구": "ojeong", "성남시": "seongnam", "분당구": "bundang", "수정구": "sujeong", "중원구": "jungwon",
            "수원시": "suwon", "권선구": "gwonseon", "영통구": "yeongtong", "장안구": "jangan", "팔달구": "paldal",
            "시흥시": "siheung", "안산시": "ansan", "단원구": "danwon", "상록구": "sangnok", "안성시": "anseong",
            "안양시": "anyang", "동안구": "dongan", "만안구": "manan", "양주시": "yangju", "양평군": "yangpyeong",
            "여주시": "yeoju", "연천군": "yeoncheon", "오산시": "osan", "용인시": "yongin", "기흥구": "giheung",
            "수지구": "suji", "처인구": "cheoin", "의왕시": "uiwang", "의정부시": "uijeongbu", "이천시": "icheon",
            "파주시": "paju", "평택시": "pyeongtaek", "포천시": "pocheon", "하남시": "hanam", "화성시": "hwaseong"
        }
    },
    "강원": {
        "slug": "gangwon",
        "districts": {
            "강릉시": "gangneung", "고성군": "goseong", "동해시": "donghae", "삼척시": "samcheok",
            "속초시": "sokcho", "양구군": "yanggu", "양양군": "yangyang", "영월군": "yeongwol",
            "원주시": "wonju", "인제군": "inje", "정선군": "jeongseon", "철원군": "cheorwon",
            "춘천시": "chuncheon", "태백시": "taebaek", "평창군": "pyeongchang", "홍천군": "hongcheon",
            "화천군": "hwacheon", "횡성군": "hoengseong"
        }
    },
    "충북": {
        "slug": "chungbuk",
        "districts": {
            "괴산군": "goesan", "단양군": "danyang", "보은군": "boeun", "영동군": "yeongdong",
            "옥천군": "okcheon", "음성군": "eumseong", "제천시": "jecheon", "증평군": "jeungpyeong",
            "진천군": "jincheon", "청주시": "cheongju", "충주시": "chungju"
        }
    },
    "충남": {
        "slug": "chungnam",
        "districts": {
            "계룡시": "gyeryong", "공주시": "gongju", "금산군": "geumsan", "논산시": "nonsan",
            "당진시": "dangjin", "보령시": "boryeong", "부여군": "buyeo", "서산시": "seosan",
            "서천군": "seocheon", "아산시": "asan", "예산군": "yesan", "천안시": "cheonan",
            "청양군": "cheongyang", "태안군": "taean", "홍성군": "hongseong"
        }
    },
    "전북": {
        "slug": "jeonbuk",
        "districts": {
            "고창군": "gochang", "군산시": "gunsan", "김제시": "gimje", "남원시": "namwon",
            "무주군": "muju", "부안군": "buan", "순창군": "sunchang", "완주군": "wanju",
            "익산시": "iksan", "임실군": "imsil", "장수군": "jangsu", "전주시": "jeonju",
            "정읍시": "jeongeup", "진안군": "jinan"
        }
    },
    "전남": {
        "slug": "전남",
        "slug": "jeonnam",
        "districts": {
            "강진군": "gangjin", "고흥군": "goheung", "곡성군": "gokseong", "광양시": "gwangyang",
            "구례군": "gurye", "나주시": "naju", "담양군": "damyang", "목포시": "mokpo",
            "무안군": "muan", "보성군": "boseong", "순천시": "suncheon", "신안군": "sinan",
            "여수시": "yeosu", "영광군": "yeonggwang", "영암군": "yeongam", "완도군": "wando",
            "장성군": "jangseong", "장흥군": "jangheung", "진도군": "jindo", "함평군": "hampyeong",
            "해남군": "haenam", "화순군": "hwasun"
        }
    },
    "경북": {
        "slug": "gyeongbuk",
        "districts": {
            "경산시": "gyeongsan", "경주시": "gyeongju", "고령군": "goryeong", "구미시": "gumi",
            "김천시": "gimcheon", "문경시": "mungyeong", "봉화군": "bonghwa", "상주시": "sangju",
            "성주군": "seongju", "안동시": "andong", "영덕군": "yeongdeok", "영양군": "yeongyang",
            "영주시": "yeongju", "영천시": "yeongcheon", "예천군": "yecheon", "울릉군": "ulleung",
            "울진군": "uljin", "의성군": "uiseong", "청도군": "cheongdo", "청송군": "cheongsong",
            "칠곡군": "chilgok", "포항시": "pohang"
        }
    },
    "경남": {
        "slug": "gyeongnam",
        "districts": {
            "거제시": "geoje", "거창군": "geochang", "고성군": "goseong", "김해시": "gimhae",
            "남해군": "namhae", "밀양시": "miryang", "사천시": "sacheon", "산청군": "sancheong",
            "양산시": "yangsan", "의령군": "uiryeong", "진주시": "jinju", "창녕군": "changnyeong",
            "창원시": "changwon", "통영시": "tongyeong", "하동군": "hadong", "함안군": "haman",
            "함양군": "hamyang", "합천군": "hapcheon"
        }
    },
    "제주": {
        "slug": "jeju",
        "districts": { "제주시": "jeju", "서귀포시": "seogwipo" }
    }
}

# 크롤링 설정
CRAWLER_CONFIG = {
    'timeout': 10,
    'max_workers_list': 5,  # 은행 목록 수집 시 최대 워커 수
    'max_workers_rate': 10,  # 금리 수집 시 최대 워커 수
    'retry_count': 3,  # 재시도 횟수
    'retry_delay': 1  # 재시도 간격 (초)
}

# API 엔드포인트
API_ENDPOINTS = {
    'bank_list': 'https://www.kfcc.co.kr/map/list.do',
    'interest_rates': 'https://www.kfcc.co.kr/map/goods_19.do',
    'grade_evaluation': 'https://www.kfcc.co.kr/gumgo/regularDisclosure_new_view.do'
}

# 경영실태평가 설정
from datetime import datetime
current_year = datetime.now().year

GRADE_CONFIG = {
    'enabled': True,  # 경영실태평가 수집 활성화 여부
    'collection_month': [4, 10],  # 수집 월 (4월 15일, 10월 15일 - 12월 31일, 6월 30일 공시 후)
    'evaluation_year': current_year,  # 평가 연도 (당해년도 자동)
    'evaluation_month': [6, 12],  # 평가 기준 월 (6월, 12월)
    'retry_count': 3,
    'retry_delay': 1,
    'timeout': 10
}

# 등급 매핑
GRADE_MAP = {
    "1": {"name": "우수", "description": "경영상태가 매우 양호한 상태"},
    "2": {"name": "양호", "description": "경영상태가 양호한 상태"},
    "3": {"name": "보통", "description": "경영상태가 보통인 상태"},
    "4": {"name": "취약", "description": "경영상태가 취약한 상태"},
    "5": {"name": "위험", "description": "경영상태가 위험한 상태"}
}
