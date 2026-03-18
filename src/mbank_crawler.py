"""
mbank_crawler.py - MG더뱅킹 모바일 API 수집용 모듈
- TYPE A API를 사용하여 상품별 전국 금리를 빠르게 수집
"""

import json
import time
import logging
import random
import requests
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class MBankCrawler:
    """MG더뱅킹 모바일 API 크롤러"""
    
    API_A = "https://mbank.kfcc.co.kr/psb/telegram/prdt/PSBPRDT020014A2"
    
    # 주요 상품 코드 (TYPE A)
    PRODUCTS = {
        "MG더뱅킹정기예금": "1003100G010",
        "MG더뱅킹정기적금": "1004200G010",
        "MG더뱅킹자유적금": "1004202G010",
        "상상모바일통장": "1002003G016",
    }

    # 모바일 User-Agent 풀 (차단 방지용)
    USER_AGENTS = [
        "Mozilla/5.0 (Linux; Android 13; SM-S928N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.7632.216 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.7632.216 Mobile Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPad; CPU OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/145.0.7632.216 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.7632.216 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 13; SM-A546B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.7632.216 Mobile Safari/537.36",
    ]

    def __init__(self, sigungu_codes_path: str = "src/data/sigungu_codes.json"):
        self.session = requests.Session()
        
        # 랜덤 User-Agent 선택
        ua = random.choice(self.USER_AGENTS)
        self.session.headers.update({
            "User-Agent": ua,
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://mbank.kfcc.co.kr",
            "Referer": "https://mbank.kfcc.co.kr/",
        })
        
        logger.info(f"🚀 모바일 크롤러 초기화 완료 (UA: {ua[:30]}...)")
        self.sigungu_codes = self._load_codes(sigungu_codes_path)

    def _load_codes(self, path: str) -> Dict[str, Dict[str, str]]:
        """시군구 코드 로드"""
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"시군구 코드 로드 실패: {e}")
            return {}

    def fetch_rates_by_region(self, sigun_gbcd: str, prdt_cd: str, term: str = "12") -> List[Dict[str, Any]]:
        """특정 시군구의 상품 금리 수집 (TYPE A)"""
        payload = {
            "CHANNELHEADER": {},
            "SYSTEMHEADER": {},
            "DATAPART": [{
                "DATA": {
                    "DATAHEADER": {"SCREEN_ID": "PMWPRDT020015"},
                    "DATABODY": {
                        "SIGUN_GBCD": sigun_gbcd,
                        "CONTR_TERM": term,
                        "PRDT_CD": prdt_cd,
                        "INQ_GBCD": "1",
                    }
                }
            }]
        }
        
        try:
            r = self.session.post(self.API_A, json=payload, timeout=15)
            r.raise_for_status()
            data = r.json()
            if data.get("CHANNELHEADER", {}).get("C_RESULT") != "00":
                return []
            return data["DATAPART"][0]["DATA"]["DATABODY"].get("GRID00") or []
        except Exception as e:
            logger.error(f"API 호출 실패 (시군구:{sigun_gbcd}, 상품:{prdt_cd}): {e}")
            return []

    def collect_patch_data(self, product_names: List[str] = None, regions: List[str] = None) -> List[Dict[str, Any]]:
        """
        V2 업데이트용 패치 데이터 수집
        - regions: ["서울", "경기", ...] (None이면 서울만 기본)
        """
        if not product_names:
            product_names = ["MG더뱅킹정기예금"] # 기본 예금
        
        if not regions:
            regions = ["서울"]

        terms = ["3", "6", "12"] # 수집할 기간 (개월)
        patch_results = []
        
        for prdt_nm in product_names:
            prdt_cd = self.PRODUCTS.get(prdt_nm)
            if not prdt_cd:
                continue

            for sido in regions:
                districts = self.sigungu_codes.get(sido, {})
                logger.info(f"📱 모바일 수집 시작: {sido} - {prdt_nm}")
                
                for dist_nm, dist_cd in districts.items():
                    for term in terms:
                        rows = self.fetch_rates_by_region(dist_cd, prdt_cd, term=term)
                        for r in rows:
                            patch_results.append({
                                "gmgoCd": r.get("GMGOCD"),
                                "prdtNm": prdt_nm,
                                "rate": float(r.get("IYUL", 0)),
                                "month": int(term)
                            })
                        time.sleep(0.3) # 과부하 방지 (기간별 요청 사이)
                    time.sleep(0.5) # 구/군별 요청 사이 지연
        
        return patch_results
