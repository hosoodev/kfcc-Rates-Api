"""
mbank_crawler.py - MG더뱅킹 모바일 API 수집용 모듈 (병렬화 + 재시도 + 유틸리티 UA 적용)
- GitHub Actions 환경 및 한국형 브라우저(Whale, Samsung) 차단 방지 최적화
"""

import json
import time
import logging
import random
import requests
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# 공통 유틸리티 임포트
from utils import generate_random_ua

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

    def __init__(self, sigungu_codes_path: str = "src/data/sigungu_codes.json", base_dir: str = None):
        self.session = requests.Session()
        self.base_dir = base_dir
        
        # 공통 헤더 설정
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://mbank.kfcc.co.kr",
            "Referer": "https://mbank.kfcc.co.kr/",
        })
        
        self.sigungu_codes = self._load_codes(sigungu_codes_path)
        logger.info(f"🚀 모바일 크롤러 초기화 완료 (Parallel Mode + Utility UA 활화)")

    def _load_codes(self, path: str) -> Dict[str, Dict[str, str]]:
        """시군구 코드 로드"""
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"시군구 코드 로드 실패: {e}")
            return {}

    def fetch_rates_worker(self, sigun_gbcd: str, prdt_cd: str, prdt_nm: str, term: str, max_retries: int = 3) -> List[Dict[str, Any]]:
        """Worker Thread 전용: 특정 조합의 금리 수집 (재시도 + 유틸리티 UA)"""
        payload = {
            "CHANNELHEADER": {}, "SYSTEMHEADER": {},
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
        
        for attempt in range(max_retries):
            try:
                # 유틸리티에서 동적 UA 생성
                current_headers = { "User-Agent": generate_random_ua() }
                
                r = self.session.post(self.API_A, json=payload, headers=current_headers, timeout=12)
                r.raise_for_status()
                data = r.json()
                
                if data.get("CHANNELHEADER", {}).get("C_RESULT") != "00":
                    return []
                    
                rows = data["DATAPART"][0]["DATA"]["DATABODY"].get("GRID00") or []
                results = []
                for r_data in rows:
                    # 상상모바일통장(입출금)인 경우 수집된 데이터의 month를 0으로 고정
                    m_val = 0 if prdt_nm == "상상모바일통장" else int(term)
                    results.append({
                        "gmgoCd": r_data.get("GMGOCD"),
                        "prdtNm": prdt_nm,
                        "rate": float(r_data.get("IYUL", 0)),
                        "month": m_val
                    })
                return results
            except Exception as e:
                # 마지막 시도가 아니면 재시도
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + random.random()
                    logger.warning(f"⚠️ 요청 실패, {wait_time:.1f}초 후 재시도 ({attempt+1}/{max_retries}): {prdt_nm} {sigun_gbcd}")
                    time.sleep(wait_time)
                else:
                    logger.debug(f"❌ 최종 요청 실패(시군구:{sigun_gbcd}, {prdt_nm}, {term}개월): {e}")
        return []

    def collect_patch_data(self, product_names: List[str] = None, regions: List[str] = None, max_workers: int = 10) -> List[Dict[str, Any]]:
        """
        병렬 방식으로 패치 데이터 수집 (GitHub Actions 최적화)
        """
        if not product_names:
            # 병렬 처리가 가능하므로 모든 모바일 상품 수집
            product_names = list(self.PRODUCTS.keys())
        
        if not regions or regions == ['all']:
            regions = list(self.sigungu_codes.keys())

        terms = ["3", "6", "12"]
        tasks = []
        
        # 태스크 조합 생성
        for prdt_nm in product_names:
            prdt_cd = self.PRODUCTS.get(prdt_nm)
            if not prdt_cd: continue
            
            # 상상모바일통장은 입출금 통장이므로 패치 시 기간(Month)이 무의미함
            # 수집 부하를 줄이기 위해 단일 기간('0')으로 한 번만 수집
            p_terms = ["0"] if prdt_nm == "상상모바일통장" else terms

            for sido in regions:
                districts = self.sigungu_codes.get(sido, {})
                for dist_nm, dist_cd in districts.items():
                    for term in p_terms:
                        tasks.append((dist_cd, prdt_cd, prdt_nm, term))

        logger.info(f"⚡ 병렬 수집 시작: 총 {len(tasks)}개 태스크 (Workers: {max_workers}, Utility UA 활성)")
        
        patch_results_batch = []
        start_time = datetime.now()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(self.fetch_rates_worker, *task): task 
                for task in tasks
            }
            
            completed = 0
            for future in as_completed(future_to_task):
                completed += 1
                try:
                    res = future.result()
                    if res:
                        patch_results_batch.extend(res)
                except Exception as e:
                    logger.error(f"❌ Worker error: {e}")

                if completed % 100 == 0:
                    pct = (completed / len(tasks)) * 100
                    logger.info(f"🔄 진행상황: {pct:.1f}% ({completed}/{len(tasks)})")

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"✅ 수집 종료: 총 {len(patch_results_batch)}건 확보 (소요시간: {elapsed:.2f}초)")
        return patch_results_batch
