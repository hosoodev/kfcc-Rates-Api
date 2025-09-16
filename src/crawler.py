import requests
import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass
from urllib.parse import urlencode

from config import REGIONS, CRAWLER_CONFIG, API_ENDPOINTS
from parser import parse_bank_list, parse_interest_rates

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class Bank:
    """은행 정보 데이터 클래스"""
    gmgoCd: str
    name: str
    city: str
    district: str
    address: str = ""
    phone: str = ""
    type: str = ""
    crawled_at: str = ""


@dataclass
class InterestRate:
    """금리 정보 데이터 클래스"""
    bank: Dict[str, Any]
    base_date: str
    products: List[Dict[str, Any]]
    crawled_at: str
    total_products: int


class KFCCCrawler:
    """새마을금고 금리 크롤러 클래스"""
    
    def __init__(self):
        """크롤러 초기화"""
        self.session = self._create_session()
        self.stats = {
            'banks_fetched': 0,
            'rates_fetched': 0,
            'errors': []
        }
    
    def _create_session(self) -> requests.Session:
        """세션 생성 및 헤더 설정"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        return session
    
    def _make_request(self, url: str, params: Dict[str, Any], 
                     max_retries: int = 3) -> Optional[requests.Response]:
        """HTTP 요청 처리 (재시도 로직 포함)"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(
                    url, 
                    params=params, 
                    timeout=CRAWLER_CONFIG['timeout']
                )
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"요청 실패 (시도 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(CRAWLER_CONFIG['retry_delay'] * (attempt + 1))
                else:
                    logger.error(f"최종 요청 실패: {url} with params {params}")
                    return None
        return None
    
    def fetch_bank_list(self, city: str, district: str) -> List[Dict[str, Any]]:
        """특정 지역의 새마을금고 목록을 가져옴"""
        # 하위 메뉴가 1개이고 city와 같은 경우 처리
        if city in REGIONS and len(REGIONS[city]) == 1 and REGIONS[city][0] == city:
            params = {'r1': city, 'r2': ''}
        else:
            params = {'r1': city, 'r2': district}
        
        response = self._make_request(
            API_ENDPOINTS['bank_list'], 
            params, 
            CRAWLER_CONFIG['retry_count']
        )
        
        if not response:
            logger.error(f"은행 목록 수집 실패: {city} {district}")
            return []
        
        try:
            banks = parse_bank_list(response.text, city, district)
            logger.info(f"✓ {city} {district}: {len(banks)}개 금고 수집 완료")
            self.stats['banks_fetched'] += len(banks)
            return banks
        except Exception as e:
            logger.error(f"파싱 오류: {city} {district} - {e}")
            self.stats['errors'].append(f"{city} {district}: {str(e)}")
            return []
    
    def fetch_interest_rates(self, bank: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """특정 금고의 금리 정보를 가져옴"""
        if 'gmgoCd' not in bank:
            logger.warning(f"{bank.get('name', 'Unknown')}: 금고 코드 없음")
            return None
        
        all_products = []
        product_types = [
            {'code': '12', 'name': '요구불예탁금'},
            {'code': '13', 'name': '거치식예탁금'},
            {'code': '14', 'name': '적립식예탁금'}
        ]
        
        for product_type in product_types:
            params = {
                'OPEN_TRMID': bank['gmgoCd'],
                'gubuncode': product_type['code']
            }
            
            response = self._make_request(
                API_ENDPOINTS['interest_rates'],
                params,
                CRAWLER_CONFIG['retry_count']
            )
            
            if response:
                try:
                    products = parse_interest_rates(
                        response.text, 
                        bank, 
                        product_type['name']
                    )
                    if products:
                        all_products.extend(products)
                except Exception as e:
                    logger.warning(f"파싱 오류: {bank['name']} - {product_type['name']}: {e}")
        
        # 중복 제거
        unique_products = self._remove_duplicate_products(all_products)
        
        if unique_products:
            result = InterestRate(
                bank=bank,
                base_date='',
                products=unique_products,
                crawled_at=datetime.now().isoformat(),
                total_products=len(unique_products)
            ).__dict__
            
            logger.info(f"✓ {bank['name']}: {len(unique_products)}개 상품 금리 수집 완료")
            self.stats['rates_fetched'] += 1
            return result
        else:
            logger.warning(f"{bank['name']}: 금리 정보 없음")
            return None
    
    def _remove_duplicate_products(self, products: List[Dict]) -> List[Dict]:
        """중복 상품 제거"""
        seen = set()
        unique = []
        
        for product in products:
            key = (
                product['product_name'], 
                product['duration_months'], 
                product['interest_rate'], 
                product['product_type']
            )
            if key not in seen:
                seen.add(key)
                unique.append(product)
        
        return unique
    
    def collect_bank_lists_parallel(self) -> List[Dict[str, Any]]:
        """모든 지역의 금고 목록을 병렬로 수집"""
        logger.info("🏦 은행 목록 수집 시작...")
        all_banks = []
        
        # 모든 지역 정보를 튜플 리스트로 준비
        regions_to_fetch = [
            (city, district) 
            for city, districts in REGIONS.items() 
            for district in districts
        ]
        
        with ThreadPoolExecutor(max_workers=CRAWLER_CONFIG['max_workers_list']) as executor:
            # 진행률 표시를 위한 카운터
            completed = 0
            total = len(regions_to_fetch)
            
            future_to_region = {
                executor.submit(self.fetch_bank_list, city, district): (city, district)
                for city, district in regions_to_fetch
            }
            
            for future in as_completed(future_to_region):
                city, district = future_to_region[future]
                try:
                    banks = future.result()
                    if banks:
                        all_banks.extend(banks)
                    
                    completed += 1
                    if completed % 10 == 0:
                        logger.info(f"진행률: {completed}/{total} ({completed/total*100:.1f}%)")
                        
                except Exception as e:
                    logger.error(f"처리 오류: {city} {district} - {e}")
                    self.stats['errors'].append(f"{city} {district}: {str(e)}")
        
        logger.info(f"🏦 총 {len(all_banks)}개 금고 목록 수집 완료")
        return all_banks
    
    def collect_interest_rates_parallel(self, banks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """모든 금고의 금리 정보를 병렬로 수집"""
        logger.info("💰 금리 정보 수집 시작...")
        all_rates = []
        
        with ThreadPoolExecutor(max_workers=CRAWLER_CONFIG['max_workers_rate']) as executor:
            completed = 0
            total = len(banks)
            
            future_to_bank = {
                executor.submit(self.fetch_interest_rates, bank): bank
                for bank in banks
            }
            
            for future in as_completed(future_to_bank):
                bank = future_to_bank[future]
                try:
                    rates = future.result()
                    if rates:
                        all_rates.append(rates)
                    
                    completed += 1
                    if completed % 20 == 0:
                        logger.info(f"진행률: {completed}/{total} ({completed/total*100:.1f}%)")
                        
                except Exception as e:
                    logger.error(f"처리 오류: {bank.get('name', 'Unknown')} - {e}")
                    self.stats['errors'].append(f"{bank.get('name', 'Unknown')}: {str(e)}")
        
        logger.info(f"💰 총 {len(all_rates)}개 금고의 금리 정보 수집 완료")
        return all_rates
    
    def run(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """전체 크롤링 프로세스 실행"""
        logger.info("🚀 새마을금고 금리 크롤링 시작")
        start_time = time.time()
        
        try:
            # 1단계: 은행 목록 수집
            banks = self.collect_bank_lists_parallel()
            if not banks:
                logger.error("❌ 은행 목록 수집 실패")
                return [], []
            
            # 2단계: 금리 정보 수집
            rates = self.collect_interest_rates_parallel(banks)
            
            # 결과 요약
            elapsed_time = time.time() - start_time
            logger.info(f"✅ 크롤링 완료: {len(banks)}개 금고, {len(rates)}개 금리 정보")
            logger.info(f"⏱️ 소요 시간: {elapsed_time:.2f}초")
            
            # 통계 출력
            self._print_statistics()
            
            return banks, rates
            
        except Exception as e:
            logger.error(f"❌ 크롤링 중 오류 발생: {e}")
            return [], []
        finally:
            self.session.close()
    
    def _print_statistics(self) -> None:
        """수집 통계 출력"""
        logger.info("=" * 50)
        logger.info("📊 수집 통계")
        logger.info(f"  - 수집된 은행 수: {self.stats['banks_fetched']}")
        logger.info(f"  - 수집된 금리 정보: {self.stats['rates_fetched']}")
        logger.info(f"  - 오류 발생 수: {len(self.stats['errors'])}")
        
        if self.stats['errors']:
            logger.warning("⚠️ 오류 발생 목록:")
            for error in self.stats['errors'][:10]:  # 처음 10개만 표시
                logger.warning(f"  - {error}")
            if len(self.stats['errors']) > 10:
                logger.warning(f"  ... 외 {len(self.stats['errors']) - 10}개")
    
    def get_region_stats(self, banks: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
        """지역별 통계 정보 생성"""
        stats = {}
        for bank in banks:
            city = bank.get('city', 'Unknown')
            district = bank.get('district', 'Unknown')
            
            if city not in stats:
                stats[city] = {}
            if district not in stats[city]:
                stats[city][district] = 0
            stats[city][district] += 1
        
        return stats