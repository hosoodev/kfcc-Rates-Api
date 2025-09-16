# -*- coding: utf-8 -*-
"""
HTML 파싱 모듈
새마을금고 웹사이트의 HTML을 파싱하여 구조화된 데이터로 변환
"""

from bs4 import BeautifulSoup
import re
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any, Tuple

logger = logging.getLogger(__name__)


class BankParser:
    """은행 목록 파서 클래스"""
    
    @staticmethod
    def extract_from_hidden_spans(row) -> Optional[Dict[str, str]]:
        """Hidden span 태그에서 정보 추출"""
        hidden_spans = row.find_all('span', {'style': 'display: none;'})
        if len(hidden_spans) < 5:
            return None
        
        bank_data = {}
        field_mapping = {
            'gmgoCd': 'gmgoCd',
            'name': 'name',
            'divNm': 'branch_name',
            'gmgoType': 'type',
            'telephone': 'phone',
            'addr': 'address'
        }
        
        for span in hidden_spans:
            title = span.get('title', '')
            value = span.get_text(strip=True)
            
            if title in field_mapping:
                bank_data[field_mapping[title]] = value
        
        # 필수 필드 확인
        if 'gmgoCd' not in bank_data or 'name' not in bank_data:
            return None
        
        # 최종 금고명 구성
        if bank_data.get('branch_name'):
            bank_data['name'] = f"{bank_data['name']}({bank_data['branch_name']})"
            del bank_data['branch_name']
        
        return bank_data
    
    @staticmethod
    def extract_from_text(row) -> Optional[Dict[str, str]]:
        """텍스트에서 정보 추출 (개선된 로직)"""
        cells = row.find_all('td')
        if len(cells) < 6:
            return None
        
        full_text = row.get_text(strip=True)
        
        # 금고 코드 추출 (5자리 숫자)
        code_match = re.search(r'^(\d{5})', full_text)
        if not code_match:
            return None
        
        bank_data = {'gmgoCd': code_match.group(1)}
        
        # 금고명 추출 및 중복 제거
        name_match = re.search(r'^\d{5}([가-힣]+)', full_text)
        if not name_match:
            return None
        
        bank_name = BankParser._remove_duplicates(name_match.group(1))
        
        # 지점명 추출
        branch_match = re.search(r'\(([^)]+)\)', full_text)
        branch_name = branch_match.group(1) if branch_match else ""
        
        # 최종 금고명 구성
        bank_data['name'] = f"{bank_name}({branch_name})" if branch_name else bank_name
        
        # 전화번호 추출
        phone_match = re.search(r'(0\d{1,2}-\d{3,4}-\d{4})', full_text)
        bank_data['phone'] = phone_match.group(1) if phone_match else ""
        
        # 주소 추출 (개선된 패턴)
        address_pattern = r'(서울|인천|경기|부산|대구|광주|대전|울산|세종|강원|충북|충남|전북|전남|경북|경남|제주)[^\d]+?(?=\d{5}|$)'
        address_match = re.search(address_pattern, full_text)
        bank_data['address'] = address_match.group(0).strip() if address_match else ""
        
        # 분류 추출
        type_keywords = ['지역', '직장']
        bank_data['type'] = next((kw for kw in type_keywords if kw in full_text), "")
        
        return bank_data
    
    @staticmethod
    def _remove_duplicates(text: str) -> str:
        """중복된 텍스트 제거"""
        for divisor in [2, 3, 4]:
            if len(text) >= divisor and len(text) % divisor == 0:
                chunk_size = len(text) // divisor
                chunks = [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]
                if len(set(chunks)) == 1:
                    return chunks[0]
        return text


def parse_bank_list(html: str, city: str, district: str) -> List[Dict[str, Any]]:
    """
    새마을금고 은행 목록 HTML을 파싱하여 구조화된 데이터로 변환
    
    Args:
        html: HTML 내용
        city: 시/도명
        district: 구/군명
        
    Returns:
        은행 정보 딕셔너리 리스트
    """
    soup = BeautifulSoup(html, 'html.parser')
    banks = []
    parser = BankParser()
    
    try:
        # 테이블 행 찾기
        rows = soup.find_all('tr')
        if not rows:
            logger.warning(f"테이블 행을 찾을 수 없음: {city} {district}")
            return []
        
        for row in rows:
            # Hidden span 우선 시도
            bank_data = parser.extract_from_hidden_spans(row)
            
            # Hidden span이 없으면 텍스트 파싱
            if not bank_data:
                bank_data = parser.extract_from_text(row)
            
            if bank_data:
                # 공통 정보 추가
                bank_data.update({
                    'city': city,
                    'district': district,
                    'crawled_at': datetime.now().isoformat()
                })
                banks.append(bank_data)
        
        logger.debug(f"파싱 완료: {city} {district} - {len(banks)}개 은행")
        
    except Exception as e:
        logger.error(f"은행 목록 파싱 오류: {city} {district} - {e}")
    
    return banks


class InterestRateParser:
    """금리 정보 파서 클래스"""
    
    @staticmethod
    def extract_base_date(soup: BeautifulSoup) -> str:
        """기준일 추출"""
        date_selectors = [
            '.base-date',
            '.date',
            '[class*="date"]'
        ]
        
        for selector in date_selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)
        
        # 정규식으로 날짜 찾기
        date_pattern = r'\d{4}[-/]\d{1,2}[-/]\d{1,2}'
        date_match = soup.find(text=re.compile(date_pattern))
        return date_match.strip() if date_match else ""
    
    @staticmethod
    def parse_product_row(cells: List, product_type: str) -> Optional[Dict[str, Any]]:
        """상품 행 파싱"""
        if len(cells) < 2:
            return None
        
        product_name = cells[0].get_text(strip=True)
        
        # 상품 유형별 필터링
        if not InterestRateParser._is_valid_product(product_name, product_type):
            return None
        
        # 기간과 금리 추출
        duration_text, rate_text = InterestRateParser._extract_duration_and_rate(
            cells, product_type
        )
        
        # 숫자 변환
        duration = InterestRateParser._parse_duration(duration_text)
        rate = InterestRateParser._parse_rate(rate_text)
        
        # 유효성 검사
        if product_type == '요구불예탁금':
            if rate <= 0:
                return None
        elif duration <= 0 or rate <= 0:
            return None
        
        return {
            'product_name': product_name,
            'duration_months': duration,
            'interest_rate': rate,
            'duration_text': duration_text,
            'rate_text': rate_text,
            'product_type': product_type
        }
    
    @staticmethod
    def _is_valid_product(product_name: str, product_type: str) -> bool:
        """상품 유효성 검사"""
        valid_products = {
            '요구불예탁금': ['온라인자립예탁금', '상상모바일통장'],
            '거치식예탁금': ['MG더뱅킹정기예금'],
            '적립식예탁금': ['MG더뱅킹정기적금', 'MG더뱅킹자유적금']
        }
        
        if product_type not in valid_products:
            return False
        
        return any(name in product_name for name in valid_products[product_type])
    
    @staticmethod
    def _extract_duration_and_rate(cells: List, product_type: str) -> Tuple[str, str]:
        """기간과 금리 텍스트 추출"""
        if product_type == '요구불예탁금':
            # 요구불예탁금은 기간이 없음
            rate_text = cells[-1].get_text(strip=True) if cells else ""
            return "", rate_text
        else:
            # 거치식/적립식은 마지막 두 컬럼
            if len(cells) >= 2:
                duration_text = cells[-2].get_text(strip=True)
                rate_text = cells[-1].get_text(strip=True)
                return duration_text, rate_text
            return "", ""
    
    @staticmethod
    def _parse_duration(text: str) -> int:
        """기간 텍스트를 월 단위 숫자로 변환"""
        if not text:
            return 0
        
        # 숫자 추출
        match = re.search(r'(\d+)', text.replace('월', '').replace('이상', ''))
        return int(match.group(1)) if match else 0
    
    @staticmethod
    def _parse_rate(text: str) -> float:
        """금리 텍스트를 숫자로 변환"""
        if not text:
            return 0.0
        
        # 숫자 추출
        match = re.search(r'(\d+\.?\d*)', text.replace('%', ''))
        return float(match.group(1)) if match else 0.0


def parse_interest_rates(html: str, bank_info: Dict[str, Any], 
                        product_type: str = '요구불예탁금') -> List[Dict[str, Any]]:
    """
    금리 정보 HTML을 파싱하여 구조화된 데이터로 변환
    
    Args:
        html: HTML 내용
        bank_info: 은행 기본 정보
        product_type: 상품 유형
        
    Returns:
        상품 정보 리스트
    """
    soup = BeautifulSoup(html, 'html.parser')
    parser = InterestRateParser()
    
    try:
        # 기준일 추출
        base_date = parser.extract_base_date(soup)
        
        # 테이블 행 찾기
        table_selectors = [
            '.tblWrap #divTmp1 tbody tr',
            '.tblWrap tbody tr',
            'table tbody tr',
            '.rate-table tbody tr',
            'tbody tr'
        ]
        
        rows = []
        for selector in table_selectors:
            rows = soup.select(selector)
            if rows:
                break
        
        if not rows:
            logger.debug(f"금리 테이블을 찾을 수 없음: {bank_info.get('name', 'Unknown')}")
            return []
        
        # 상품 정보 추출
        products = []
        for row in rows:
            cells = row.find_all('td')
            product = parser.parse_product_row(cells, product_type)
            if product:
                products.append(product)
        
        # 중복 제거
        unique_products = remove_duplicate_products(products)
        
        logger.debug(f"금리 파싱 완료: {bank_info.get('name')} - {len(unique_products)}개 상품")
        return unique_products
        
    except Exception as e:
        logger.error(f"금리 정보 파싱 오류: {bank_info.get('name', 'Unknown')} - {e}")
        return []


def remove_duplicate_products(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """중복 상품 제거"""
    seen = set()
    unique = []
    
    for product in products:
        key = (
            product['product_name'],
            product['duration_months'],
            product['interest_rate'],
            product.get('product_type', '')
        )
        if key not in seen:
            seen.add(key)
            unique.append(product)
    
    return unique


def parse_summary_data(rates_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    수집된 금리 데이터를 요약하여 통계 정보 생성
    
    Args:
        rates_data: 금리 데이터 리스트
        
    Returns:
        요약 통계 정보
    """
    if not rates_data:
        return create_empty_summary()
    
    total_banks = len(rates_data)
    all_products = []
    
    # 모든 상품 수집
    for rate_data in rates_data:
        if 'products' in rate_data:
            all_products.extend(rate_data['products'])
    
    if not all_products:
        return create_empty_summary(total_banks)
    
    # 통계 계산
    stats = calculate_statistics(all_products)
    
    return {
        'total_banks': total_banks,
        'total_products': len(all_products),
        'average_rate': stats['average_rate'],
        'rate_range': stats['rate_range'],
        'duration_stats': stats['duration_stats'],
        'product_type_stats': stats['product_type_stats'],
        'crawled_at': datetime.now().isoformat()
    }


def create_empty_summary(total_banks: int = 0) -> Dict[str, Any]:
    """빈 요약 데이터 생성"""
    return {
        'total_banks': total_banks,
        'total_products': 0,
        'average_rate': 0.0,
        'min_rate': 0.0,
        'max_rate': 0.0,
        'rate_range': {'min': 0.0, 'max': 0.0},
        'duration_stats': {},
        'product_type_stats': {},
        'crawled_at': datetime.now().isoformat()
    }


def calculate_statistics(products: List[Dict[str, Any]]) -> Dict[str, Any]:
    """상품 통계 계산"""
    rates = [p['interest_rate'] for p in products if 'interest_rate' in p and p['interest_rate'] > 0]
    
    # 기본 통계
    stats = {
        'average_rate': round(sum(rates) / len(rates), 2) if rates else 0.0,
        'min_rate': round(min(rates), 2) if rates else 0.0,
        'max_rate': round(max(rates), 2) if rates else 0.0,
        'rate_range': {
            'min': round(min(rates), 2) if rates else 0.0,
            'max': round(max(rates), 2) if rates else 0.0
        },
        'duration_stats': {},
        'product_type_stats': {}
    }
    
    # 기간별 통계
    duration_groups = {}
    for product in products:
        duration = product.get('duration_months', 0)
        if duration not in duration_groups:
            duration_groups[duration] = []
        duration_groups[duration].append(product.get('interest_rate', 0))
    
    for duration, rates in duration_groups.items():
        if rates:
            stats['duration_stats'][duration] = {
                'count': len(rates),
                'average_rate': round(sum(rates) / len(rates), 2),
                'min_rate': round(min(rates), 2),
                'max_rate': round(max(rates), 2)
            }
    
    # 상품 유형별 통계
    type_groups = {}
    for product in products:
        ptype = product.get('product_type', 'Unknown')
        if ptype not in type_groups:
            type_groups[ptype] = []
        type_groups[ptype].append(product.get('interest_rate', 0))
    
    for ptype, rates in type_groups.items():
        if rates:
            stats['product_type_stats'][ptype] = {
                'count': len(rates),
                'average_rate': round(sum(rates) / len(rates), 2),
                'min_rate': round(min(rates), 2),
                'max_rate': round(max(rates), 2)
            }
    
    return stats