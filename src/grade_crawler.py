"""
새마을금고 경영실태평가 크롤러
매년 7월에 한 번만 수집하여 1년간 사용
"""

import requests
import time
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
try:
    from .config import GRADE_CONFIG, API_ENDPOINTS, GRADE_MAP
except ImportError:
    from config import GRADE_CONFIG, API_ENDPOINTS, GRADE_MAP


class GradeCrawler:
    """경영실태평가 크롤러 클래스"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://www.kfcc.co.kr/gumgo/regulardisclosure.do',
            'Origin': 'https://www.kfcc.co.kr',
            'Content-Type': 'application/x-www-form-urlencoded'
        })
    
    def should_collect_grades(self):
        """경영실태평가 수집이 필요한지 확인"""
        if not GRADE_CONFIG['enabled']:
            return False
        
        # 환경변수로 강제 실행 가능
        import os
        if os.getenv('FORCE_GRADE_COLLECTION', '').lower() == 'true':
            return True
        
        current_month = datetime.now().month
        collection_months = GRADE_CONFIG['collection_month']
        
        # collection_month가 리스트인지 단일 값인지 확인
        if isinstance(collection_months, list):
            return current_month in collection_months
        else:
            return current_month == collection_months
    
    def fetch_grade_for_bank(self, gmgo_cd, bank_name, city='', district=''):
        """특정 금고의 경영실태평가 데이터 수집"""
        url = API_ENDPOINTS['grade_evaluation']
        
        # 평가 기준일 생성 (YYYYMM 형식)
        evaluation_date = f"{GRADE_CONFIG['evaluation_year']}{GRADE_CONFIG['evaluation_month']:02d}"
        
        payload = {
            "procGbcd": "1",
            "pageNo": "",
            "gongsiGmgoid": "",
            "gmgocd": gmgo_cd,
            "hpageBrwsUm": "1",
            "gongsiDate": "",
            "strd_yymm": evaluation_date,
            "gmgoNm": "",
            "gonsiYear": "",
            "gonsiMonth": "",
        }
        
        for attempt in range(GRADE_CONFIG['retry_count']):
            try:
                response = self.session.post(
                    url,
                    data=payload,
                    timeout=GRADE_CONFIG['timeout']
                )
                response.raise_for_status()
                
                grade_data = self.parse_grade_data(response.text, gmgo_cd, bank_name, city, district)
                if grade_data:
                    print(f"✓ {bank_name}: 경영실태평가 수집 완료")
                    return grade_data
                else:
                    print(f"✗ {bank_name}: 경영실태평가 데이터 없음")
                    return None
                    
            except requests.exceptions.RequestException as e:
                print(f"✗ {bank_name} 경영실태평가 수집 실패 (시도 {attempt + 1}/{GRADE_CONFIG['retry_count']}): {e}")
                if attempt < GRADE_CONFIG['retry_count'] - 1:
                    time.sleep(GRADE_CONFIG['retry_delay'])
                else:
                    return None
            except Exception as e:
                print(f"✗ {bank_name} 경영실태평가 파싱 오류: {e}")
                return None
        
        return None
    
    def parse_grade_data(self, html, gmgo_cd, bank_name, city='', district=''):
        """경영실태평가 HTML 파싱"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # contentsdata input 태그에서 데이터 추출
            contents_input = soup.find("input", {"id": "contentsdata"})
            if not contents_input:
                return None
            
            data_str = contents_input.get("value")
            if not data_str:
                return None
            
            # 정규식으로 경영실태평가 데이터 추출
            # 패턴: 31000001 + (기관명) + | + (기준일) + | + (등급)
            pattern = re.compile(r"31000001([^\|]+)\|([0-9]{8})\|([0-9])")
            matches = pattern.findall(data_str)
            
            if not matches:
                return None
            
            # 가장 최근 평가 데이터 사용 (첫 번째 매치)
            기관명, 기준일, 등급코드 = matches[0]
            
            # 등급 정보 가져오기
            grade_info = GRADE_MAP.get(등급코드, {"name": "알수없음", "description": "등급 정보 없음"})
            
            return {
                'gmgo_cd': gmgo_cd,
                'bank_name': bank_name,
                'city': city,
                'district': district,
                'evaluation_agency': 기관명.strip(),
                'evaluation_date': 기준일,
                'grade_code': 등급코드,
                'grade_name': grade_info['name'],
                'grade_description': grade_info['description'],
                'collected_at': datetime.now().isoformat(),
                'evaluation_year': GRADE_CONFIG['evaluation_year'],
                'evaluation_month': GRADE_CONFIG['evaluation_month']
            }
            
        except Exception as e:
            print(f"❌ 경영실태평가 파싱 중 오류 발생 (금고: {bank_name}): {e}")
            return None
    
    def collect_all_grades(self, banks):
        """모든 금고의 경영실태평가 수집"""
        if not self.should_collect_grades():
            print("📅 경영실태평가 수집 시기가 아닙니다. (7월에만 수집)")
            return []
        
        print(f"📊 경영실태평가 수집 시작... (총 {len(banks)}개 금고)")
        
        all_grades = []
        successful_count = 0
        
        # 병렬 처리로 수집
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_bank = {
                executor.submit(self.fetch_grade_for_bank, bank['gmgoCd'], bank['name'], bank.get('city', ''), bank.get('district', '')): bank 
                for bank in banks
            }
            
            for future in as_completed(future_to_bank):
                bank = future_to_bank[future]
                try:
                    grade_data = future.result()
                    if grade_data:
                        all_grades.append(grade_data)
                        successful_count += 1
                except Exception as e:
                    print(f"✗ {bank['name']}: 경영실태평가 수집 중 오류 - {e}")
        
        print(f"📊 경영실태평가 수집 완료: {successful_count}/{len(banks)}개 금고")
        return all_grades
