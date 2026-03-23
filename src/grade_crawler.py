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
from utils import generate_desktop_ua
from config import GRADE_CONFIG, API_ENDPOINTS, GRADE_MAP


class GradeCrawler:
    """경영실태평가 크롤러 클래스"""
    
    def __init__(self, base_dir: str = None):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': generate_desktop_ua(),
            'Referer': 'https://www.kfcc.co.kr/gumgo/regulardisclosure.do',
            'Origin': 'https://www.kfcc.co.kr',
            'Content-Type': 'application/x-www-form-urlencoded'
        })
        from storage import StorageManager
        self.storage = StorageManager(base_dir=base_dir)
    
    def should_collect_grades(self):
        """경영실태평가 수집이 필요한지 확인"""
        if not GRADE_CONFIG['enabled']:
            return False
        
        # 환경변수로 강제 실행 가능
        import os
        if os.getenv('FORCE_GRADE_COLLECTION', '').lower() == 'true':
            return True
        
        # GitHub Actions workflow_dispatch에서 collect_grades가 true인 경우
        if os.getenv('GITHUB_ACTIONS') == 'true' and os.getenv('GITHUB_EVENT_NAME') == 'workflow_dispatch':
            # GitHub Actions에서 전달된 입력값 확인
            collect_grades = os.getenv('INPUT_COLLECT_GRADES', '').lower()
            if collect_grades == 'true':
                return True
        
        current_month = datetime.now().month
        collection_months = GRADE_CONFIG['collection_month']
        
        # collection_month가 리스트인지 단일 값인지 확인
        if isinstance(collection_months, list):
            return current_month in collection_months
        else:
            return current_month == collection_months

    def fetch_grade_for_bank(self, gmgo_cd, bank_name, province='', district='', evaluation_date=None):
        """특정 금고의 경영실태평가 데이터 수집"""
        url = API_ENDPOINTS['grade_evaluation']
        
        # evaluation_date가 명시되지 않은 경우에만 자동 계산
        if not evaluation_date:
            # 평가 기준일 생성 (YYYYMM 형식) - 현재 월에 맞는 평가 월 선택
            current_month = datetime.now().month
            if current_month in [1, 2, 3, 4, 5, 6, 7]:
                # 1-7월: 6월 평가 기준
                evaluation_month = 6
            else:
                # 8-12월: 12월 평가 기준
                evaluation_month = 12
            
            evaluation_date = f"{GRADE_CONFIG['evaluation_year']}{evaluation_month:02d}"
        else:
            # 전달받은 날짜에서 월 정보 추출 (파싱 시 필드를 위해)
            # YYYYMM 형식에서 뒤의 2자리
            try:
                evaluation_month = int(evaluation_date[4:6])
            except:
                evaluation_month = 12
        
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
                
                grade_data = self.parse_grade_data(response.text, gmgo_cd, bank_name, province, district, evaluation_month=evaluation_month)
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
    
    def parse_grade_data(self, html, gmgo_cd, bank_name, province='', district='', evaluation_month=12):
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
            
            # BIS 비율 (자본적정성) 추출
            # 패턴: 25000001 + (기관명) + | + 위험가중자산대비자기자본비율 + | + (당기) + | + (전기) + | + (증감)
            bis_pattern = re.compile(r"25000001[^\|]+\|위험가중자산대비자기자본비율\|([^\|]+)")
            bis_matches = bis_pattern.findall(data_str)
            bis_ratio = bis_matches[0] if bis_matches else "0.00"
            
            # 출자배당율 추출
            # 패턴: 14000003출자배당율 + | + (당기) + | + (전기) + | + (증감)
            dividend_pattern = re.compile(r"14000003출자배당율\|([^\|]+)")
            dividend_matches = dividend_pattern.findall(data_str)
            dividend_rate = dividend_matches[0] if dividend_matches else "0.00"
            
            return {
                'gmgo_cd': gmgo_cd,
                'bank_name': bank_name,
                'province': province,
                'district': district,
                'evaluation_agency': 기관명.strip(),
                'evaluation_date': 기준일,
                'grade_code': 등급코드,
                'grade_name': grade_info['name'],
                'grade_description': grade_info['description'],
                'bis_ratio': bis_ratio,
                'dividend_rate': dividend_rate,
                'dividend_rate_year': 기준일[:4] if dividend_matches else None,
                'collected_at': datetime.now().isoformat(),
                'evaluation_year': 기준일[:4],
                'evaluation_month': evaluation_month
            }
            
        except Exception as e:
            print(f"❌ 경영실태평가 파싱 중 오류 발생 (금고: {bank_name}): {e}")
            return None
    
    def collect_all_grades(self, banks, evaluation_date=None):
        """모든 금고의 경영실태평가 수집"""
        if not evaluation_date and not self.should_collect_grades():
            print("📅 경영실태평가 수집 시기가 아닙니다. (1월 또는 7월에 수집)")
            return []
        
        # evaluation_date에서 월 추출 (6월인 경우 전년도 배당율 참조 필요)
        eval_month = 12
        eval_year = datetime.now().year
        if evaluation_date:
            eval_year = int(evaluation_date[:4])
            eval_month = int(evaluation_date[4:6])
        else:
            current_month = datetime.now().month
            eval_month = 6 if current_month in range(1, 8) else 12
            eval_year = eval_year # GRADE_CONFIG['evaluation_year'] (보통 current_year)

        print(f"📊 경영실태평가 데이터 수집 시작... (기준: {eval_year}년 {eval_month:02d}월, 총 {len(banks)}개 금고)")
        
        # 6월 공시인 경우 전년도 12월 배당율 데이터 로드
        prev_dividend_map = {}
        if eval_month == 6:
            try:
                import json
                # storage.py를 참조하여 v2/grades 폴더에서 이전 데이터 로드
                data_dir = self.storage.v2_dir / "grades"
                prev_file = data_dir / f"grades_{eval_year - 1}_12.json"
                
                if prev_file.exists():
                    print(f"📦 전년도 배당율 참조 데이터 로드: {prev_file}")
                    with open(prev_file, "r", encoding="utf-8") as f:
                        prev_data = json.load(f)
                        for g in prev_data.get("grades", []):
                            if g.get("dividend_rate") and g.get("gmgo_cd"):
                                prev_dividend_map[g["gmgo_cd"]] = {
                                    "rate": g["dividend_rate"],
                                    "year": g.get("evaluation_year", str(eval_year - 1))
                                }
                else:
                    print(f"⚠️ 전년도 배당율 데이터({prev_file.name})를 찾을 수 없습니다. 배당율이 0으로 표시될 수 있습니다.")
            except Exception as e:
                print(f"⚠️ 전년도 데이터 로드 중 오류: {e}")

        all_grades = []
        successful_count = 0
        
        # 병렬 처리로 수집
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_bank = {
                executor.submit(self.fetch_grade_for_bank, bank['gmgoCd'], bank['name'], bank.get('province', ''), bank.get('district', ''), evaluation_date=evaluation_date): bank 
                for bank in banks
            }
            
            for future in as_completed(future_to_bank):
                bank = future_to_bank[future]
                try:
                    grade_data = future.result()
                    if grade_data:
                        # 6월 공시인데 배당율이 없거나 0인 경우 전년도 데이터 매핑
                        gmgo_cd = grade_data['gmgo_cd']
                        # print(f"DEBUG: gmgo_cd={gmgo_cd}, eval_month={eval_month}, rate={grade_data.get('dividend_rate')}")
                        if eval_month == 6 and (not grade_data.get('dividend_rate') or grade_data['dividend_rate'] in ["0", "0.00"]):
                            if gmgo_cd in prev_dividend_map:
                                grade_data['dividend_rate'] = prev_dividend_map[gmgo_cd]['rate']
                                grade_data['dividend_rate_year'] = prev_dividend_map[gmgo_cd]['year']
                                print(f"  - {bank['name']}: 전년도 배당율 적용 ({grade_data['dividend_rate']}% from {grade_data['dividend_rate_year']})")
                            else:
                                print(f"  - {bank['name']}: 전년도 데이터에서 금고코드 {gmgo_cd}를 찾을 수 없습니다.")
                        
                        all_grades.append(grade_data)
                        successful_count += 1
                except Exception as e:
                    print(f"✗ {bank['name']}: 경영실태평가 수집 중 오류 - {e}")
        
        print(f"📊 경영실태평가 수집 완료: {successful_count}/{len(banks)}개 금고")
        return all_grades
