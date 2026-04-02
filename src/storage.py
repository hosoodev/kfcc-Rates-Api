"""
데이터 저장 및 관리 모듈
크롤링된 데이터를 JSON 형식으로 저장하고 관리
"""

import os
import json
import re
import gzip
import shutil
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

from config import DATA_DIR, BANK_LIST_FILE, REGIONS
from parser import parse_summary_data, parse_summary_data_v2

logger = logging.getLogger(__name__)


class StorageManager:
    """데이터 저장소 관리 클래스"""
    
    def __init__(self, base_dir: str = None):
        """저장소 초기화"""
        # base_dir이 주어지면 (예: 'api-data') 루트로 사용. 없으면 기본 설정된 DATA_DIR의 상위 (프로젝트 루트)
        self.base_dir = Path(base_dir) if base_dir else Path(DATA_DIR).parent
        self.v2_dir = self.base_dir / "v2"
        self.daily_raw_dir = self.base_dir / "dailyRaw"
        
        # V2 기반 상세 경로
        self.meta_dir = self.v2_dir / "meta"
        self.grades_dir = self.v2_dir / "grades"
        self.branches_dir = self.v2_dir / "branches"
        self.rates_v2_dir = self.v2_dir / "rates"
        
        self.archive_rates_dir = self.base_dir / "_archive" / "rates"
        
        # 디렉토리 생성
        self._ensure_directories()
    
    def _ensure_directories(self) -> None:
        """필요한 디렉토리 생성"""
        dirs = [
            self.v2_dir, self.daily_raw_dir, self.meta_dir, 
            self.grades_dir, self.branches_dir, self.rates_v2_dir, 
            self.archive_rates_dir
        ]
        for directory in dirs:
            directory.mkdir(parents=True, exist_ok=True)
    
    def save_json(self, data: Any, filepath: Union[str, Path], 
                  compress: bool = False, pretty: bool = True) -> bool:
        """
        데이터를 JSON 파일로 저장
        
        Args:
            data: 저장할 데이터
            filepath: 저장할 파일 경로
            compress: gzip 압축 여부
            pretty: 예쁘게 포맷팅할지 여부
            
        Returns:
            저장 성공 여부
        """
        filepath = Path(filepath)
        
        try:
            # 디렉토리 확인
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            # JSON 직렬화 옵션
            json_kwargs = {
                'ensure_ascii': False,
                'separators': (',', ': ') if pretty else (',', ':')
            }
            if pretty:
                json_kwargs['indent'] = 2
            
            if compress:
                # gzip 압축 저장
                filepath = filepath.with_suffix('.json.gz')
                with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                    json.dump(data, f, **json_kwargs)
            else:
                # 일반 저장
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(data, f, **json_kwargs)
            
            logger.info(f"✓ 파일 저장 완료: {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"✗ 파일 저장 실패 ({filepath}): {e}")
            return False
    
    def load_json(self, filepath: Union[str, Path]) -> Optional[Any]:
        """
        JSON 파일 로드
        
        Args:
            filepath: 로드할 파일 경로
            
        Returns:
            로드된 데이터 또는 None
        """
        filepath = Path(filepath)
        
        # gzip 파일 확인
        if not filepath.exists() and filepath.with_suffix('.json.gz').exists():
            filepath = filepath.with_suffix('.json.gz')
        
        if not filepath.exists():
            logger.debug(f"파일이 존재하지 않음: {filepath}")
            return None
        
        try:
            if filepath.suffix == '.gz':
                with gzip.open(filepath, 'rt', encoding='utf-8') as f:
                    return json.load(f)
            else:
                with open(filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
                    
        except Exception as e:
            logger.error(f"✗ 파일 로드 실패 ({filepath}): {e}")
            return None
    
    def save_bank_list(self, banks: List[Dict[str, Any]], target_dir: Optional[Path] = None) -> bool:
        """은행 목록 저장 (V2 Meta 저장)"""
        target_v2_dir = target_dir if target_dir else self.v2_dir
        if not banks:
            logger.warning("저장할 은행 목록이 없습니다")
            return False
            
        # 1. [V2 Meta] v2/meta/banks.json 저장 (계층 구조)
        hierarchical_banks = self._group_banks_hierarchically(banks)
        bank_data_v2 = {
            'metadata': {
                'total_groups': len(hierarchical_banks),
                'total_branches': len(banks),
                'crawled_at': datetime.now().isoformat(),
                'version': '2.0'
            },
            'banks': hierarchical_banks
        }
        
        meta_v2_dir = target_v2_dir / "meta"
        meta_v2_dir.mkdir(parents=True, exist_ok=True)
        meta_filepath = meta_v2_dir / "banks.json"
        
        success = self.save_json(bank_data_v2, meta_filepath)
        if success:
            self.save_json(bank_data_v2, meta_filepath, compress=True)
            logger.info(f"🏦 은행 목록 저장 완료: V2 Meta({len(hierarchical_banks)} groups)")
        
        return success

    def _group_banks_hierarchically(self, banks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """은행 목록을 gmgoCd 기준으로 그룹화하여 계층 구조 생성"""
        from collections import defaultdict
        groups = defaultdict(list)
        for b in banks:
            groups[b.get('gmgoCd')].append(b)
            
        hierarchical = []
        for gmgo_cd, entries in groups.items():
            # 1. 본점(Head Office) 찾기: 이름에 '본점'이 포함된 것 우선, 없으면 첫 번째
            head = next((e for e in entries if '본점' in e.get('name', '')), entries[0])
            
            # 2. 공통 그룹 정보 추출 (본점 이름에서 (본점) 등 수식어 제거)
            # (주)풍산안강공장(본점) 처럼 (주)로 시작하는 경우 대응
            full_name = head.get('name', '')
            if full_name.startswith('('):
                second_paren = full_name.find('(', 1)
                group_name = full_name[:second_paren].strip() if second_paren != -1 else full_name
            else:
                group_name = full_name.split('(')[0].strip()
            
            hierarchical.append({
                "gmgoCd": gmgo_cd,
                "group_name": group_name,
                "head_office": {
                    "name": head.get('name'),
                    "address": head.get('address'),
                    "phone": head.get('phone'),
                    "province": head.get('province'),
                    "district": head.get('district')
                },
                "branches": [
                    {
                        "name": e.get('name'),
                        "address": e.get('address'),
                        "phone": e.get('phone'),
                        "district": e.get('district')
                    } for e in entries if e.get('name') != head.get('name')
                ]
            })
            
        return sorted(hierarchical, key=lambda x: x['gmgoCd'])

    def load_banks(self) -> Optional[Dict[str, Any]]:
        """은행 목록 로드 (Meta 계층구조 로드 시 평면화하여 반환)"""
        try:
            # 1. v2 디렉토리 우선 확인
            meta_file = self.v2_dir / "meta" / "banks.json"
            
            if meta_file.exists():
                data = self.load_json(meta_file)
                if data and data.get('metadata', {}).get('version') == '2.0':
                    # V2 계층 구조를 평면화하여 크롤러 호환성 유지
                    flattened_banks = []
                    for group in data.get('banks', []):
                        head = group.get('head_office', {})
                        # 최소한의 데이터 재구성
                        flattened_banks.append({
                            "gmgoCd": group['gmgoCd'],
                            "name": head.get('name'),
                            "province": head.get('province'),
                            "district": head.get('district'),
                            "address": head.get('address'),
                            "phone": head.get('phone')
                        })
                    return {"banks": flattened_banks}
                return data
            
            return None
                
        except Exception as e:
            logger.error(f"은행 목록 로드 실패: {e}")
            
        return None
    
    def save_daily_rates(self, rates: List[Dict[str, Any]], 
                        date_str: Optional[str] = None) -> bool:
        """일별 금리 데이터 저장 (Archive 전용)"""
        if not rates:
            return False
        
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        # 데이터 구성
        rates_data = {
            'metadata': {
                'date': date_str,
                'total_banks': len(rates),
                'crawled_at': datetime.now().isoformat(),
                'version': '1.1'
            },
            'rates': rates
        }
        
        # 압축 옵션
        compress = len(rates) > 100
        
        # _archive/rates/ 저장
        archive_filepath = self.archive_rates_dir / f"{date_str}.json"
        success = self.save_json(rates_data, archive_filepath, compress=compress)
        
        if success:
            logger.info(f"📦 금리 데이터 아카이브 완료: {date_str}")
        
        return success
    
    def save_summary(self, summary_data: Dict[str, Any], 
                    date_str: Optional[str] = None) -> bool:
        """요약 정보 저장 (V2에 요약 로직이 있으므로 레거시는 스킵)"""
        return True
    
    def get_latest_rates(self) -> Optional[Dict[str, Any]]:
        """최신 금리 데이터 가져오기 (Archive 기준)"""
        if not self.archive_rates_dir.exists():
            return None
        
        # JSON 및 압축 파일 모두 검색
        rate_files = list(self.archive_rates_dir.glob('*.json')) + \
                    list(self.archive_rates_dir.glob('*.json.gz'))
        
        if not rate_files:
            return None
        
        # 최신 파일 선택
        latest_file = max(rate_files, key=lambda f: f.stem.replace('.json', ''))
        
        return self.load_json(latest_file)
    
    def get_rates_by_date(self, date_str: str) -> Optional[Dict[str, Any]]:
        """특정 날짜의 금리 데이터 가져오기 (Archive 기준)"""
        filepath = self.archive_rates_dir / f"{date_str}.json"
        return self.load_json(filepath)
    
    def list_available_dates(self) -> List[str]:
        """사용 가능한 날짜 목록 반환 (Archive 기준)"""
        if not self.archive_rates_dir.exists():
            return []
        
        # 파일명에서 날짜 추출
        dates = []
        for file in self.archive_rates_dir.glob('*.json*'):
            date_str = file.stem.replace('.json', '')
            try:
                # 날짜 형식 검증
                datetime.strptime(date_str, '%Y-%m-%d')
                dates.append(date_str)
            except ValueError:
                continue
        
        return sorted(dates, reverse=True)
    def save_grades(self, grades_data: List[Dict[str, Any]]) -> bool:
        """경영실태평가 데이터 저장 (V2 전용)"""
        try:
            # 1. V2 v2/grades 디렉토리 생성 및 저장
            v2_grades_dir = self.v2_dir / "grades"
            v2_grades_dir.mkdir(parents=True, exist_ok=True)
            
            # 파일명 결정
            if grades_data:
                evaluation_year = grades_data[0].get('evaluation_year', datetime.now().year)
                evaluation_month = grades_data[0].get('evaluation_month', 12)
                filename = f"grades_{evaluation_year}_{evaluation_month:02d}.json"
            else:
                current_year = datetime.now().year
                current_month = datetime.now().month
                filename = f"grades_{current_year}_{current_month:02d}.json"
            
            # 데이터 구성
            data = {
                "collection_info": {
                    "collected_at": datetime.now().isoformat(),
                    "total_banks": len(grades_data),
                    "evaluation_year": grades_data[0].get('evaluation_year') if grades_data else None,
                    "evaluation_month": grades_data[0].get('evaluation_month') if grades_data else None
                },
                "grades": grades_data
            }
            
            # V2 경로 저장
            filepath = v2_grades_dir / filename
            success = self.save_json(data, filepath)
            success &= self.save_json(data, filepath, compress=True)
            
            if success:
                logger.info("경영실태평가 데이터 저장 완료 (V2)")
                # 인덱스 파일 갱신
                self.update_grades_index()
            return success
            
        except Exception as e:
            logger.error(f"경영실태평가 데이터 저장 실패: {e}")
            return False
    
    def load_grades(self, year: int = None, month: int = None) -> Optional[Dict[str, Any]]:
        """경영실태평가 데이터 로드 (v2/grades/ 확인)"""
        try:
            # 1. v2 디렉토리 확인
            grades_dir = self.v2_dir / "grades"
            
            if not grades_dir.exists():
                return None
                
            if year is None or month is None:
                # 저장된 파일 중 가장 최신의 파일을 찾아 로드
                grade_files = list(grades_dir.glob("grades_*_*.json"))
                if not grade_files:
                    return None
                        
                grade_files.sort(reverse=True)
                filepath = grade_files[0]
            else:
                filepath = grades_dir / f"grades_{year}_{month:02d}.json"
                
                if not filepath.exists():
                    return None
            
            if not filepath.exists():
                return None
            
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
                
        except Exception as e:
            print(f"❌ 경영실태평가 데이터 로드 실패: {e}")
            return None
    
    def get_grade_by_gmgo_cd(self, gmgo_cd: str, year: int = None, month: int = None) -> Optional[Dict[str, Any]]:
        """특정 금고 코드의 경영실태평가 데이터 가져오기"""
        grades_data = self.load_grades(year, month)
        if not grades_data:
            return None
        
        for grade in grades_data.get('grades', []):
            if grade.get('gmgo_cd') == gmgo_cd:
                return grade
        
        return None
    
    def build_v2_api(self, rates: List[Dict[str, Any]], grades: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        V2 API용 초경량 해시맵 데이터 생성
        - deposit.json (거치식)
        - saving.json (적립식)
        - demand.json (요구불/파킹)
        """
        now_iso = datetime.now().isoformat()
        
        # 금고 코드별 경영지표 매핑
        grade_map = {g['gmgo_cd']: g for g in grades}
        
        # V2 데이터 구조 초기화
        v2_data = {
            "deposit": {"updated_at": now_iso, "data": []},
            "saving": {"updated_at": now_iso, "data": []},
            "demand": {"updated_at": now_iso, "data": []}
        }

        # 각 타입별로 데이터를 분류하여 저장하기 위한 임시 맵 {type: {gmgoCd: bank_data}}
        temp_maps = {
            "deposit": {},
            "saving": {},
            "demand": {}
        }

        for entry in rates:
            bank_info = entry.get('bank', {})
            gmgo_cd = bank_info.get('gmgoCd')
            grade_info = grade_map.get(gmgo_cd, {})
            
            # 기본 정보 템플릿
            def get_base_info():
                bis_val = grade_info.get('bis_ratio')
                if bis_val:
                    try:
                        bis_val = float(str(bis_val).replace(',', ''))
                    except ValueError:
                        bis_val = None
                
                # 경영지표(공시) 정보가 있는 경우 별도 필드로 그룹화
                disclosure = None
                if grade_info:
                    # 배당률(dividend_rate) 처리
                    div_val = grade_info.get('dividend_rate')
                    if div_val:
                        try:
                            div_val = float(str(div_val).replace('%', '').replace(',', ''))
                        except ValueError:
                            div_val = None

                    disclosure = {
                        "evaluation_date": grade_info.get('evaluation_date'), # 공시 기준일
                        "grade": grade_info.get('grade_code'),
                        "bis_ratio": bis_val,
                        "dividend_rate": div_val
                    }

                return {
                    "gmgoCd": gmgo_cd,
                    "name": bank_info.get('name'),
                    "province": bank_info.get('province'),
                    "district": bank_info.get('district'),
                    "disclosure": disclosure, # 등급 및 BIS 비율 통합
                    "products": {}
                }

            # 상품 분류 및 데이터 구조화
            for product in entry.get('products', []):
                p_name = product.get('product_name', 'Unknown')
                p_type = product.get('product_type', '거치식예탁금')
                
                # V2 스키마 분류
                schema_key = "deposit"
                if "적립식" in p_type:
                    schema_key = "saving"
                elif "요구불" in p_type:
                    schema_key = "demand"

                # 해당 스키마 맵에 금고가 없으면 추가
                if gmgo_cd not in temp_maps[schema_key]:
                    temp_maps[schema_key][gmgo_cd] = get_base_info()
                
                bank_entry = temp_maps[schema_key][gmgo_cd]
                
                if p_name not in bank_entry["products"]:
                    bank_entry["products"][p_name] = {}
                
                # 금리 정보 (개월수 기준 해시맵)
                month = str(product.get('duration_months', 0))
                bank_entry["products"][p_name][month] = {
                    "r": product.get('interest_rate', 0),
                    "s": "w" # Web source
                }

        # 맵을 리스트로 변환
        for key in v2_data:
            v2_data[key]["data"] = list(temp_maps[key].values())

        return v2_data

    def upsert_mbank_patch(self, v2_data: Dict[str, Any], mbank_rates: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        모바일 크롤링 데이터를 V2 API 데이터에 덮어쓰기 (Upsert)
        - mbank_rates 구조: [{"gmgoCd": "...", "prdtNm": "...", "rate": 5.5, "month": 12}, ...]
        """
        # updated_at 갱신
        v2_data["updated_at"] = datetime.now().isoformat()
        
        # 금고 코드별로 빠르게 찾기 위해 매핑
        bank_map = {bank["gmgoCd"]: bank for bank in v2_data["data"]}
        
        updated_count = 0
        for patch in mbank_rates:
            gmgo_cd = patch.get("gmgoCd")
            prdt_nm = patch.get("prdtNm")
            rate = patch.get("rate")
            month = str(patch.get("month", "12"))
            
            if gmgo_cd in bank_map:
                bank = bank_map[gmgo_cd]
                if prdt_nm not in bank["products"]:
                    bank["products"][prdt_nm] = {}
                
                # 기존 데이터 덮어쓰기 및 출처 변경
                bank["products"][prdt_nm][month] = {
                    "r": rate,
                    "s": "m" # mBank source
                }
                updated_count += 1
        
        logger.info(f"📱 모바일 데이터 패치 완료: {updated_count}건 업데이트")
        return v2_data

    def _build_top_mobile_rates(self, data_list: List[Dict[str, Any]], month_keys: List[str], target_products: List[str] = None) -> Dict[str, Any]:
        """
        모바일 금리 중 상위 N개를 선별 (all.json 수준의 전체 뱅크 객체 구조 유지)
        """
        now_iso = datetime.now().isoformat()
        result = {
            "updated_at": now_iso
        }
        for m in month_keys:
            # 'all' -> 'data'로 필드명 통일 (all.json 호환성)
            result[m] = {"data": [], "regions": {}}
            
        temp_data = {m: {} for m in month_keys}
        
        # 정렬 키 헬퍼 함수
        def get_rank_sort_key(bank_data):
            r = bank_data.get("_r", 0)
            disc = bank_data.get("disclosure", {}) or {}
            
            # 1. r (DESC), 2. grade (ASC), 3. bis (DESC), 4. div (DESC), 5. name (ASC)
            grade = float(disc.get("grade") or 99)
            bis = float(disc.get("bis_ratio") or 0)
            div = float(disc.get("dividend_rate") or 0)
            name = bank_data.get("name", "")
            
            return (-r, grade, -bis, -div, name)
        
        for bank in data_list:
            gmgo_cd = bank.get("gmgoCd")
            products = bank.get("products", {})
            
            for prdt_name, months_data in products.items():
                if target_products and prdt_name not in target_products:
                    continue
                    
                for month in month_keys:
                    if month in months_data:
                        rate_info = months_data[month]
                        # 모바일 소스(s=="m")인 경우만 포함
                        if rate_info.get("r", 0) > 0 and rate_info.get("s") == "m":
                            # 랭킹 정렬을 위해 임시 필드 포함한 뱅크 객체 복사
                            entry = bank.copy()
                            entry["_r"] = rate_info.get("r")      # 정렬용 임시 필드
                            entry["_prdt"] = prdt_name           # 정보용 임시 필드
                            
                            if "data" not in temp_data[month]:
                                temp_data[month]["data"] = []
                            temp_data[month]["data"].append(entry)
                            
                            region = bank.get("region")
                            if region:
                                if region not in temp_data[month]:
                                    temp_data[month][region] = []
                                temp_data[month][region].append(entry)
                                
        for month in month_keys:
            # 1. 전체(data) 랭킹 처리
            all_list = temp_data[month].get("data", [])
            unique_all = {item['gmgoCd']: item for item in all_list}.values() # 금고당 최고 금리 상품 하나만
            
            final_all = sorted(list(unique_all), key=get_rank_sort_key)
            result[month]["data"] = final_all[:20]
            
            # 2. 지역별 랭킹 처리
            for rgn, rgn_list in temp_data[month].items():
                if rgn == "data": continue
                
                unique_rgn = {item['gmgoCd']: item for item in rgn_list}.values()
                final_rgn = sorted(list(unique_rgn), key=get_rank_sort_key)
                result[month]["regions"][rgn] = final_rgn[:10]
                
        return result

    def _filter_mbank_only(self, v2_data: Dict[str, Any]) -> Dict[str, Any]:
        """v2 데이터에서 모바일 소스(s: "m")인 데이터만 추출하여 동일한 형식으로 반환"""
        filtered_data = {
            "updated_at": v2_data.get("updated_at", datetime.now().isoformat()),
            "data": []
        }
        
        for bank in v2_data.get("data", []):
            new_bank = bank.copy()
            new_products = {}
            for prdt_name, months_data in bank.get("products", {}).items():
                mbank_months = {m: d for m, d in months_data.items() if d.get("s") == "m"}
                if mbank_months:
                    new_products[prdt_name] = mbank_months
            
            if new_products:
                new_bank["products"] = new_products
                filtered_data["data"].append(new_bank)
        
        return filtered_data

    def _get_district_slug(self, province_name: str, district_name: str) -> str:
        """한글 구명을 영문 슬러그로 변환 (SEO용, 시도 컨텍스트 포함)"""
        if not district_name:
            return "etc"
        
        # 1. 시도 정보 탐색
        province_info = REGIONS.get(province_name)
        if not province_info:
            return "etc"
            
        # 2. 시도 내 구 매핑 확인
        districts = province_info.get("districts", {})
        if district_name in districts:
            return districts[district_name]
            
        # 3. 매핑되지 않은 경우 접미사 제거 시도
        name = district_name.strip()
        if len(name) > 1 and name[-1] in ["시", "군", "구"]:
            name = name[:-1]
            # 접미사 제거 버전으로 재탐색
            for k, v in districts.items():
                if k.startswith(name):
                    return v

        return "etc"

    def build_seo_regions_api(self, v2_data_all: Dict[str, Dict[str, Any]], target_dir: Optional[Path] = None) -> None:
        """
        지역별(시도/시군구) SEO용 정적 JSON 파일 생성
        - v2/rates/{type}/regions/{province_slug}/all.json
        - v2/rates/{type}/regions/{province_slug}/{district_slug}.json
        """
        target_v2_dir = target_dir if target_dir else self.v2_dir
        v2_rates_dir = target_v2_dir / "rates"
        
        for p_type, wrapper in v2_data_all.items():
            src_data = wrapper.get("data", [])
            if not src_data:
                continue
                
            # 1. 지역별 그룹화
            # grouped[province][district] = [bank1, bank2, ...]
            grouped = {}
            for bank in src_data:
                province = bank.get("province", "기타")
                district = bank.get("district", "기타")
                
                if province not in grouped:
                    grouped[province] = {}
                if district not in grouped[province]:
                    grouped[province][district] = []
                    
                grouped[province][district].append(bank)
            
            # 2. 그룹별 정렬 및 파일 저장
            regions_base_dir = v2_rates_dir / p_type / "regions"
            
            # 정렬 키 함수 (Rate, Grade, BIS, Dividend, Name 순)
            def get_rate_sort_key(bank_data):
                products = bank_data.get("products", {})
                max_rate = 0.0
                if p_type in ["deposit", "saving"]:
                    for p_name, months in products.items():
                        if "12" in months:
                            max_rate = max(max_rate, float(months["12"].get("r", 0)))
                        else:
                            for m_data in months.values():
                                max_rate = max(max_rate, float(m_data.get("r", 0)))
                else: # demand
                    for p_name, months in products.items():
                        for m_data in months.values():
                            max_rate = max(max_rate, float(m_data.get("r", 0)))
                
                disc = bank_data.get("disclosure", {}) or {}
                grade = float(disc.get("grade") or 99)
                bis = float(disc.get("bis_ratio") or 0)
                div = float(disc.get("dividend_rate") or 0)
                name = bank_data.get("name", "")
                
                return (-max_rate, grade, -bis, -div, name)

            updated_at = datetime.now().isoformat()

            for province, districts in grouped.items():
                province_info = REGIONS.get(province, {"slug": "etc"})
                province_slug = province_info.get("slug", "etc")
                province_dir = regions_base_dir / province_slug
                province_dir.mkdir(parents=True, exist_ok=True)
                
                all_province_banks = []
                
                for district, banks in districts.items():
                    district_slug = self._get_district_slug(province, district)
                    
                    # 시군구별 정렬
                    sorted_district_banks = sorted(banks, key=get_rate_sort_key, reverse=True)
                    all_province_banks.extend(sorted_district_banks)
                    
                    # district_slug.json 저장
                    district_filepath = province_dir / f"{district_slug}.json"
                    district_data = {
                        "updated_at": updated_at,
                        "province": province,
                        "district": district,
                        "data": sorted_district_banks
                    }
                    self.save_json(district_data, district_filepath, pretty=False)
                
                # 시도 전체(all.json) 정렬 및 저장
                sorted_province_banks = sorted(all_province_banks, key=get_rate_sort_key, reverse=True)
                province_all_filepath = province_dir / "all.json"
                province_data = {
                    "updated_at": updated_at,
                    "province": province,
                    "data": sorted_province_banks
                }
                self.save_json(province_data, province_all_filepath, pretty=False)

            logger.info(f"📍 V2 SEO Regional API [{p_type}] 생성 완료")

    def build_main_page_api(self, v2_data_all: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        프론트엔드 메인 페이지 전용 BFF API 생성 (Top 15 정렬)
        - 원본 데이터 스키마를 유지하면서 카테고리별 금리 상위 15개만 추출
        """
        now_iso = datetime.now().isoformat()
        main_api = {
            "updated_at": now_iso,
            "deposit": [],
            "saving": [],
            "demand": []
        }
        
        for key in ["deposit", "saving", "demand"]:
            if key not in v2_data_all:
                continue
                
            src_data = v2_data_all[key].get("data", [])
            
            # 정렬 키 생성 함수
            def get_sort_key(bank_data):
                products = bank_data.get("products", {})
                max_rate = 0.0
                
                if key in ["deposit", "saving"]:
                    # 12개월("12") 금리 우선, 없으면 해당 금고의 상품 중 최대 금리 사용
                    for p_name, months in products.items():
                        if "12" in months:
                            max_rate = max(max_rate, float(months["12"].get("r", 0)))
                        else:
                            # 12개월이 없을 경우 모든 개월 수 중 최댓값
                            for m_data in months.values():
                                max_rate = max(max_rate, float(m_data.get("r", 0)))
                else:
                    # demand(자유입출금)는 개월 수가 무의미하므로 전체 상품 중 최대 금리 사용
                    for p_name, months in products.items():
                        for m_data in months.values():
                            max_rate = max(max_rate, float(m_data.get("r", 0)))
                
                disc = bank_data.get("disclosure", {}) or {}
                grade = float(disc.get("grade") or 99)
                bis = float(disc.get("bis_ratio") or 0)
                div = float(disc.get("dividend_rate") or 0)
                name = bank_data.get("name", "")
                
                return (-max_rate, grade, -bis, -div, name)

            # 금리 중심 다중 조건 정렬 후 상위 15개 추출
            sorted_data = sorted(src_data, key=get_sort_key)
            main_api[key] = sorted_data[:15]
            
        return main_api

    def save_v2_summary(self, v2_data_all: Dict[str, Dict[str, Any]], target_dir: Optional[Path] = None):
        """v2_data_all을 기반으로 summary.json 생성 및 저장"""
        target_dir_for_summary = target_dir if target_dir else self.v2_dir
        rates_v2 = []
        
        for p_type_key, wrapper in v2_data_all.items():
            for bank_data in wrapper.get("data", []):
                products_v1 = []
                for p_name, months_data in bank_data.get("products", {}).items():
                    for month, m_data in months_data.items():
                        # p_type_key를 기반으로 product_type 한글 명칭 복원 (parse_summary_data_v2 분류용)
                        p_type_name = "거치식예탁금"
                        if p_type_key == "saving": p_type_name = "적립식예탁금"
                        elif p_type_key == "demand": p_type_name = "요구불예탁금"
                        
                        products_v1.append({
                            "product_name": p_name,
                            "product_type": p_type_name,
                            "interest_rate": m_data.get("r", 0),
                            "duration_months": int(month),
                            "s": m_data.get("s", "w")
                        })
                
                # 등급 정보 추출 (disclosure 필드가 있으면 거기서, 없으면 평면 구조에서 - 마이그레이션 대응)
                disclosure = bank_data.get("disclosure", {})
                grade = disclosure.get("grade") if disclosure else bank_data.get("grade")
                
                rates_v2.append({
                    "gmgoCd": bank_data.get("gmgoCd"),
                    "name": bank_data.get("name"),
                    "grade": grade,
                    "products": products_v1
                })
        
        summary_v2 = parse_summary_data_v2(rates_v2)
        v2_summary_path = target_dir_for_summary / "rates" / "summary.json"
        v2_summary_path.parent.mkdir(parents=True, exist_ok=True)
        
        if self.save_json(summary_v2, v2_summary_path, pretty=False):
            logger.info(f"📊 V2 Dashboard Summary 저장 완료: {v2_summary_path}")
        else:
            logger.error(f"❌ V2 Dashboard Summary 저장 실패: {v2_summary_path}")

    def save_v2_api(self, v2_data_all: Dict[str, Dict[str, Any]], target_dir: Optional[Path] = None) -> bool:
        """
        V2 API 데이터를 파일로 저장
        - target_dir이 지정되면 해당 디렉토리에 저장 (v2/ 또는 dailyRaw/)
        - v2/rates/deposit/all.json, v2/rates/saving/all.json, v2/rates/demand/all.json
        - v2/rates/deposit/mbank.json 등 모바일 전용 데이터 포함
        - v2/main.json (BFF API)
        """
        try:
            target_v2_dir = target_dir if target_dir else self.v2_dir
            v2_rates_dir = target_v2_dir / "rates"
            v2_rates_dir.mkdir(parents=True, exist_ok=True)
            
            success = True
            for key, data in v2_data_all.items():
                # 각각의 라우트별 폴더 생성 (deposit, saving, demand)
                product_dir = v2_rates_dir / key
                product_dir.mkdir(parents=True, exist_ok=True)
                
                # 1. all.json 저장
                filepath = product_dir / "all.json"
                success &= self.save_json(data, filepath, pretty=False)

                # 2. mbank.json 저장 (모바일 전용)
                mbank_data = self._filter_mbank_only(data)
                mbank_filepath = product_dir / "mbank.json"
                if self.save_json(mbank_data, mbank_filepath, pretty=False):
                    logger.info(f"📱 V2 {key} 모바일 전용 데이터 저장 완료: {mbank_filepath}")
                else:
                    success = False
            
            # 3. main.json 저장 (BFF API)
            main_api_data = self.build_main_page_api(v2_data_all)
            main_filepath = target_v2_dir / "main.json"
            if self.save_json(main_api_data, main_filepath, pretty=False):
                logger.info(f"🏠 V2 Main Page BFF API 저장 완료: {main_filepath}")
            else:
                success = False

            # 4. summary.json 저장 (Dashboard 요약)
            try:
                self.save_v2_summary(v2_data_all)
            except Exception as e:
                logger.error(f"⚠️ V2 요약 데이터 생성 중 오류 발생: {e}")
                success = False

            # 5. 지역별 SEO API 생성
            try:
                self.build_seo_regions_api(v2_data_all, target_dir=target_v2_dir)
            except Exception as e:
                logger.error(f"⚠️ SEO 지역별 API 생성 중 오류 발생: {e}")
                success = False

            if success:
                logger.info(f"🚀 V2 API 기본 데이터 저장 완료: {v2_rates_dir}")
            
            top_configs = [
                ("deposit", ["3", "6", "12"], ["MG더뱅킹정기예금"], "m.json"),
                ("saving", ["3", "6", "12"], ["MG더뱅킹정기적금", "MG더뱅킹자유적금"], "m.json"),
                ("demand", ["0"], ["상상모바일통장"], "m.json")
            ]
            
            for key, m_keys, products, filename in top_configs:
                if key in v2_data_all:
                    src_data = v2_data_all[key].get("data", [])
                    top_mobile_data = self._build_top_mobile_rates(src_data, m_keys, products)
                    
                    top_dir = v2_rates_dir / key / "top"
                    top_dir.mkdir(parents=True, exist_ok=True)
                    
                    top_filepath = top_dir / filename
                    
                    if self.save_json(top_mobile_data, top_filepath, pretty=False):
                        logger.info(f"✨ V2 {key} Top 금리 API 저장 완료: {top_filepath}")
                    else:
                        success = False
            # 6. 개별 지점 상세 API 생성
            try:
                self.build_branch_detail_api(v2_data_all, target_dir=target_v2_dir)
            except Exception as e:
                logger.error(f"⚠️ 지점 상세 API 생성 중 오류 발생: {e}")

            return success
        except Exception as e:
            logger.error(f"❌ V2 API 데이터 저장 실패: {e}")
            return False
    
    def build_branch_detail_api(self, v2_data_all: Dict[str, Dict[str, Any]], target_dir: Optional[Path] = None) -> bool:
        """
        개별 지점 상세 페이지용 정적 API 파일 생성
        v2/branches/{gmgoCd}.json 또는 dailyRaw/branches/{gmgoCd}.json
        """
        try:
            target_v2_dir = target_dir if target_dir else self.v2_dir
            # 1. 메타데이터 로드 및 지역별 그룹화
            # V2 메타데이터 폴더 (api-data/v2/meta/banks.json 예상)
            banks_file = target_v2_dir / "meta" / "banks.json"
            if not banks_file.exists():
                logger.error(f"❌ 메타데이터 파일을 찾을 수 없습니다: {banks_file}")
                return False
            
            with open(banks_file, "r", encoding="utf-8") as f:
                banks_data = json.load(f)
            
            bank_map = {}
            district_groups = {}
            for bank in banks_data.get("banks", []):
                gmgo_cd = bank.get("gmgoCd")
                if not gmgo_cd: continue
                bank_map[gmgo_cd] = bank
                # 지역 그룹핑 (head_office.district 기준)
                head = bank.get("head_office", {})
                dist = head.get("district")
                if dist:
                    if dist not in district_groups:
                        district_groups[dist] = []
                    district_groups[dist].append({
                        "gmgoCd": gmgo_cd, 
                        "name": bank.get("group_name"),
                        "province": head.get("province"),
                        "district": dist
                    })

            # 2. 금리 데이터 인덱싱
            rates_index = {"deposit": {}, "saving": {}, "demand": {}}
            for key in ["deposit", "saving", "demand"]:
                if key in v2_data_all:
                    for bank_rate in v2_data_all[key].get("data", []):
                        g_cd = bank_rate.get("gmgoCd")
                        if g_cd:
                            rates_index[key][g_cd] = bank_rate.get("products", {})

            # 3. 경영평가 히스토리 로드 (v2/grades/*.json)
            from collections import defaultdict
            disclosure_history_index = defaultdict(list)
            
            grades_dir = target_v2_dir / "grades"
            if grades_dir.exists():
                # 최신 파일순으로 정렬하여 로드
                for gf in sorted(grades_dir.glob("grades_*.json"), reverse=True):
                    match = re.search(r"grades_(\d{4})_(\d{2})", gf.name)
                    if not match: continue
                    
                    year, month = match.groups()
                    fallback_date = f"{year}-{month}-{'31' if month == '12' else '30'}"
                    
                    g_data = self.load_json(gf) or {}
                    for g in g_data.get("grades", []):
                        g_cd = g.get("gmgo_cd")
                        if g_cd:
                            disclosure_history_index[g_cd].append({
                                "evaluation_date": g.get('evaluation_date') or fallback_date,
                                "grade": g.get("grade_code"),
                                "bis_ratio": self._clean_float(g.get('bis_ratio')),
                                "dividend_rate": self._clean_float(g.get('dividend_rate'))
                            })

            # 4. 개별 지점 JSON 조립
            branches_dir = target_v2_dir / "branches"
            branches_dir.mkdir(parents=True, exist_ok=True)
            
            updated_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S+09:00")
            success_count = 0
            
            for gmgo_cd, meta in bank_map.items():
                try:
                    head_office = meta.get("head_office", {})
                    branch_detail = {
                        "updated_at": updated_at,
                        "gmgoCd": gmgo_cd,
                        "name": meta.get("group_name"),
                        "meta": {
                            "head_office": head_office,
                            "branches": meta.get("branches", [])
                        },
                        "rates": {
                            "deposit": rates_index["deposit"].get(gmgo_cd, {}),
                            "saving": rates_index["saving"].get(gmgo_cd, {}),
                            "demand": rates_index["demand"].get(gmgo_cd, {})
                        },
                        "top_picks": {
                            "deposit": self._get_best_rate(rates_index["deposit"].get(gmgo_cd, {})),
                            "saving": self._get_best_rate(rates_index["saving"].get(gmgo_cd, {}))
                        },
                        "disclosure_history": disclosure_history_index.get(gmgo_cd, []),
                        "nearby_branches": []
                    }
                    
                    # 주변 지점 추천 (같은 district, 최대 5개, 자기 자신 제외)
                    dist = head_office.get("district")
                    if dist and dist in district_groups:
                        nearby = [b for b in district_groups[dist] if b["gmgoCd"] != gmgo_cd]
                        branch_detail["nearby_branches"] = nearby[:5]
                    
                    # 파일 저장
                    filepath = branches_dir / f"{gmgo_cd}.json"
                    self.save_json(branch_detail, filepath, pretty=True)
                    success_count += 1
                except Exception as e:
                    logger.error(f"⚠️ 지점 {gmgo_cd} 데이터 조립 및 저장 실패: {e}")
            
            logger.info(f"📁 V2 Branch 상세 API {success_count}개 생성 완료: {branches_dir}")
            return True
        except Exception as e:
            logger.error(f"❌ Branch Detail API 생성 중 오류: {e}")
            return False

    def _get_best_rate(self, product_map: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """해당 지점에서 가장 높은 금리의 상품 추천용 데이터 추출"""
        best = None
        for p_name, terms in product_map.items():
            for term, info in terms.items():
                rate = info.get("r", 0)
                if best is None or rate > best["r"]:
                    best = {
                        "name": p_name,
                        "month": term,
                        "r": rate,
                        "s": info.get("s", "w")
                    }
        return best

    def _clean_float(self, value: Any) -> Optional[float]:
        """문자열(%, , 포함)을 float으로 안전하게 변환"""
        if value is None or value == "":
            return None
        try:
            return float(str(value).replace('%', '').replace(',', ''))
        except (ValueError, TypeError):
            return None

    def cleanup_old_data(self, days_to_keep: int = 30) -> int:
        """
        오래된 아카이브 데이터 파일 정리
        """
        if not self.archive_rates_dir.exists():
            return 0
        
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        removed_count = 0
        
        for file in self.archive_rates_dir.glob('*.json*'):
            try:
                date_str = file.stem.replace('.json', '')
                file_date = datetime.strptime(date_str, '%Y-%m-%d')
                
                if file_date < cutoff_date:
                    file.unlink()
                    removed_count += 1
                    logger.info(f"🗑️ 오래된 아카이브 삭제: {file.name}")
                    
            except (ValueError, OSError) as e:
                logger.warning(f"파일 처리 오류: {file.name} - {e}")
        
        if removed_count > 0:
            logger.info(f"🧹 정리 완료: {removed_count}개 아카이브 삭제")
        
        return removed_count
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """저장소 통계 정보 반환 (최신 V2 기준)"""
        stats = {
            'base_directory': str(self.base_dir),
            'v2_directory': str(self.v2_dir),
            'daily_raw_directory': str(self.daily_raw_dir),
            'available_dates': self.list_available_dates(),
            'total_archive_files': 0,
            'latest_archive_date': None,
            'storage_size_mb': 0
        }
        
        # 아카이브 파일 수 및 최신 날짜 계산
        if self.archive_rates_dir.exists():
            rate_files = list(self.archive_rates_dir.glob('*.json*'))
            stats['total_archive_files'] = len(rate_files)
            if rate_files:
                stats['latest_archive_date'] = max(
                    f.stem.replace('.json', '') for f in rate_files
                )
        
        # 전체 저장소 크기 계산
        total_size = 0
        try:
            for file in self.base_dir.rglob('*'):
                if file.is_file():
                    total_size += file.stat().st_size
        except Exception:
            pass
        
        stats['storage_size_mb'] = round(total_size / (1024 * 1024), 2)
        
        return stats

    def update_grades_index(self) -> bool:
        """v2/grades/index.json 생성 및 갱신"""
        try:
            grades_dir = self.v2_dir / "grades"
            if not grades_dir.exists():
                logger.warning(f"경영실태평가 디렉토리가 존재하지 않습니다: {grades_dir}")
                return False
                
            # JSON 파일 탐색 (인덱스 제외)
            grade_files = sorted(
                [f for f in grades_dir.glob("grades_*_*.json") if "index" not in f.name],
                reverse=True
            )
            
            versions = []
            for file in grade_files:
                data = self.load_json(file)
                if data and "collection_info" in data:
                    info = data["collection_info"]
                    grades = data.get("grades", [])
                    
                    # 요약 생성 (등급별 분포)
                    summary = {}
                    for g in grades:
                        code = g.get("grade_code", "unknown")
                        summary[code] = summary.get(code, 0) + 1
                    
                    # 정렬된 요약 (1, 2, 3, 4, 5 순)
                    sorted_summary = {k: summary[k] for k in sorted(summary.keys()) if k.isdigit()}
                    
                    versions.append({
                        "year": info.get("evaluation_year"),
                        "month": info.get("evaluation_month"),
                        "version": f"{info.get('evaluation_year')}-{info.get('evaluation_month'):02d}",
                        "filename": file.name,
                        "total_banks": info.get("total_banks"),
                        "summary": sorted_summary,
                        "collected_at": info.get("collected_at")
                    })
            
            index_data = {
                "metadata": {
                    "updated_at": datetime.now().isoformat(),
                    "total_versions": len(versions),
                    "description": "새마을금고 경영실태평가(등급) 수집 데이터 인덱스"
                },
                "versions": versions
            }
            
            index_path = grades_dir / "index.json"
            success = self.save_json(index_data, index_path)
            if success:
                logger.info(f"📊 경영실태평가 인덱스 갱신 완료: {len(versions)}개 버전")
            return success
            
        except Exception as e:
            logger.error(f"경영실태평가 인덱스 갱신 실패: {e}")
            return False


# 모듈 레벨 함수들 (기존 인터페이스 유지)
def save_all(banks: List[Dict[str, Any]], rates: List[Dict[str, Any]], 
            date_str: Optional[str] = None, base_dir: str = None) -> bool:
    """모든 데이터를 저장"""
    manager = StorageManager(base_dir=base_dir)
    if date_str is None:
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    logger.info("💾 데이터 저장 시작...")
    
    try:
        success = True
        
        # 1. 은행 목록 저장
        if banks:
            success &= manager.save_bank_list(banks)
            # dailyRaw에도 메타데이터 저장 (Patch 로드시 필요)
            success &= manager.save_bank_list(banks, target_dir=manager.daily_raw_dir)
        
        # 2. 금리 데이터 저장
        if rates:
            success &= manager.save_daily_rates(rates, date_str)
            
            # 3. 요약 데이터 저장
            summary = parse_summary_data(rates)
            success &= manager.save_summary(summary, date_str)

            # 4. V2 대시보드 요약 데이터 저장 (BFF 전용)
            try:
                # 최근 수집된 등급 데이터 로드
                grades_data = manager.load_grades()
                v2_data_all = manager.build_v2_api(rates, grades_data.get('grades', []) if grades_data else [])
                
                # 공개용 V2 요약 저장
                manager.save_v2_summary(v2_data_all)
                # dailyRaw용 요약 저장
                manager.save_v2_summary(v2_data_all, target_dir=manager.daily_raw_dir)
            except Exception as e:
                logger.error(f"⚠️ V2 요약 데이터 생성 중 오류 발생: {e}")
        
        if success:
            logger.info("✅ 모든 데이터 저장 완료")
        else:
            logger.warning("⚠️ 일부 데이터 저장 실패")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ 데이터 저장 중 오류 발생: {e}")
        return False

def get_latest_rates(base_dir: str = None) -> Optional[Dict[str, Any]]:
    """최신 금리 데이터를 가져옴"""
    return StorageManager(base_dir=base_dir).get_latest_rates()

def get_rates_by_date(date_str: str, base_dir: str = None) -> Optional[Dict[str, Any]]:
    """특정 날짜의 금리 데이터를 가져옴"""
    return StorageManager(base_dir=base_dir).get_rates_by_date(date_str)

def list_available_dates(base_dir: str = None) -> List[str]:
    """사용 가능한 날짜 목록을 반환"""
    return StorageManager(base_dir=base_dir).list_available_dates()

def cleanup_old_data(days_to_keep: int = 30, base_dir: str = None) -> int:
    """오래된 데이터 파일 정리"""
    return StorageManager(base_dir=base_dir).cleanup_old_data(days_to_keep)

def get_storage_stats(base_dir: str = None) -> Dict[str, Any]:
    """저장소 통계 정보 반환"""
    return StorageManager(base_dir=base_dir).get_storage_stats()