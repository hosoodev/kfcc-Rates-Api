"""
데이터 저장 및 관리 모듈
크롤링된 데이터를 JSON 형식으로 저장하고 관리
"""

import os
import json
import gzip
import shutil
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

from config import DATA_DIR, BANK_LIST_FILE
from parser import parse_summary_data

logger = logging.getLogger(__name__)


class StorageManager:
    """데이터 저장소 관리 클래스"""
    
    def __init__(self, base_dir: str = None):
        """저장소 초기화"""
        # base_dir이 주어지면 (예: 'api-data') 루트로 사용. 없으면 기본 설정된 DATA_DIR의 상위 (프로젝트 루트)
        self.base_dir = Path(base_dir) if base_dir else Path(DATA_DIR).parent
        self.data_dir = self.base_dir / "data"
        self.v2_dir = self.base_dir / "v2"
        
        self.rates_dir = self.data_dir / 'rates'
        self.grades_dir = self.data_dir / 'grades'
        self.backup_dir = self.data_dir / 'backups'
        self.bank_list_file = self.data_dir / "banks.json"
        
        # 디렉토리 생성
        self._ensure_directories()
    
    def _ensure_directories(self) -> None:
        """필요한 디렉토리 생성"""
        for directory in [self.data_dir, self.v2_dir, self.rates_dir, self.backup_dir]:
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
    
    def save_bank_list(self, banks: List[Dict[str, Any]]) -> bool:
        """은행 목록 저장 (Legacy 및 V2 Meta 병행 저장)"""
        if not banks:
            logger.warning("저장할 은행 목록이 없습니다")
            return False
        
        # 백업 생성
        self._create_backup(self.bank_list_file)
        
        # 1. [Legacy] data/banks.json 저장 (평면 구조 유지)
        unique_banks = self._remove_duplicate_banks(banks)
        bank_data_legacy = {
            'metadata': {
                'total_count': len(unique_banks),
                'unique_count': len(set(b['gmgoCd'] for b in unique_banks)),
                'crawled_at': datetime.now().isoformat(),
                'version': '1.1'
            },
            'banks': unique_banks
        }
        
        success = self.save_json(bank_data_legacy, self.bank_list_file)
        if success:
            self.save_json(bank_data_legacy, self.bank_list_file, compress=True)
            
        # 2. [V2 Meta] v2/meta/banks.json 저장 (계층 구조)
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
        
        meta_v2_dir = self.v2_dir / "meta"
        meta_v2_dir.mkdir(parents=True, exist_ok=True)
        meta_filepath = meta_v2_dir / "banks.json"
        
        meta_success = self.save_json(bank_data_v2, meta_filepath)
        if meta_success:
            self.save_json(bank_data_v2, meta_filepath, compress=True)
            
        if success:
            logger.info(f"🏦 은행 목록 저장 완료: Legacy({len(unique_banks)}) & V2 Meta({len(hierarchical_banks)} groups)")
        
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
            
            # 2. 공통 그룹 정보 추출 (본점 이름에서 괄호 부분 제거 등)
            group_name = head.get('name', '').split('(')[0].strip()
            
            hierarchical.append({
                "gmgoCd": gmgo_cd,
                "group_name": group_name,
                "head_office": {
                    "name": head.get('name'),
                    "address": head.get('address'),
                    "phone": head.get('phone'),
                    "city": head.get('city'),
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
            if not meta_file.exists():
                meta_file = self.data_dir / "meta" / "banks.json"
            
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
                            "city": head.get('city'),
                            "district": head.get('district'),
                            "address": head.get('address'),
                            "phone": head.get('phone')
                        })
                    return {"banks": flattened_banks}
                return data
            
            if self.bank_list_file.exists():
                return self.load_json(self.bank_list_file)
                
        except Exception as e:
            logger.error(f"은행 목록 로드 실패: {e}")
            
        return None
    
    def save_daily_rates(self, rates: List[Dict[str, Any]], 
                        date_str: Optional[str] = None) -> bool:
        """일별 금리 데이터 저장"""
        if not rates:
            logger.warning("저장할 금리 데이터가 없습니다")
            return False
        
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        filepath = self.rates_dir / f"{date_str}.json"
        
        # 백업 생성
        self._create_backup(filepath)
        
        # 요약 통계 생성
        summary = parse_summary_data(rates)
        
        # 데이터 구성
        rates_data = {
            'metadata': {
                'date': date_str,
                'total_banks': len(rates),
                'successful_banks': len([r for r in rates if r.get('total_products', 0) > 0]),
                'crawled_at': datetime.now().isoformat(),
                'version': '1.1'
            },
            'summary': summary,
            'rates': rates
        }
        
        # 압축 옵션 (큰 파일의 경우)
        compress = len(rates) > 100
        
        success = self.save_json(rates_data, filepath, compress=compress)
        if success:
            logger.info(f"💰 금리 데이터 저장 완료: {date_str} ({len(rates)}개 금고)")
        
        return success
    
    def save_summary(self, summary_data: Dict[str, Any], 
                    date_str: Optional[str] = None) -> bool:
        """요약 정보 저장"""
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        summary_file = self.data_dir / 'summary.json'
        
        # 기존 요약 데이터 로드
        existing_summary = self.load_json(summary_file) or {}
        
        # 새로운 데이터 추가
        existing_summary[date_str] = summary_data
        
        # 최근 90일치만 유지
        cutoff_date = datetime.now() - timedelta(days=90)
        existing_summary = {
            date: data for date, data in existing_summary.items()
            if datetime.strptime(date, '%Y-%m-%d') >= cutoff_date
        }
        
        success = self.save_json(existing_summary, summary_file)
        if success:
            # 레거시 호환: 압축본도 저장
            self.save_json(existing_summary, summary_file, compress=True)
        if success:
            logger.info(f"📊 요약 데이터 저장 완료: {date_str}")
        
        return success
    
    def _remove_duplicate_banks(self, banks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """중복 은행 제거"""
        seen = set()
        unique = []
        
        for bank in banks:
            key = bank.get('gmgoCd')
            if key and key not in seen:
                seen.add(key)
                unique.append(bank)
        
        if len(banks) != len(unique):
            logger.info(f"중복 제거: {len(banks)} → {len(unique)}개")
        
        return unique
    
    def _create_backup(self, filepath: Union[str, Path]) -> bool:
        """파일 백업 생성"""
        filepath = Path(filepath)
        
        if not filepath.exists():
            return False
        
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_name = f"{filepath.stem}_{timestamp}{filepath.suffix}"
            backup_path = self.backup_dir / backup_name
            
            shutil.copy2(filepath, backup_path)
            logger.debug(f"백업 생성: {backup_path}")
            
            # 레거시 호환: 백업 파일의 압축본도 생성 (.gz)
            try:
                gz_backup_path = backup_path.with_name(backup_path.name + '.gz')
                with open(backup_path, 'rb') as f_in, gzip.open(gz_backup_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                logger.debug(f"백업 압축본 생성: {gz_backup_path}")
            except Exception as gz_err:
                logger.warning(f"백업 압축본 생성 실패: {gz_err}")
            
            # 오래된 백업 정리 (7일 이상)
            self._cleanup_old_backups()
            
            return True
            
        except Exception as e:
            logger.warning(f"백업 생성 실패: {e}")
            return False
    
    def _cleanup_old_backups(self, days_to_keep: int = 7) -> None:
        """오래된 백업 파일 정리"""
        if not self.backup_dir.exists():
            return
        
        cutoff_time = datetime.now() - timedelta(days=days_to_keep)
        
        for backup_file in self.backup_dir.glob('*'):
            if backup_file.stat().st_mtime < cutoff_time.timestamp():
                backup_file.unlink()
                logger.debug(f"오래된 백업 삭제: {backup_file}")
    
    def get_latest_rates(self) -> Optional[Dict[str, Any]]:
        """최신 금리 데이터 가져오기"""
        if not self.rates_dir.exists():
            return None
        
        # JSON 및 압축 파일 모두 검색
        rate_files = list(self.rates_dir.glob('*.json')) + \
                    list(self.rates_dir.glob('*.json.gz'))
        
        if not rate_files:
            return None
        
        # 최신 파일 선택
        latest_file = max(rate_files, key=lambda f: f.stem.replace('.json', ''))
        
        return self.load_json(latest_file)
    
    def get_rates_by_date(self, date_str: str) -> Optional[Dict[str, Any]]:
        """특정 날짜의 금리 데이터 가져오기"""
        filepath = self.rates_dir / f"{date_str}.json"
        return self.load_json(filepath)
    
    def list_available_dates(self) -> List[str]:
        """사용 가능한 날짜 목록 반환"""
        if not self.rates_dir.exists():
            return []
        
        # 파일명에서 날짜 추출
        dates = []
        for file in self.rates_dir.glob('*.json*'):
            date_str = file.stem.replace('.json', '')
            try:
                # 날짜 형식 검증
                datetime.strptime(date_str, '%Y-%m-%d')
                dates.append(date_str)
            except ValueError:
                continue
        
        return sorted(dates, reverse=True)
    
    def save_grades(self, grades_data: List[Dict[str, Any]]) -> bool:
        """경영실태평가 데이터 저장 (v2/grades/ 및 data/grades/ 병행 저장)"""
        try:
            # 1. 원본 data/grades 디렉토리 생성 및 저장 (Legacy 호환성)
            legacy_grades_dir = self.data_dir / "grades"
            legacy_grades_dir.mkdir(parents=True, exist_ok=True)
            
            # 2. V2 v2/grades 디렉토리 생성 및 저장
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
            
            # 두 경로 모두 저장
            success = True
            for base_path in [legacy_grades_dir, v2_grades_dir]:
                filepath = base_path / filename
                success &= self.save_json(data, filepath)
                success &= self.save_json(data, filepath, compress=True)
            
            if success:
                print(f"✓ 경영실태평가 데이터 저장 완료 (V1 & V2)")
            return success
            
        except Exception as e:
            print(f"❌ 경영실태평가 데이터 저장 실패: {e}")
            return False
    
    def load_grades(self, year: int = None, month: int = None) -> Optional[Dict[str, Any]]:
        """경영실태평가 데이터 로드 (v2/grades/ 우선 확인)"""
        try:
            # 1. v2 디렉토리 우선 확인
            grades_dir = self.v2_dir / "grades"
            if not grades_dir.exists():
                grades_dir = self.data_dir / "grades"
            
            if not grades_dir.exists():
                return None
                
            if year is None or month is None:
                # 저장된 파일 중 가장 최신의 파일을 찾아 로드
                grade_files = list(grades_dir.glob("grades_*_*.json"))
                if not grade_files:
                    # v2에 없으면 legacy도 한 번 더 시도
                    if grades_dir != (self.data_dir / "grades"):
                        grades_dir = self.data_dir / "grades"
                        grade_files = list(grades_dir.glob("grades_*_*.json"))
                    
                    if not grade_files:
                        return None
                        
                grade_files.sort(reverse=True)
                filepath = grade_files[0]
            else:
                filepath = grades_dir / f"grades_{year}_{month:02d}.json"
                if not filepath.exists() and grades_dir != (self.data_dir / "grades"):
                    filepath = self.data_dir / "grades" / f"grades_{year}_{month:02d}.json"
                
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
                return {
                    "gmgoCd": gmgo_cd,
                    "name": bank_info.get('name'),
                    "region": bank_info.get('city'),
                    "grade": grade_info.get('grade_code'),
                    "bis_ratio": bis_val,
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
        """모바일 상품 거치식/적립식/요구불 전국 및 지역별 Top 금리 추출"""
        result = {
            "updated_at": datetime.now().isoformat()
        }
        for m in month_keys:
            result[m] = {"all": [], "regions": {}}
            
        temp_data = {m: {} for m in month_keys}
        
        for bank in data_list:
            gmgo_cd = bank.get("gmgoCd")
            name = bank.get("name")
            region = bank.get("region")
            products = bank.get("products", {})
            
            for prdt_name, months_data in products.items():
                if target_products and prdt_name not in target_products:
                    continue
                    
                for month in month_keys:
                    if month in months_data:
                        rate_info = months_data[month]
                        if rate_info.get("s") == "m" and rate_info.get("r", 0) > 0:
                            entry = {
                                "gmgoCd": gmgo_cd,
                                "name": name,
                                "r": rate_info.get("r"),
                                "prdtNm": prdt_name
                            }
                            if "all" not in temp_data[month]:
                                temp_data[month]["all"] = []
                            temp_data[month]["all"].append(entry)
                            if region:
                                if region not in temp_data[month]:
                                    temp_data[month][region] = []
                                temp_data[month][region].append(entry)
                                
        for month in month_keys:
            all_list = temp_data[month].get("all", [])
            unique_all = {}
            for item in all_list:
                key = f"{item['gmgoCd']}_{item['prdtNm']}"
                if key not in unique_all or unique_all[key]["r"] < item["r"]:
                    unique_all[key] = item
            all_list = list(unique_all.values())
            all_list.sort(key=lambda x: (-x["r"], x["name"]))
            result[month]["all"] = all_list[:20]
            
            for rgn, rgn_list in temp_data[month].items():
                if rgn == "all": continue
                unique_rgn = {}
                for item in rgn_list:
                    key = f"{item['gmgoCd']}_{item['prdtNm']}"
                    if key not in unique_rgn or unique_rgn[key]["r"] < item["r"]:
                        unique_rgn[key] = item
                rgn_list = list(unique_rgn.values())
                rgn_list.sort(key=lambda x: (-x["r"], x["name"]))
                result[month]["regions"][rgn] = rgn_list[:10]
                
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
                return max_rate

            # 금리 내림차순 정렬 후 상위 15개 추출
            sorted_data = sorted(src_data, key=get_sort_key, reverse=True)
            main_api[key] = sorted_data[:15]
            
        return main_api

    def save_v2_api(self, v2_data_all: Dict[str, Dict[str, Any]]) -> bool:
        """
        V2 API 데이터를 파일로 저장
        - v2/rates/deposit/all.json, v2/rates/saving/all.json, v2/rates/demand/all.json
        - v2/rates/deposit/mbank.json 등 모바일 전용 데이터 포함
        - v2/main.json (BFF API)
        """
        try:
            v2_rates_dir = self.v2_dir / "rates"
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
            main_filepath = self.v2_dir / "main.json"
            if self.save_json(main_api_data, main_filepath, pretty=False):
                logger.info(f"🏠 V2 Main Page BFF API 저장 완료: {main_filepath}")
            else:
                success = False

            if success:
                logger.info(f"🚀 V2 API 기본 데이터 저장 완료: {v2_rates_dir}")
            
            top_configs = [
                ("deposit", ["3", "6", "12"], ["MG더뱅킹정기예금"], "m.json"),
                ("saving", ["3", "6", "12"], ["MG더뱅킹정기적금", "MG더뱅킹자유적금"], "m.json"),
                ("demand", ["0", "12"], ["상상모바일통장"], "all.json")
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
            return success
        except Exception as e:
            logger.error(f"❌ V2 API 데이터 저장 실패: {e}")
            return False
    
    def cleanup_old_data(self, days_to_keep: int = 30) -> int:
        """
        오래된 데이터 파일 정리
        
        Args:
            days_to_keep: 보관할 일수
            
        Returns:
            삭제된 파일 수
        """
        if not self.rates_dir.exists():
            return 0
        
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        removed_count = 0
        
        for file in self.rates_dir.glob('*.json*'):
            try:
                date_str = file.stem.replace('.json', '')
                file_date = datetime.strptime(date_str, '%Y-%m-%d')
                
                if file_date < cutoff_date:
                    file.unlink()
                    removed_count += 1
                    logger.info(f"🗑️ 오래된 파일 삭제: {file.name}")
                    
            except (ValueError, OSError) as e:
                logger.warning(f"파일 처리 오류: {file.name} - {e}")
        
        if removed_count > 0:
            logger.info(f"🧹 정리 완료: {removed_count}개 파일 삭제")
        
        return removed_count
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """저장소 통계 정보 반환"""
        stats = {
            'data_directory': str(self.data_dir),
            'bank_list_exists': self.bank_list_file.exists(),
            'rates_directory_exists': self.rates_dir.exists(),
            'backup_directory_exists': self.backup_dir.exists(),
            'available_dates': self.list_available_dates(),
            'total_rate_files': 0,
            'total_backup_files': 0,
            'latest_date': None,
            'storage_size_mb': 0
        }
        
        # 파일 수 계산
        if self.rates_dir.exists():
            rate_files = list(self.rates_dir.glob('*.json*'))
            stats['total_rate_files'] = len(rate_files)
            if rate_files:
                stats['latest_date'] = max(
                    f.stem.replace('.json', '') for f in rate_files
                )
        
        if self.backup_dir.exists():
            stats['total_backup_files'] = len(list(self.backup_dir.glob('*')))
        
        # 전체 저장소 크기 계산
        total_size = 0
        for file in self.data_dir.rglob('*'):
            if file.is_file():
                total_size += file.stat().st_size
        
        stats['storage_size_mb'] = round(total_size / (1024 * 1024), 2)
        
        return stats


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
        
        # 2. 금리 데이터 저장
        if rates:
            success &= manager.save_daily_rates(rates, date_str)
            
            # 3. 요약 데이터 저장
            summary = parse_summary_data(rates)
            success &= manager.save_summary(summary, date_str)
        
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