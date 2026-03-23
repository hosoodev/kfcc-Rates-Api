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
        self.data_dir = self.base_dir / "data"
        self.v2_dir = self.base_dir / "v2"
        
        self.rates_dir = self.data_dir / 'rates'
        self.grades_dir = self.data_dir / 'grades'
        self.backup_dir = self.data_dir / 'backups'
        self.bank_list_file = self.data_dir / "banks.json"
        self.archive_rates_dir = self.base_dir / "_archive" / "rates"
        
        # 디렉토리 생성
        self._ensure_directories()
    
    def _ensure_directories(self) -> None:
        """필요한 디렉토리 생성"""
        for directory in [self.data_dir, self.v2_dir, self.rates_dir, self.backup_dir, self.archive_rates_dir]:
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
                    "province": head.get('province') or head.get('city'),
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
                            "province": head.get('province'),
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
        
        # 1. data/rates/ 저장
        success = self.save_json(rates_data, filepath, compress=compress)
        
        # 2. _archive/rates/ (레거시 아카이브) 저장
        archive_filepath = self.archive_rates_dir / f"{date_str}.json"
        success &= self.save_json(rates_data, archive_filepath, compress=compress)
        
        if success:
            logger.info(f"💰 금리 데이터 저장 및 아카이브 완료: {date_str} ({len(rates)}개 금고)")
        
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
                logger.info("경영실태평가 데이터 저장 완료 (V1 & V2)")
            return success
            
        except Exception as e:
            logger.error(f"경영실태평가 데이터 저장 실패: {e}")
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
                    "province": bank_info.get('province'),
                    "district": bank_info.get('district'),
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
        
        for bank in data_list:
            gmgo_cd = bank.get("gmgoCd")
            products = bank.get("products", {})
            
            for prdt_name, months_data in products.items():
                if target_products and prdt_name not in target_products:
                    continue
                    
                for month in month_keys:
                    if month in months_data:
                        rate_info = months_data[month]
                        # 모바일 소스(s=="m")인 경우만 포함 (또는 demand의 경우 최고 금리)
                        if rate_info.get("r", 0) > 0:
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
            
            final_all = sorted(list(unique_all), key=lambda x: (-x["_r"], x["name"]))
            result[month]["data"] = final_all[:20]
            
            # 2. 지역별 랭킹 처리
            for rgn, rgn_list in temp_data[month].items():
                if rgn == "data": continue
                
                unique_rgn = {item['gmgoCd']: item for item in rgn_list}.values()
                final_rgn = sorted(list(unique_rgn), key=lambda x: (-x["_r"], x["_r"]))
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

    def build_seo_regions_api(self, v2_data_all: Dict[str, Dict[str, Any]]) -> None:
        """
        지역별(시도/시군구) SEO용 정적 JSON 파일 생성
        - v2/rates/{type}/regions/{province_slug}/all.json
        - v2/rates/{type}/regions/{province_slug}/{district_slug}.json
        """
        v2_rates_dir = self.v2_dir / "rates"
        
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
            
            # 정렬 키 함수 (main.json 로직 재사용)
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
                return max_rate

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
                return max_rate

            # 금리 내림차순 정렬 후 상위 15개 추출
            sorted_data = sorted(src_data, key=get_sort_key, reverse=True)
            main_api[key] = sorted_data[:15]
            
        return main_api

    def save_v2_summary(self, v2_data_all: Dict[str, Dict[str, Any]]):
        """v2_data_all을 기반으로 summary.json 생성 및 저장"""
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
                
                rates_v2.append({
                    "gmgoCd": bank_data.get("gmgoCd"),
                    "name": bank_data.get("name"),
                    "grade": bank_data.get("grade"),
                    "products": products_v1
                })
        
        summary_v2 = parse_summary_data_v2(rates_v2)
        v2_summary_path = self.v2_dir / "rates" / "summary.json"
        v2_summary_path.parent.mkdir(parents=True, exist_ok=True)
        
        if self.save_json(summary_v2, v2_summary_path, pretty=False):
            logger.info(f"📊 V2 Dashboard Summary 저장 완료: {v2_summary_path}")
        else:
            logger.error(f"❌ V2 Dashboard Summary 저장 실패: {v2_summary_path}")

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

            # 4. summary.json 저장 (Dashboard 요약)
            try:
                self.save_v2_summary(v2_data_all)
            except Exception as e:
                logger.error(f"⚠️ V2 요약 데이터 생성 중 오류 발생: {e}")
                success = False

            # 5. 지역별 SEO API 생성
            try:
                self.build_seo_regions_api(v2_data_all)
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
                self.build_branch_detail_api(v2_data_all)
            except Exception as e:
                logger.error(f"⚠️ 지점 상세 API 생성 중 오류 발생: {e}")

            return success
        except Exception as e:
            logger.error(f"❌ V2 API 데이터 저장 실패: {e}")
            return False
    
    def build_branch_detail_api(self, v2_data_all: Dict[str, Dict[str, Any]]) -> bool:
        """
        개별 지점 상세 페이지용 정적 API 파일 생성
        v2/branches/{gmgoCd}.json
        """
        try:
            # 1. 메타데이터 로드 및 지역별 그룹화
            # V2 메타데이터 폴더 (api-data/v2/meta/banks.json 예상)
            banks_file = self.v2_dir / "meta" / "banks.json"
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
            grades_history_index = {}
            grades_dir = self.v2_dir / "grades"
            if grades_dir.exists():
                grade_files = list(grades_dir.glob("grades_*.json"))
                periods = []
                for gf in grade_files:
                    match = re.search(r"grades_(\d{4})_(\d{2})", gf.name)
                    if match:
                        period = f"{match.group(1)}_{match.group(2)}"
                        periods.append((period, gf))
                
                # 최신순 정렬
                periods.sort(key=lambda x: x[0], reverse=True)
                
                for period, gf in periods:
                    try:
                        with open(gf, "r", encoding="utf-8") as f:
                            g_data = json.load(f)
                            for grade in g_data.get("grades", []):
                                g_cd = grade.get("gmgo_cd")
                                if g_cd:
                                    if g_cd not in grades_history_index:
                                        grades_history_index[g_cd] = []
                                    grades_history_index[g_cd].append({
                                        "period": period,
                                        "grade": grade.get("grade_code"),
                                        "bis_ratio": grade.get("bis_ratio")
                                    })
                    except Exception as e:
                        logger.error(f"⚠️ {gf.name} 로드 실패: {e}")

            # 4. 개별 지점 JSON 조립
            branches_dir = self.v2_dir / "branches"
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
                        "grades_history": grades_history_index.get(gmgo_cd, []),
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

            # 4. V2 대시보드 요약 데이터 저장 (BFF 전용)
            try:
                # v2_data_all을 가져오기 위해 build_v2_api 다시 호출 (또는 이미 생성된 파일 로드)
                # save_all 내부에서는 build_v2_api 결과인 v2_data_all이 없으므로 새로 생성하거나 save_v2_api 호출
                # 여기서는 명시적으로 save_v2_summary를 위한 형식 변환 수행
                v2_data_all = manager.build_v2_api(rates, grades_data.get('grades', []) if grades_data else [])
                manager.save_v2_summary(v2_data_all)
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