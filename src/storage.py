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
    
    def __init__(self, data_dir: str = DATA_DIR):
        """저장소 초기화"""
        self.data_dir = Path(data_dir)
        self.rates_dir = self.data_dir / 'rates'
        self.backup_dir = self.data_dir / 'backups'
        self.bank_list_file = Path(BANK_LIST_FILE)
        
        # 디렉토리 생성
        self._ensure_directories()
    
    def _ensure_directories(self) -> None:
        """필요한 디렉토리 생성"""
        for directory in [self.data_dir, self.rates_dir, self.backup_dir]:
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
        """은행 목록 저장"""
        if not banks:
            logger.warning("저장할 은행 목록이 없습니다")
            return False
        
        # 백업 생성
        self._create_backup(self.bank_list_file)
        
        # 중복 제거
        unique_banks = self._remove_duplicate_banks(banks)
        
        # 메타데이터 추가
        bank_data = {
            'metadata': {
                'total_count': len(unique_banks),
                'unique_count': len(set(b['gmgoCd'] for b in unique_banks)),
                'crawled_at': datetime.now().isoformat(),
                'version': '1.1'
            },
            'banks': unique_banks
        }
        
        success = self.save_json(bank_data, self.bank_list_file)
        if success:
            logger.info(f"🏦 은행 목록 저장 완료: {len(unique_banks)}개")
        
        return success
    
    def load_banks(self) -> Optional[Dict[str, Any]]:
        """은행 목록 로드"""
        try:
            if not self.bank_list_file.exists():
                return None
            
            with open(self.bank_list_file, 'r', encoding='utf-8') as f:
                return json.load(f)
                
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
        """경영실태평가 데이터 저장"""
        try:
            # grades 디렉토리 생성
            grades_dir = self.data_dir / "grades"
            grades_dir.mkdir(exist_ok=True)
            
            # 파일명: grades_YYYY_MM.json (월 정보 포함)
            if grades_data:
                evaluation_year = grades_data[0]['evaluation_year']
                evaluation_month = grades_data[0]['evaluation_month']
                filename = f"grades_{evaluation_year}_{evaluation_month:02d}.json"
            else:
                current_year = datetime.now().year
                current_month = datetime.now().month
                filename = f"grades_{current_year}_{current_month:02d}.json"
            filepath = grades_dir / filename
            
            # 데이터 구성
            data = {
                "collection_info": {
                    "collected_at": datetime.now().isoformat(),
                    "total_banks": len(grades_data),
                    "evaluation_year": grades_data[0]['evaluation_year'] if grades_data else None,
                    "evaluation_month": grades_data[0]['evaluation_month'] if grades_data else None
                },
                "grades": grades_data
            }
            
            # JSON 파일로 저장
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"✓ 경영실태평가 데이터 저장 완료: {filepath}")
            return True
            
        except Exception as e:
            print(f"❌ 경영실태평가 데이터 저장 실패: {e}")
            return False
    
    def load_grades(self, year: int = None, month: int = None) -> Optional[Dict[str, Any]]:
        """경영실태평가 데이터 로드"""
        try:
            if year is None:
                year = datetime.now().year
            if month is None:
                month = datetime.now().month
            
            grades_dir = self.data_dir / "grades"
            filepath = grades_dir / f"grades_{year}_{month:02d}.json"
            
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
_storage_manager = StorageManager()

def save_all(banks: List[Dict[str, Any]], rates: List[Dict[str, Any]], 
            date_str: Optional[str] = None) -> bool:
    """모든 데이터를 저장"""
    if date_str is None:
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    logger.info("💾 데이터 저장 시작...")
    
    try:
        success = True
        
        # 1. 은행 목록 저장
        if banks:
            success &= _storage_manager.save_bank_list(banks)
        
        # 2. 금리 데이터 저장
        if rates:
            success &= _storage_manager.save_daily_rates(rates, date_str)
            
            # 3. 요약 데이터 저장
            summary = parse_summary_data(rates)
            success &= _storage_manager.save_summary(summary, date_str)
        
        if success:
            logger.info("✅ 모든 데이터 저장 완료")
        else:
            logger.warning("⚠️ 일부 데이터 저장 실패")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ 데이터 저장 중 오류 발생: {e}")
        return False

def get_latest_rates() -> Optional[Dict[str, Any]]:
    """최신 금리 데이터를 가져옴"""
    return _storage_manager.get_latest_rates()

def get_rates_by_date(date_str: str) -> Optional[Dict[str, Any]]:
    """특정 날짜의 금리 데이터를 가져옴"""
    return _storage_manager.get_rates_by_date(date_str)

def list_available_dates() -> List[str]:
    """사용 가능한 날짜 목록을 반환"""
    return _storage_manager.list_available_dates()

def cleanup_old_data(days_to_keep: int = 30) -> int:
    """오래된 데이터 파일 정리"""
    return _storage_manager.cleanup_old_data(days_to_keep)

def get_storage_stats() -> Dict[str, Any]:
    """저장소 통계 정보 반환"""
    return _storage_manager.get_storage_stats()