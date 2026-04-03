#!/usr/bin/env python3
"""
새마을금고 금리 크롤러 메인 실행 파일

이 스크립트는 새마을금고 웹사이트에서 금리 정보를 크롤링하고
JSON 형태로 저장하는 전체 워크플로우를 실행합니다.
"""

import sys
import os
import argparse
import shutil
import logging
from datetime import datetime
from crawler import KFCCCrawler
from grade_crawler import GradeCrawler
from mbank_crawler import MBankCrawler
from storage import save_all, get_storage_stats, cleanup_old_data, StorageManager

# 로그 설정 (main.py에서 전체 제어)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("main")

def print_banner():
    """프로그램 시작 배너 출력"""
    banner = f"""
{"=" * 60}
🏦 새마을금고 금리 크롤러 v2.0
{"=" * 60}
⏰ 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
    logger.info(banner)

def print_summary(banks, rates, start_time):
    """실행 결과 요약 출력"""
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    
    summary = f"""
{"=" * 60}
📊 실행 결과 요약
{"=" * 60}
🏦 수집된 금고 수: {len(banks)}
💰 수집된 금리 정보: {len(rates)}
⏱️ 소요 시간: {elapsed:.2f}초
"""
    if rates:
        successful_banks = len([r for r in rates if r.get('total_products', 0) > 0])
        total_products = sum(r.get('total_products', 0) for r in rates)
        summary += f"✅ 성공한 금고: {successful_banks}/{len(rates)} ({successful_banks/len(rates)*100:.1f}%)\n"
        summary += f"📈 총 상품 수: {total_products}\n"
    
    summary += "=" * 60
    for line in summary.strip().split('\n'):
        logger.info(line)

def run_crawler(cleanup_days=None, test_mode=False, test_branch=None, refresh_banks=False, base_dir=None):
    """크롤러 실행"""
    start_time = datetime.now()
    
    try:
        # 크롤러 초기화 및 실행
        crawler = KFCCCrawler()
        banks, rates = crawler.run(test_branch=test_branch, refresh_banks=refresh_banks)
        
        if not banks and not rates:
            if not test_mode:
                logger.error("❌ 크롤링 실패: 데이터를 수집할 수 없습니다")
            return False
        
        # 테스트 모드인 경우 여기서 종료 (저장 안함)
        if test_mode:
            logger.info("🧪 테스트 모드: 데이터 저장을 스킵합니다.")
            return True
            
        # 데이터 저장
        logger.info("💾 데이터 저장 중...")
        save_all(banks, rates, base_dir=base_dir)
        
        # V2 API 데이터 생성 및 저장
        logger.info("🚀 V2 Static API를 생성 중...")
        storage = StorageManager(base_dir=base_dir)
        grades_data = storage.load_grades()
        grades = grades_data.get('grades', []) if grades_data else []
        
        v2_api_all = storage.build_v2_api(rates, grades)
        storage.save_v2_api(v2_api_all)
        storage.save_v2_api(v2_api_all, target_dir=storage.daily_raw_dir)

        # 오래된 데이터 정리
        if cleanup_days:
            logger.info(f"🧹 {cleanup_days}일 이상 된 데이터 정리 중...")
            cleanup_old_data(cleanup_days, base_dir=base_dir)
        
        # 결과 요약 출력
        print_summary(banks, rates, start_time)
        return True
    except KeyboardInterrupt:
        logger.warning("⚠️ 사용자에 의해 중단되었습니다")
        return False
    except Exception as e:
        logger.error(f"❌ 실행 중 오류 발생: {e}", exc_info=True)
        return False

def run_patch(regions=None, base_dir=None):
    """모바일 크롤러를 통한 실시간 금리 패치 모드"""
    print("📱 모바일 실시간 금리 패치를 시작합니다...")
    start_time = datetime.now()
    
    try:
        storage = StorageManager(base_dir=base_dir)
        # 1. 기존 데이터 로드 (Patch를 위해 누적되지 않은 일일 원본인 dailyRaw에서 로드)
        v2_data_all = {}
        target_load_dir = storage.daily_raw_dir if storage.daily_raw_dir.exists() else storage.v2_dir
        
        if target_load_dir == storage.v2_dir:
            print("⚠️ dailyRaw 폴더가 없어 v2 폴더에서 데이터를 로드합니다. (최초 실행 또는 예외 상황)")
        else:
            print(f"📁 일일 원본 데이터({target_load_dir})에서 베이스 데이터를 로드합니다.")

        for key in ["deposit", "saving", "demand"]:
            data = storage.load_json(target_load_dir / "rates" / key / "all.json")
            if data:
                v2_data_all[key] = data
                
        if not v2_data_all:
            print("❌ 기존 V2 데이터가 없습니다. --mode base를 먼저 실행해주세요.")
            return False
        
        # 2. 모바일 데이터 수집
        mbank = MBankCrawler(base_dir=base_dir)
        
        # 'all'인 경우 전체 지역 목록 가져오기
        if regions == ['all']:
            regions = list(mbank.sigungu_codes.keys())
            print(f"🌍 전체 지역 패치 모드: {len(regions)}개 지역 수집")
            
        # 병렬 크롤러를 사용하여 전체 상품(예금, 적금, 입출금) 수집
        patch_data = mbank.collect_patch_data(regions=regions)
        
        if not patch_data:
            print("⚠️ 수집된 모바일 데이터가 없습니다.")
            return True
            
        # 3. 데이터 패치 (Upsert) - 모든 카테고리 반영
        product_mappings = {
            "deposit": ["MG더뱅킹정기예금"],
            "saving": ["MG더뱅킹정기적금", "MG더뱅킹자유적금"],
            "demand": ["상상모바일통장"]
        }
        for key, p_names in product_mappings.items():
            if key in v2_data_all:
                filtered_patches = [p for p in patch_data if p["prdtNm"] in p_names]
                if filtered_patches:
                    v2_data_all[key] = storage.upsert_mbank_patch(v2_data_all[key], filtered_patches)
        
        # 4. 저장 (save_v2_api를 통해 top 모바일 api도 자동 갱신됨)
        storage.save_v2_api(v2_data_all)
        
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"✅ 패치 완료: {len(patch_data)}건 수집됨 (소요시간: {elapsed:.2f}초)")
        return True
        
    except Exception as e:
        print(f"❌ 패치 중 오류 발생: {e}")
        return False

def show_stats(base_dir=None):
    """저장소 통계 정보 출력"""
    print("📊 저장소 통계 정보")
    print("-" * 40)
    
    stats = get_storage_stats(base_dir=base_dir)
    
    print(f"📁 데이터 디렉토리: {stats['data_directory']}")
    print(f"🏦 은행 목록 파일: {'✅ 존재' if stats['bank_list_exists'] else '❌ 없음'}")
    print(f"📂 금리 데이터 디렉토리: {'✅ 존재' if stats['rates_directory_exists'] else '❌ 없음'}")
    print(f"📅 사용 가능한 날짜: {len(stats['available_dates'])}개")
    print(f"📄 금리 파일 수: {stats['total_rate_files']}개")
    
    if stats['available_dates']:
        print(f"🕐 최신 데이터: {stats['latest_date']}")
        print("\n📅 사용 가능한 날짜 목록:")
        for i, date in enumerate(stats['available_dates'][:10]):  # 최근 10개만 표시
            print(f"  {i+1:2d}. {date}")
        if len(stats['available_dates']) > 10:
            print(f"  ... 외 {len(stats['available_dates']) - 10}개")

def collect_grades(evaluation_date=None, base_dir=None, use_cache=False):
    """경영실태평가 데이터 수집"""
    logger.info(f"📊 경영실태평가 수집 작업 시작 (기준: {evaluation_date or '최신'})")
    if use_cache:
        logger.info("💡 캐시 모드 활성화: 기존 수집 데이터를 활용합니다.")
    
    storage = StorageManager(base_dir=base_dir)
    banks_data = storage.load_banks()
    
    if not banks_data or 'banks' not in banks_data:
        logger.error("❌ 은행 목록이 필요합니다. --mode base 또는 --refresh를 먼저 실행하세요.")
        return False
    
    banks = banks_data['banks']
    total_targets = len(banks)
    logger.info(f"📋 총 {total_targets}개 금고의 경영실태평가 정보 확인 시작")
    
    # 경영실태평가 크롤러 실행
    grade_crawler = GradeCrawler(base_dir=base_dir)
    grades_data = grade_crawler.collect_all_grades(banks, evaluation_date=evaluation_date, use_cache=use_cache)
    
    if grades_data is not None:
        # 데이터 저장
        success = storage.save_grades(grades_data)
        if success:
            # 최종 통계 산출
            total_saved = len(grades_data)
            missing = total_targets - total_saved
            summary_msg = f"""
{"=" * 60}
📊 경영실태평가 수집 결과 요약
{"=" * 60}
✅ 최종 저장된 금고: {total_saved}개 (성공)
❌ 데이터 누락 금고: {missing}개 (공시 미등록 등)
📈 수집 대상 대비 성공률: {(total_saved/total_targets*100):.1f}%
{"=" * 60}
"""
            for line in summary_msg.strip().split('\n'):
                logger.info(line)
            return True
        else:
            logger.error("❌ 경영실태평가 데이터 저장 실패")
            return False
    else:
        logger.error("❌ 경영실태평가 데이터 수집 실패")
        return False

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description='새마을금고 금리 크롤러',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  python main.py                    # 기본 크롤링 실행
  python main.py --cleanup 30       # 30일 이상 된 데이터 정리하며 크롤링
  python main.py --stats            # 저장소 통계만 출력
  python main.py --test --branch 강동 # '강동' 지점 테스트 크롤링 (저장 안함)
  python main.py --help             # 도움말 출력
        """
    )
    
    parser.add_argument(
        '--cleanup', 
        type=int, 
        metavar='DAYS',
        help='지정된 일수 이상 된 데이터를 정리합니다'
    )
    
    parser.add_argument(
        '--stats', 
        action='store_true',
        help='저장소 통계 정보만 출력하고 종료합니다'
    )
    
    parser.add_argument(
        '--grades', 
        action='store_true',
        help='경영실태평가 데이터 수집 (7월에만 실행)'
    )
    
    parser.add_argument(
        '--date',
        type=str,
        help='경영실태평가 기준연월 (YYYYMM)'
    )
    
    parser.add_argument(
        '--use-cache',
        action='store_true',
        help='경영실태평가 수집 시 기존 데이터를 로드하여 이미 수집된 곳은 건너뜁니다'
    )

    parser.add_argument(
        '--test',
        action='store_true',
        help='테스트 모드로 실행 (결과 출력만 하고 저장하지 않음)'
    )

    parser.add_argument('--branch', type=str, help='테스트 모드에서 특정 지점명 또는 금고코드 필터링')
    parser.add_argument('--refresh', action='store_true', help='은행 목록 캐시를 무시하고 새로 수집')
    
    parser.add_argument(
        '--mode',
        choices=['base', 'patch'],
        default='base',
        help='실행 모드: base(전수조합+V2빌드), patch(모바일 실시간 업데이트)'
    )

    parser.add_argument(
        '--regions',
        type=str,
        help='패치할 지역 (콤마로 구분, 예: 서울,경기)'
    )

    parser.add_argument(
        '--base-dir',
        type=str,
        help='저장 공간 베이스 디렉토리 강제 변경 (api-data 등)'
    )

    parser.add_argument(
        '--version', 
        action='version', 
        version='새마을금고 금리 크롤러 v2.0'
    )
    
    args = parser.parse_args()
    
    # 경영실태평가 수집
    if args.grades:
        print_banner()
        success = collect_grades(
            evaluation_date=args.date, 
            base_dir=args.base_dir,
            use_cache=args.use_cache
        )
        return 0 if success else 1
    
    # 통계만 출력하는 경우
    if args.stats:
        print_banner()
        show_stats(base_dir=args.base_dir)
        return 0
    
    # 크롤링 실행
    print_banner()
    
    if args.mode == 'patch':
        # 패치 모드 실행
        regions = args.regions.split(',') if args.regions else ['all']
        success = run_patch(regions=regions, base_dir=args.base_dir)
    else:
        # 베이스 모드 실행
        success = run_crawler(
            cleanup_days=args.cleanup,
            test_mode=args.test,
            test_branch=args.branch,
            refresh_banks=args.refresh,
            base_dir=args.base_dir
        )
    
    if success:
        if not args.test:
            print("\n🎉 크롤링이 성공적으로 완료되었습니다!")
        return 0
    else:
        print("\n💥 크롤링이 실패했습니다.")
        return 1

if __name__ == '__main__':
    sys.exit(main())
