#!/usr/bin/env python3
"""
새마을금고 금리 크롤러 메인 실행 파일

이 스크립트는 새마을금고 웹사이트에서 금리 정보를 크롤링하고
JSON 형태로 저장하는 전체 워크플로우를 실행합니다.
"""

import sys
import os
import argparse
from datetime import datetime
from crawler import KFCCCrawler
try:
    from .grade_crawler import GradeCrawler
except ImportError:
    from grade_crawler import GradeCrawler
try:
    from .storage import save_all, get_storage_stats, cleanup_old_data, StorageManager
except ImportError:
    from storage import save_all, get_storage_stats, cleanup_old_data, StorageManager

def print_banner():
    """프로그램 시작 배너 출력"""
    print("=" * 60)
    print("🏦 새마을금고 금리 크롤러 v1.0")
    print("=" * 60)
    print(f"⏰ 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

def print_summary(banks, rates, start_time):
    """실행 결과 요약 출력"""
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    print("📊 실행 결과 요약")
    print("=" * 60)
    print(f"🏦 수집된 금고 수: {len(banks)}")
    print(f"💰 수집된 금리 정보: {len(rates)}")
    print(f"⏱️ 소요 시간: {elapsed:.2f}초")
    
    if rates:
        # 성공적으로 수집된 금고 수 계산
        successful_banks = len([r for r in rates if r.get('total_products', 0) > 0])
        print(f"✅ 성공한 금고: {successful_banks}/{len(rates)} ({successful_banks/len(rates)*100:.1f}%)")
        
        # 총 상품 수 계산
        total_products = sum(r.get('total_products', 0) for r in rates)
        print(f"📈 총 상품 수: {total_products}")
    
    print("=" * 60)

def run_crawler(cleanup_days=None):
    """
    크롤러 실행
    
    Args:
        cleanup_days (int): 오래된 데이터 정리 일수 (None이면 정리 안함)
    """
    start_time = datetime.now()
    
    try:
        # 크롤러 초기화 및 실행
        crawler = KFCCCrawler()
        banks, rates = crawler.run()
        
        if not banks and not rates:
            print("❌ 크롤링 실패: 데이터를 수집할 수 없습니다")
            return False
        
        # 데이터 저장
        print("\n💾 데이터 저장 중...")
        save_all(banks, rates)
        
        # 오래된 데이터 정리
        if cleanup_days:
            print(f"\n🧹 {cleanup_days}일 이상 된 데이터 정리 중...")
            cleanup_old_data(cleanup_days)
        
        # 결과 요약 출력
        print_summary(banks, rates, start_time)
        
        return True
        
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다")
        return False
    except Exception as e:
        print(f"\n❌ 실행 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return False

def show_stats():
    """저장소 통계 정보 출력"""
    print("📊 저장소 통계 정보")
    print("-" * 40)
    
    stats = get_storage_stats()
    
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

def collect_grades():
    """경영실태평가 데이터 수집"""
    print("📊 경영실태평가 데이터 수집 시작...")
    
    # 은행 목록 로드
    storage = StorageManager()
    banks_data = storage.load_banks()
    
    if not banks_data or 'banks' not in banks_data:
        print("❌ 은행 목록을 먼저 수집해주세요.")
        return False
    
    banks = banks_data['banks']
    print(f"📋 {len(banks)}개 금고의 경영실태평가 수집 시작")
    
    # 경영실태평가 크롤러 실행
    grade_crawler = GradeCrawler()
    grades_data = grade_crawler.collect_all_grades(banks)
    
    if grades_data:
        # 데이터 저장
        success = storage.save_grades(grades_data)
        if success:
            print(f"✅ 경영실태평가 수집 완료: {len(grades_data)}개 금고")
            return True
        else:
            print("❌ 경영실태평가 데이터 저장 실패")
            return False
    else:
        print("❌ 경영실태평가 데이터 수집 실패")
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
        '--version', 
        action='version', 
        version='새마을금고 금리 크롤러 v1.0'
    )
    
    args = parser.parse_args()
    
    # 경영실태평가 수집
    if args.grades:
        print_banner()
        success = collect_grades()
        return 0 if success else 1
    
    # 통계만 출력하는 경우
    if args.stats:
        print_banner()
        show_stats()
        return 0
    
    # 크롤링 실행
    print_banner()
    
    success = run_crawler(cleanup_days=args.cleanup)
    
    if success:
        print("\n🎉 크롤링이 성공적으로 완료되었습니다!")
        return 0
    else:
        print("\n💥 크롤링이 실패했습니다.")
        return 1

if __name__ == '__main__':
    sys.exit(main())
