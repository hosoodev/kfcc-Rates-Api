#!/usr/bin/env python3
"""
7월 스케줄 복원 스크립트
2026년 6월 30일에 실행하여 7월 스케줄로 되돌립니다.
"""

import os
import re
from datetime import datetime

def restore_july_schedule():
    """7월 스케줄로 복원"""
    print("🔄 7월 스케줄 복원 시작...")
    
    # 1. config.py 수정
    config_file = "src/config.py"
    if os.path.exists(config_file):
        with open(config_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # collection_month를 7로 변경 (배열에서 단일 값으로)
        content = re.sub(
            r"'collection_month': \[.*?\],.*# 수집 월.*",
            "'collection_month': 7,  # 수집 월 (7월)",
            content
        )
        
        with open(config_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"✅ {config_file} 수정 완료")
    
    # 2. crawler.yml 수정
    workflow_file = ".github/workflows/crawler.yml"
    if os.path.exists(workflow_file):
        with open(workflow_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 7월과 9월을 7월만으로 변경
        content = re.sub(
            r"Collect grade evaluations \(July and September\)",
            "Collect grade evaluations (July only)",
            content
        )
        content = re.sub(
            r"if: github\.event\.schedule == '0 2 1 7 \*' \|\| github\.event\.schedule == '0 2 1 9 \*'",
            "if: github.event.schedule == '0 2 1 7 *'",
            content
        )
        
        with open(workflow_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"✅ {workflow_file} 수정 완료")
    
    # 3. grade_crawler.yml 삭제 (선택사항)
    grade_workflow = ".github/workflows/grade-crawler.yml"
    if os.path.exists(grade_workflow):
        os.remove(grade_workflow)
        print(f"✅ {grade_workflow} 삭제 완료")
    
    print("🎉 7월 스케줄 복원 완료!")
    print("📅 2026년 7월 1일부터 정상적으로 경영실태평가가 수집됩니다.")

if __name__ == '__main__':
    restore_july_schedule()
