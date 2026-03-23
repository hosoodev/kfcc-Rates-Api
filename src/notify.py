import os
import requests
import sys
import json
from datetime import datetime

def notify_revalidate(tags: list[str]):
    """
    Vercel 프론트엔드 캐시 재검증(Revalidation) 트리거
    """
    site_url = os.environ.get("SITE_URL")
    secret = os.environ.get("REVALIDATE_SECRET")

    if not site_url or not secret:
        print("⚠️ SITE_URL 또는 REVALIDATE_SECRET이 설정되지 않았습니다. 캐시 초기화를 건너뜁니다.")
        return

    # URL 정규화
    url = f"{site_url.rstrip('/')}/api/revalidate"

    print(f"[{datetime.now()}] 🔄 캐시 초기화 시도 중... (태그: {tags})")
    
    try:
        response = requests.post(
            url,
            headers={
                "x-revalidate-secret": secret,
                "Content-Type": "application/json",
            },
            json={"tags": tags},
            timeout=15
        )
        response.raise_for_status()
        print(f"[{datetime.now()}] ✅ 캐시 초기화 성공: {response.json()}")
    except requests.exceptions.HTTPError as e:
        print(f"[{datetime.now()}] ❌ 캐시 초기화 실패 (HTTP {response.status_code}): {response.text}")
    except Exception as e:
        print(f"[{datetime.now()}] ❌ 캐시 초기화 중 오류 발생: {e}")

if __name__ == "__main__":
    # 인자로 JSON 형태의 태그 리스트를 받음
    # 예: python src/notify.py '["main", "rates"]'
    if len(sys.argv) > 1:
        try:
            tags_arg = sys.argv[1]
            tags = json.loads(tags_arg)
            if isinstance(tags, list):
                notify_revalidate(tags)
            else:
                print("❌ 태그는 리스트 형태여야 합니다.")
        except json.JSONDecodeError:
            print(f"❌ 올바르지 않은 태그 형식입니다 (JSON 필요): {sys.argv[1]}")
    else:
        print("❌ 전달된 태그가 없습니다.")
