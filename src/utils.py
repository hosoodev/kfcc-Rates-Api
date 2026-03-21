import random
import requests
import logging

logger = logging.getLogger(__name__)

# 전역 캐시 (메모리에 한 번 로드 후 재사용)
_LATEST_CHROME_MAJOR = None

def get_latest_chrome_major() -> int:
    """공식 Chrome 버전 히스토리 API를 통해 최신 안정 버전의 메이저 번호 획득"""
    global _LATEST_CHROME_MAJOR
    if _LATEST_CHROME_MAJOR:
        return _LATEST_CHROME_MAJOR
    
    try:
        # Chrome Platforms API (Windows Stable 채널)
        api_url = "https://versionhistory.googleapis.com/v1/chrome/platforms/win/channels/stable/versions"
        response = requests.get(api_url, timeout=5)
        response.raise_for_status()
        data = response.json()
        
        # 'versions' 리스트의 첫 번째 항목이 보통 가장 최신 버전 (예: "123.0.6312.59")
        latest_version = data['versions'][0]['version']
        major = int(latest_version.split('.')[0])
        _LATEST_CHROME_MAJOR = major
        logger.info(f"🌐 최신 Chrome 메이저 버전 확인: {major}")
        return major
    except Exception as e:
        logger.warning(f"⚠️ 최신 Chrome 버전 획득 실패, 기본값(122) 사용: {e}")
        return 122

def generate_mobile_ua() -> str:
    """기종, OS, 브라우저 엔진(Chrome, Whale, Samsung Internet)을 조합하여 랜덤 수천 가지 모바일 UA 생성"""
    
    # 1. 안드로이드 조합용 데이터
    android_versions = ["11", "12", "13", "14"]
    android_models = [
        "SM-S928N", "SM-S921N", "SM-S918N", "SM-G998N", "SM-A546B", "SM-A346B",
        "Pixel 8 Pro", "Pixel 7", "Pixel 6a", "Nothing Phone (2)", "Xperia 5 IV"
    ]
    
    # 동적으로 획득한 최신 버전과 그 이전 5개 버전 중 랜덤 선택
    latest_major = get_latest_chrome_major()
    chrome_major = random.randint(latest_major - 5, latest_major)
    chrome_ver = f"{chrome_major}.0.{random.randint(4000, 6000)}.{random.randint(100, 250)}"
    
    # 2. iOS 조합용 데이터
    ios_versions = ["15_7", "16_6", "17_1", "17_2", "17_3_1"]
    iphone_models = ["iPhone", "iPad"]
    safari_ver = f"{random.randint(15, 17)}.{random.randint(1, 6)}"

    # 3. 브라우저 유형 선택 (한국 시장 비중 고려: Chrome 40%, Samsung 30%, Whale 10%, Safari 20%)
    browser_type = random.choices(
        ["chrome", "samsung", "whale", "safari"], 
        weights=[40, 30, 10, 20], k=1
    )[0]

    if browser_type == "chrome":
        os_ver = random.choice(android_versions)
        model = random.choice(android_models)
        return f"Mozilla/5.0 (Linux; Android {os_ver}; {model}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_ver} Mobile Safari/537.36"
    
    elif browser_type == "samsung":
        os_ver = random.choice(android_versions)
        model_list = [m for m in android_models if "SM-" in m]
        model = random.choice(model_list) if model_list else "SM-S928N"
        samsung_ver = f"{random.randint(20, 23)}.0"
        return f"Mozilla/5.0 (Linux; Android {os_ver}; {model}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_ver} Mobile Safari/537.36 SamsungBrowser/{samsung_ver}"
        
    elif browser_type == "whale":
        os_ver = random.choice(android_versions)
        model = random.choice(android_models)
        whale_ver = f"{random.randint(3, 4)}.{random.randint(20, 25)}.{random.randint(200, 250)}.{random.randint(1, 10)}"
        return f"Mozilla/5.0 (Linux; Android {os_ver}; {model}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_ver} Mobile Safari/537.36 Whale/{whale_ver}"
        
    else: # safari (iOS)
        os_ver = random.choice(ios_versions)
        device = random.choice(iphone_models)
        return f"Mozilla/5.0 ({device}; CPU {device} OS {os_ver} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{safari_ver} Mobile/15E148 Safari/604.1"

def generate_desktop_ua() -> str:
    """Windows, Mac 환경의 최신 데스크탑 UA 생성"""
    latest_major = get_latest_chrome_major()
    chrome_major = random.randint(latest_major - 5, latest_major)
    chrome_ver = f"{chrome_major}.0.{random.randint(4000, 6000)}.{random.randint(100, 250)}"
    
    os_configs = [
        "Windows NT 10.0; Win64; x64",
        "Macintosh; Intel Mac OS X 10_15_7",
        "X11; Linux x86_64"
    ]
    os_type = random.choice(os_configs)
    
    return f"Mozilla/5.0 ({os_type}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_ver} Safari/537.36"

# 레거시 호환성을 위해 기존 이름 유지
def generate_random_ua() -> str:
    return generate_mobile_ua()
