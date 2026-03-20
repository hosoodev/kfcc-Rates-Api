import random

def generate_random_ua() -> str:
    """기종, OS, 브라우저 엔진(Chrome, Whale, Samsung Internet)을 조합하여 랜덤 수천 가지 UA 생성"""
    
    # 1. 안드로이드 조합용 데이터
    android_versions = ["11", "12", "13", "14"]
    android_models = [
        "SM-S928N", "SM-S921N", "SM-S918N", "SM-G998N", "SM-A546B", "SM-A346B",
        "Pixel 8 Pro", "Pixel 7", "Pixel 6a", "Nothing Phone (2)", "Xperia 5 IV"
    ]
    chrome_major = random.randint(115, 122)
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
