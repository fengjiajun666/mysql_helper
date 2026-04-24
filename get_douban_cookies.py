import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

def get_driver():
    options = Options()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    driver = webdriver.Chrome(options=options)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    })
    return driver

def save_cookies():
    driver = get_driver()
    driver.get('https://movie.douban.com/top250')
    print("👉 请在弹出的浏览器中扫码或输入账号密码登录...")
    try:
        WebDriverWait(driver, 300).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".nav-user-account"))
        )
        print("✅ 登录成功，正在保存 Cookies...")
    except Exception as e:
        print("❌ 登录超时或失败，请重试")
        driver.quit()
        return

    cookies = driver.get_cookies()
    # 改为保存全部
    with open('douban_cookies.json', 'w', encoding='utf-8') as f:
        json.dump(cookies, f, ensure_ascii=False, indent=2)
    print(f"✅ 已保存 {len(cookies)} 个 Cookies 到 douban_cookies.json")
    driver.quit()

if __name__ == "__main__":
    save_cookies()