import time
import random
from fanpage_crawler import crawl_fanpage_info
from post_crawler import crawl_posts
from reel_crawler import crawl_fanpage_reels
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from concurrent.futures import ThreadPoolExecutor

kol_list = [
    {
        "name": "Thủng Long Family",
        "url": "https://www.facebook.com/thunglongfamily"
    },
    {
        "name": "Hoàng Luân",
        "url": "https://www.facebook.com/hoangluanblv"
    },
    {
        "name": "Văn Tùng",
        "url": "https://www.facebook.com/BLVVanTung.Box"
    },
    {
        "name": "Đặng Thu Hà",
        "url": "https://www.facebook.com/dangthuhaf.official"
    },
]

def split_kol_list(kols, n):
    k, m = divmod(len(kols), n)
    return [kols[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]

profile_dirs = [
    "C:/UserData/new_pf1",
]

def setup_driver(profile_dir):
    options = Options()
    options.add_argument(f"--user-data-dir={profile_dir}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    # options.add_argument("--headless=new")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(service=Service("E:/chromedriver-win64/chromedriver.exe"), options=options)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
        """
    })
    return driver

def crawl_worker(profile_dir, kol_sublist):
    driver = setup_driver(profile_dir)
    driver.get("https://www.facebook.com/")
    time.sleep(random.uniform(5, 8))

    for kol in kol_sublist:
        print(f"\n[{profile_dir}] Crawling {kol['name']}")
        try:
            page_id = crawl_fanpage_info(driver, kol['url'])
            crawl_posts(driver, kol['url'], page_id, 150)
            crawl_fanpage_reels(driver, kol['url'], page_id, 5)
            print(f"✅ {kol['name']} done")
        except Exception as e:
            print(f"❌ Error with {kol['name']}: {e}")

    driver.quit()

if __name__ == "__main__":
    kol_chunks = split_kol_list(kol_list, len(profile_dirs))
    with ThreadPoolExecutor(max_workers=len(profile_dirs)) as executor:
        executor.map(lambda args: crawl_worker(*args), zip(profile_dirs, kol_chunks))