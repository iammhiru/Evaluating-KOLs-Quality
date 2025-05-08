import time
from fanpage_crawler import crawl_fanpage_info
from post_crawler import crawl_posts, dev_crawl_vid
from reel_crawler import crawl_fanpage_reels
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
from utils import save_to_json, save_to_csv

load_dotenv()

kol_list = [
    # {
    #     "name": "Trinh Phạm",
    #     "url": "https://www.facebook.com/profile.php?id=100044592212208"
    # },
    {
        "name": "Châu Bùi",
        "url": "https://www.facebook.com/chaubui.official"
    },
    # {
    #     "name": "Đen Vâu",
    #     "url": "https://www.facebook.com/denvau"
    # },
    # {
    #     "name": "Giang Ơi",
    #     "url": "https://www.facebook.com/giangoivlog"
    # },
    # {
    #     "name": "Khoai Lang Thang",
    #     "url": "https://www.facebook.com/KhoaiLangThang"
    # },
    # {
    #     "name": "Độ Mixi",
    #     "url": "https://www.facebook.com/MixiGaming"
    # }
]

if __name__ == "__main__":
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    options.add_argument(r"--user-data-dir=C:/UserData/selenium_profile")
    # options.add_argument("--headless=new")
    driver = webdriver.Chrome(service=Service("E:\chromedriver-win64\chromedriver.exe"), options=options)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
        """
    })

    driver.get("https://www.facebook.com/")

    # with open("data/kol_list.json", "r", encoding="utf-8") as f:
    #     kol_list = json.load(f)

    for kol in kol_list:
        print(f"\n🔍 Crawling {kol['name']}")

        time.sleep(3.5)
        # posts = crawl_posts(driver, kol['url'])
        vid = dev_crawl_vid(driver, kol['url'])
        # info = crawl_fanpage_info(driver, kol['url'])
        # reels = crawl_fanpage_reels(driver, kol['url'])

        result = {
            # "profile": info,
            # "reels": reels,
            # "posts": posts,
            "vid": vid,
        }

        save_to_json(result, "info", f"{kol['name'].replace(' ', '_')}demo.json")
        print(f"✅ Đã lưu dữ liệu {kol['name']}")

    driver.quit()