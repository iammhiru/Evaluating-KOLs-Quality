import undetected_chromedriver as uc
import time
import os
import json
import csv
from fanpage_crawler import crawl_fanpage_info
from post_crawler import crawl_posts
from reel_crawler import crawl_fanpage_reels
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv

load_dotenv()

kol_list = [
    # {
    #     "name": "Trinh Phạm",
    #     "url": "https://www.facebook.com/profile.php?id=100044592212208"
    # },
    {
        "name": "Châu Bùi",
        "url": "https://www.facebook.com/chaubuiofficial"
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

def save_to_json(data, directory, filename):
    os.makedirs(directory, exist_ok=True)
    with open(f"{directory}/{filename}", 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def save_profile(profile, directory, filename):
    os.makedirs(directory, exist_ok=True)
    try:
        with open(f"{directory}/{filename}", 'a', encoding='utf-8') as f:
            data = json.dump(profile, ensure_ascii=False, indent=4)
            f.write(data + "\n")
            f.flush()
    except Exception as e:
        print(f"Error saving profile: {e}")

def save_to_csv(data, filename):
    keys = data[0].keys()
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        dict_writer = csv.DictWriter(f, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)

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

    # Giấu navigator.webdriver (một dấu hiệu nhận dạng bot)
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
        posts = crawl_posts(driver, kol['url'])
        info = crawl_fanpage_info(driver, kol['url'])
        reels = crawl_fanpage_reels(driver, kol['url'])

        result = {
            "name": kol["name"],
            "profile": info,
            "reels": reels,
            # "profile": None,
            # "reels": None,
            "posts": posts,
        }

        save_to_json(result, "info", f"{kol['name'].replace(' ', '_')}demo.json")
        print(f"✅ Đã lưu dữ liệu {kol['name']}")

    driver.quit()