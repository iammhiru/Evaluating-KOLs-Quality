import undetected_chromedriver as uc
import time
import os
import json
import csv
from fanpage_crawler import crawl_fanpage_info
from post_crawler import crawl_recent_posts
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from dotenv import load_dotenv

load_dotenv()

kol_list = [
    {
        "name": "Châu Bùi",
        "url": "https://www.facebook.com/chaubuiofficial"
    },
    {
        "name": "Đen Vâu",
        "url": "https://www.facebook.com/denvau"
    },
    {
        "name": "Giang Ơi",
        "url": "https://www.facebook.com/giangoivlog"
    },
    {
        "name": "Khoai Lang Thang",
        "url": "https://www.facebook.com/KhoaiLangThang"
    },
    {
        "name": "Ninh Tito",
        "url": "https://www.facebook.com/ninheating"
    },
    {
        "name": "Trinh Phạm",
        "url": "https://www.facebook.com/profile.php?id=100044592212208"
    },
    {
        "name": "Duy Thẩm",
        "url": "https://www.facebook.com/duythamchannel"
    },
    {
        "name": "Độ Mixi",
        "url": "https://www.facebook.com/MixiGaming"
    }
]


USER_DATA_DIR = "C:/Users/AD/AppData/Local/Google/Chrome/User Data"
PROFILE_NAME = "Default" 

def save_to_json(data, directory, filename):
    os.makedirs(directory, exist_ok=True)
    with open(f"{directory}/{filename}", 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def save_to_csv(data, filename):
    keys = data[0].keys()
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        dict_writer = csv.DictWriter(f, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)

if __name__ == "__main__":
    options = uc.ChromeOptions()
    options.add_argument(f"--user-data-dir={USER_DATA_DIR}")
    options.add_argument(f"--profile-directory={PROFILE_NAME}")
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = uc.Chrome(options=options)
    driver.maximize_window()

    driver.get("https://www.facebook.com/")

    for kol in kol_list:
        print(f"\n🔍 Crawling {kol['name']} ({kol['field']})")

        time.sleep(3.5)
        info = crawl_fanpage_info(driver, kol['url'])
        # posts = crawl_recent_posts(driver, scroll_count=5)

        result = {
            "name": kol["name"],
            "profile": info,
            "posts": None,
        }

        save_to_json(result, "info", f"{kol['name'].replace(' ', '_')}.json")
        print(f"✅ Đã lưu dữ liệu {kol['name']}")

    driver.quit()