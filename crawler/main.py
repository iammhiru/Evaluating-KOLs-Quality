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
        "name": "Đen Vâu",
        "field": "Âm nhạc (Rapper)",
        "url": "https://www.facebook.com/denvau"
    },
    {
        "name": "Châu Bùi",
        "field": "Thời trang, Lifestyle",
        "url": "https://www.facebook.com/chaubuiofficial"
    },
    {
        "name": "Giang Ơi",
        "field": "Lifestyle, Giáo dục",
        "url": "https://www.facebook.com/giangoivlog"
    },
    {
        "name": "Khoai Lang Thang",
        "field": "Du lịch, Ẩm thực",
        "url": "https://www.facebook.com/khoailangthangblogger"
    },
    {
        "name": "Ninh Tito",
        "field": "Ẩm thực, Du lịch",
        "url": "https://www.facebook.com/ninhtito"
    },
    {
        "name": "Trinh Phạm",
        "field": "Làm đẹp, Mỹ phẩm",
        "url": "https://www.facebook.com/trinh.phamm"
    },
    {
        "name": "Duy Thẩm",
        "field": "Công nghệ, Đánh giá sản phẩm",
        "url": "https://www.facebook.com/duytham.reviews"
    },
    {
        "name": "Độ Mixi",
        "field": "Giải trí, Streamer",
        "url": "https://www.facebook.com/MixiGaming"
    }
]


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

def login(driver, email, password):
    driver.get("https://www.facebook.com/login")
    time.sleep(2)
    driver.find_element(By.ID, "email").send_keys(email)
    driver.find_element(By.ID, "pass").send_keys(password)
    driver.find_element(By.NAME, "login").click()
    time.sleep(5)


if __name__ == "__main__":
    options = Options()
    options.headless = True  
    driver = uc.Chrome(options=options)
    driver.maximize_window()

    email = os.getenv('EMAIL')
    password = os.getenv('PASSWORD')

    login(driver, email, password)

    for kol in kol_list:
        print(f"🔍 Crawling {kol['name']} ({kol['field']})")
        info = crawl_fanpage_info(driver, kol['url'])
        posts = crawl_recent_posts(driver)
        result = {
            "name": kol["name"],
            "field": kol["field"],
            "profile": info,
            "posts": posts
        }
        save_to_json(result, "info", f"{kol['name'].replace(' ', '_')}.json")

    driver.quit()
