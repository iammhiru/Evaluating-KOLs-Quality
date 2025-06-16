import time
import json
import random
import argparse
import os
from dotenv import load_dotenv
from fanpage_crawler import crawl_fanpage_info
from post_crawler import crawl_posts
from reel_crawler import crawl_fanpage_reels
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from concurrent.futures import ThreadPoolExecutor
from kafka.profile import main as produce_profile_files
from kafka.post_info import main as produce_post_info_files
from kafka.reel_info import main as produce_reel_info_files
from kafka.comment import main as produce_comment_files

with open('crawl_list.json', 'r', encoding='utf-8') as f:
    kol_list = json.load(f)

def split_kol_list(kols, n):
    k, m = divmod(len(kols), n)
    return [kols[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]

load_dotenv()
profile_dirs_raw = os.getenv("PROFILE_DIRS")
profile_dirs = [dir.strip() for dir in profile_dirs_raw.split(",") if dir.strip()]

def setup_driver(profile_dir):
    options = Options()
    options.add_argument(f"--user-data-dir={profile_dir}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--headless=new")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(service=Service("/usr/bin/chromedriver"), options=options)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
        """
    })
    return driver

def crawl_worker(profile_dir, kol_sublist, post_limit=10, reel_limit=1):
    driver = setup_driver(profile_dir)
    driver.get("https://www.facebook.com/")
    time.sleep(random.uniform(4, 6.5))
    current_timestamp = int(time.time())
    for kol in kol_sublist:
        print(f"\n[{profile_dir}] Crawling {kol['name']}")
        try:
            page_id = crawl_fanpage_info(driver, kol['url'], current_timestamp)
            crawl_posts(driver, kol['url'], page_id, post_limit, current_timestamp)
            crawl_fanpage_reels(driver, kol['url'], page_id, reel_limit, current_timestamp)
            print(f"✅ {kol['name']} done")
        except Exception as e:
            print(f"❌ Error with {kol['name']}: {e}")
    produce_profile_files(current_timestamp)
    produce_post_info_files(current_timestamp)
    produce_reel_info_files(current_timestamp)
    produce_comment_files(current_timestamp)
    driver.quit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Crawl KOL fanpage with custom limits"
    )
    parser.add_argument(
        "--post_limit", type=int, default=3,
    ) 
    parser.add_argument(
        "--reel_limit", type=int, default=1,
    )
    args = parser.parse_args()
    kol_chunks = split_kol_list(kol_list, len(profile_dirs))
    with ThreadPoolExecutor(max_workers=len(profile_dirs)) as executor:
        executor.map(lambda args: crawl_worker(*args), zip(profile_dirs, kol_chunks, [args.post_limit] * len(profile_dirs), [args.reel_limit] * len(profile_dirs)))