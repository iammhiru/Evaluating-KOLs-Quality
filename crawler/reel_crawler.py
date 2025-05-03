from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import time
import random

def clean_caption_and_split(text: str):
    text = text.strip()
    if text.endswith("Ẩn bớt"):
        text = text[: -len("Ẩn bớt")].strip()

    words = text.split()
    hashtags = []

    for word in reversed(words):
        if word.startswith("#"):
            hashtags.insert(0, word)
        else:
            break

    content_words_count = len(words) - len(hashtags)
    content = ' '.join(words[:content_words_count]).strip()

    return content, hashtags

def crawl_fanpage_reels(driver, page_url):
    driver.get(page_url)
    time.sleep(3)

    try:
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.XPATH, "//div[@role='main']"))
        )
    except TimeoutException:
        print("⏰ Timeout đợi trang chính render xong.")
        return {}
    
    reels = []

    reels_url = page_url.rstrip("/") + ("/reels" if "id=" not in page_url else "&sk=reels_tab")
    driver.get(reels_url)
    time.sleep(random.uniform(2.6, 3.8))
    driver.execute_script("window.scrollBy(0, 450);")
    time.sleep(random.uniform(2.7, 3.4))
    checked_reels_id = set()
    reels_view = dict()

    for _ in range(3):
        time.sleep(2.6)
        reels_elements = driver.find_elements(By.XPATH, "//a[contains(@href, '/reel/')]")
        for reel in reels_elements:
            reel_id = reel.get_attribute("href").split('?')[0]
            reel_view = reel.text.strip()
            reels_view[reel_id.split("/")[-2]] = reel_view
            checked_reels_id.add(reel_id)
        driver.execute_script("window.scrollBy(0, 350);")
        time.sleep(3)

    # print(checked_reels_id)
    for reel in checked_reels_id:
        reel_info = dict()
        driver.get(reel)
        time.sleep(random.uniform(2.8, 3.6))
        reel_info['reel_id'] = reel.split("/")[-2]
        reel_info['views'] = reels_view[reel_info['reel_id']]
        reel_content = driver.find_element(By.XPATH, "//div[@aria-label=\"Thẻ trước đó\"]/following-sibling::div[1]")
        find_more_content_button = reel_content.find_elements(By.XPATH, ".//div[contains(text(), 'Xem thêm')]")
        if find_more_content_button:
            driver.execute_script("arguments[0].click();", find_more_content_button[0])
            time.sleep(random.uniform(1.6, 2.8))
        content = reel_content.text.strip().split("\n")[2]
        content, hashtags = clean_caption_and_split(content)
        reel_info['content'] = content
        reel_info['hashtags'] = hashtags
        reel_info['likes'] = driver.find_element(By.XPATH, "//div[@aria-label=\"Thích\"]/../following-sibling::div[1]").text.strip()
        reel_info['comments'] = driver.find_element(By.XPATH, "//div[@aria-label=\"Bình luận\"]/../following-sibling::div[1]").text.strip()
        reel_info['shares'] = driver.find_element(By.XPATH, "//div[@aria-label=\"Chia sẻ\"]/../following-sibling::div[1]").text.strip()
        reels.append(reel_info)

    return reels
