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


def crawl_fanpage_info(driver, page_url):
    def safe_get(xpath):
        try:
            element = driver.find_element(By.XPATH, xpath)
            return element.text.strip()
        except NoSuchElementException:
            return None
        
    def check_verified_account():
        try:
            driver.find_element(By.XPATH, "//svg[contains(@title, 'xác minh')]")
            return True
        except NoSuchElementException:
            return False
        
    driver.get(page_url)
    time.sleep(3)

    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//div[@role='main']"))
        )
    except TimeoutException:
        print("⏰ Timeout đợi trang chính render xong.")
        return {}
    
    info = {}

    info['name'] = safe_get("//div[@role='main']//h1")
    info['followers_count'] = safe_get("//a[contains(@href, 'followers')]")
    info['following_count'] = safe_get("//a[contains(@href, 'following')]") 
    info['verified_account'] = check_verified_account()

    about_url = page_url.rstrip("/") + ("/about" if "id=" not in page_url else "&sk=about")
    driver.get(about_url)
    time.sleep(4)

    driver.execute_script("window.scrollBy(0, 400);")
    time.sleep(3)

    basic_info = driver.find_elements(By.XPATH, "//a[contains(@href, 'about_contact_and_basic_info')]")
    if basic_info:
        basic_info = basic_info[0]
        driver.execute_script("arguments[0].click();", basic_info)
        time.sleep(2)
        info['category'] = driver.find_elements(By.XPATH, "//span[contains(text(), \"Hạng mục\")]/../../../../div[2]")
        if info['category']:
            info['category'] = info['category'][0].text.strip()
        else:
            info['category'] = None

        info['contact'] = driver.find_elements(By.XPATH, "//span[contains(text(), \"Thông tin liên hệ\")]/../../../../..//ul")
        if info['contact']:
            lines = info['contact'][0].text.strip().split("\n")
            info['contact'] = {}
            if len(lines) % 2 == 1:
                lines = lines[1:]
            for i in range(0, len(lines) - 1, 2):
                value = lines[i].strip()
                label = lines[i + 1].strip()
                info['contact'][label] = value
        else:
            info['contact'] = None

        info['social_links'] = driver.find_elements(By.XPATH, "//span[contains(text(), \"liên kết mạng xã hội\")]/../../../..")
        if info['social_links']:
            lines = info['social_links'][0].text.strip().split("\n")
            info['social_links'] = {}
            if len(lines) > 2 and not (":" in lines[0] or "@" in lines[0] or lines[0].startswith("http")):
                lines = lines[1:]
            for i in range(0, len(lines) - 1, 2):
                value = lines[i].strip()
                label = lines[i + 1].strip()
                info['social_links'][label] = value
        else:
            info['social_links'] = None

    transparency_a = driver.find_elements(By.XPATH, "//a[contains(@href, 'about_profile_transparency')]")
    if transparency_a:
        transparency_a = transparency_a[0]
        driver.execute_script("arguments[0].click();", transparency_a)
        time.sleep(3)
        page_id = driver.find_element(By.XPATH, "//span[contains(text(), \"ID Trang\")]/../../../../div[1]").text.strip()
        if page_id == "":
            page_id = None
        else:
            info['page_id'] = page_id

        create_date = driver.find_element(By.XPATH, "//span[contains(text(), \"Ngày tạo\")]/../../../../div[1]").text.strip()
        if create_date == "":
            create_date = None
        else:
            info['create_date'] = create_date


    return info

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
