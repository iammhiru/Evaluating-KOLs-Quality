from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import time
import random
from utils import save_to_json

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
    try:
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
        
        save_to_json(info, f"info/{time.strftime('%d%m%Y')}/profile", f"{info['name'].replace(' ', '_')}.json")
        return info
    except Exception as e:
        print(f"❌ Lỗi khi crawl thông tin fanpage: {e}")
        return None