from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def crawl_post_by_click(driver, a_element):
    try:
        # Click vào link bài viết
        driver.execute_script("arguments[0].click();", a_element)
        time.sleep(2.5)  # Đợi overlay modal mở ra

        # Tìm overlay/modal bài viết
        post_container = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, '//div[@role="dialog"]'))
        )
        post_text = post_container.text
        print("📝 Nội dung bài viết:", post_text[:150], "...")

        # Tìm và bấm nút đóng (X)
        close_btn = post_container.find_element(By.XPATH, './/div[@aria-label="Đóng" or @aria-label="Close"]')
        driver.execute_script("arguments[0].click();", close_btn)
        time.sleep(1.5)

        return post_text

    except Exception as e:
        print(f"❌ Lỗi khi crawl bài viết (click): {e}")
        return None

def crawl_recent_posts(driver, scroll_count=5):
    seen_links = set()
    all_posts = []

    for i in range(scroll_count):
        print(f"\n🔄 Scroll {i+1}/{scroll_count}...")
        driver.execute_script("window.scrollBy(0, 700);")
        time.sleep(2)

        a_tags = driver.find_elements(By.XPATH, '//a[contains(@href, "/posts/") or contains(@href, "/permalink/")]')

        for a in a_tags:
            href = a.get_attribute("href")
            if not href or href in seen_links:
                continue

            seen_links.add(href)
            print(f"👉 Đang mở bài viết: {href}")
            post_text = crawl_post_by_click(driver, a)
            if post_text:
                all_posts.append(post_text)

    print(f"\n✅ Đã crawl {len(all_posts)} bài viết.")
    return all_posts
