from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException, ElementClickInterceptedException
import time
import re
import random
import base64
import urllib.parse

def decode_comment_base64(comment_str):
    try:
        comment_str = urllib.parse.unquote(comment_str)
        decoded = base64.b64decode(comment_str).decode("utf-8")
        match = re.search(r'comment:\d+_(\d+)', decoded)
        return match.group(1) if match else None
    except Exception as e:
        print(f"Decode error: {e}")
        return None

def crawl_posts(driver, page_url, num_of_scroll=50):
    driver.get(page_url)
    
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//div[@role='main']"))
        )
    except TimeoutException:
        print("Timeout waiting for main content")
        return []

    posts = []
    list_urls = set()
    driver.execute_script("window.scrollBy(0, 550);")

    for _ in range(num_of_scroll):
            driver.execute_script("window.scrollBy(0, 500);")
            time.sleep(random.uniform(2.5, 3.5))
            
            all_links = driver.find_elements(By.XPATH, "//a[contains(@href, '/posts/') or contains(@href, 'story_fbid')]")
            
            for link in all_links:
                try:
                    url = link.get_attribute("href")
                    if 'story_fbid' in url:
                        url = url.split("&")
                        url = '&'.join(url[:2])
                    else:
                        url = url.split("?")[0]
                    list_urls.add(url)
                except (StaleElementReferenceException, Exception) as e:
                    print(f"Error processing link: {e}")
                    continue
        
    for url in list_urls:
        driver.get(url)
        time.sleep(3)
        post = {}
        post['url'] = url
                
        try:
            dialog = driver.find_elements(By.XPATH, "//div[@role='dialog']")
            if dialog:
                dialog = dialog[-1]
            else:
                post['content'] = None

            time_anchor = dialog.find_elements(By.XPATH, ".//a[contains(@href, '__cft__[0]') and contains(@href, '__tn__')]")
            if time_anchor:
                time_anchor = time_anchor[2]
                ActionChains(driver).move_to_element(time_anchor).perform()
                time.sleep(2.5)
                post_time = driver.find_elements(By.XPATH, "//span[contains(text(), 'Tháng') and contains(text(), 'lúc')]")
                if post_time:
                    post['post_time'] = post_time[0].text.strip()
                else:
                    post['post_time'] = None

            content = dialog.find_elements(By.XPATH, ".//div[@data-ad-rendering-role='story_message']")
            if content:
                post['content'] = content[0].text.strip()
            else:
                post['content'] = None
            scrollable_element = dialog.find_elements(By.XPATH, "./div/div/div/div[2]")
            if scrollable_element:
                driver.execute_script("arguments[0].scrollTop += 250", scrollable_element[0])
            time.sleep(3)
            total_comment = driver.find_elements(By.XPATH, ".//span[contains(text(), 'bình luận')]")
            if total_comment:
                post['total_comment'] = total_comment[0].text.strip()
            else:
                post['total_comment'] = None

            total_share = driver.find_elements(By.XPATH, ".//span[contains(text(), 'lượt chia sẻ')]")
            if total_share:
                post['total_share'] = total_share[0].text.strip()
            else:
                post['total_share'] = None

            emote_button = dialog.find_elements(By.XPATH, ".//div[@role='button'][.//div[contains(text(), 'Tất cả cảm xúc')]]")
            if emote_button:
                emote_button = emote_button[0]
                ActionChains(driver).move_to_element(emote_button).click().perform()
                time.sleep(3.5)
                total_emote = driver.find_elements(By.XPATH, "//div[contains(@aria-label, 'đã bày tỏ cảm xúc') and contains(@aria-label, 'Hiển thị')]")
                post['emotes'] = {}
                if total_emote:
                    for emote in total_emote:
                        text = emote.get_attribute("aria-label")
                        number = re.search(r'[\d.,]+', text).group()
                        label = text.split("cảm xúc", 1)[-1].strip()
                        if label == "Tất cả":
                            post['emotes'][label] = number
                        else:
                            emote_count = emote.find_elements(By.XPATH, ".//span")
                            if emote_count:
                                post['emotes'][label] = emote_count[0].text.strip()
                                if post['emotes'][label] == "":
                                    post['emotes'][label] = number
                            else:
                                post['emotes'][label] = 0
                else:
                    post['emotes'] = None
                close_button = driver.find_elements(By.XPATH, "//div[@aria-label='Đóng' and @role='button']")
                clicked = False
                for btn in close_button:
                    try:
                        driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                        time.sleep(1)
                        btn.click()
                        clicked = True
                        break
                    except ElementClickInterceptedException:
                        print("⚠️ Button bị che, thử cái tiếp theo...")
                    except Exception as e:
                        print(f"❌ Không thể click nút: {e}")
                if not clicked:
                    print("❌ Không có nút Đóng nào click được.")
                time.sleep(2)
            else:
                post['emotes'] = None
            time.sleep(1.5)
            change_comment_button = driver.find_elements(By.XPATH, ".//div[@role='button' and contains(., 'hợp nhất')]")
            if change_comment_button:
                change_comment_button = change_comment_button[0]
                ActionChains(driver).move_to_element(change_comment_button).click().perform()
                time.sleep(2)
                comment_buttons = driver.find_elements(By.XPATH, "//div[@role='menuitem' and contains(., 'Tất cả bình luận')]")
                if comment_buttons:
                    comment_buttons[0].click()
                    time.sleep(3)
                else:
                    print("Comment buttons not found")
            comment_list = list()
            comment_id_set = set()
            for i in range(10):
                driver.execute_script("arguments[0].scrollTop += 150", scrollable_element[0])
                time.sleep(2)
                comment_elements = driver.find_elements(By.XPATH, ".//div[contains(@aria-label, 'Bình luận dưới tên')]")
                for comment in comment_elements:
                    comment_info = dict()
                    comment_anchor = comment.find_elements(By.XPATH, ".//a[contains(@href, 'comment_id')]")
                    if comment_anchor:
                        anchor_infor = comment_anchor[0].get_attribute("href")
                        user_url = None
                        if 'profile.php' in anchor_infor:
                            user_url = anchor_infor.split("&")[0]
                            comment_id = anchor_infor.split("&")[1].lstrip('comment_id=')
                        else:
                            user_url = anchor_infor.split("?")[0]
                            comment_id = anchor_infor.split("?")[1].split("&")[0].lstrip('comment_id=')
                        comment_info['user_url'] = user_url
                        comment_id = decode_comment_base64(comment_id)
                        if comment_id in comment_id_set:
                            continue
                        comment_info['comment_id'] = comment_id
                        comment_info['user_name'] = comment_anchor[1].text.strip()
                        comment_text = comment_anchor[1].find_elements(By.XPATH, "./ancestor::span[2]/following-sibling::div")
                        if comment_text:
                            comment_info['comment_text'] = comment_text[0].text.strip()
                        else:
                            comment_info['comment_text'] = None
                    emote_count = comment.find_elements(By.XPATH, ".//div[contains(@aria-label, 'xem ai đã bày tỏ cảm xúc')]")
                    if emote_count:
                        emote_count = emote_count[0].get_attribute("aria-label")
                        emote_count = emote_count.split(" ")[0]
                        comment_info['emote_count'] = emote_count
                    else:
                        comment_info['emote_count'] = 0
                    comment_id_set.add(comment_id)
                    comment_list.append(comment_info)
            post['comments'] = comment_list
        except NoSuchElementException:
            print("Element not found")
            post['post_info'] = None
        posts.append(post)
        
    return posts
