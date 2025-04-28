from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException
import time
import re
import random

def crawl_posts(driver, page_url, num_of_scroll=50):
    driver.get(page_url)
    
    try:
        WebDriverWait(driver, 7).until(
            EC.presence_of_element_located((By.XPATH, "//div[@role='main']"))
        )
    except TimeoutException:
        print("Timeout waiting for main content")
        return []

    posts = []
    list_urls = set()
    driver.execute_script("window.scrollBy(0, 550);")

    for _ in range(num_of_scroll):
            driver.execute_script("window.scrollBy(0, 450);")
            time.sleep(5)
            
            all_links = driver.find_elements(By.XPATH, "//a[contains(@href, '/posts/') or contains(@href, '/videos/') or contains(@href, 'story_fbid')]")
            
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
                time.sleep(3)
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
            time.sleep(4)
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
                time.sleep(4)
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
                if close_button:
                    if len(close_button) > 3:
                        close_button[2].click()
                    else:
                        close_button[-1].click()
                else:
                    print("Close button not found")
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
                        anchor_infor = comment_anchor[1].get_attribute("href")
                        user_url, query = anchor_infor.split("?")
                        comment_id = query.split("&")[0].lstrip('comment_id=')
                        comment_info['user_url'] = user_url
                        comment_info['comment_id'] = comment_id
                        comment_info['user_name'] = comment_anchor[1].text.strip()
                        comment_text = comment_anchor[1].find_elements(By.XPATH, "./ancestor::span[2]/following-sibling::div")
                        if comment_text:
                            comment_info['comment_text'] = comment_text[0].text.strip()
                        else:
                            comment_info['comment_text'] = None
                    # if comment_id not in comment_id_set:
                    #     time_stamp_anchor = comment.find_elements(By.XPATH, ".//a[contains(@href, 'https://')]")
                    #     time_stamp_anchor = time_stamp_anchor[2]
                    #     ActionChains(driver).move_to_element(time_stamp_anchor).perform()
                    #     time.sleep(2)
                    #     comment_post_time = driver.find_elements(By.XPATH, "//span[contains(text(), 'Tháng') and contains(text(), 'lúc')]")
                    #     if comment_post_time:
                    #         comment_info['post_time'] = comment_post_time[0].text.strip()
                    #     else:
                    #         comment_info['post_time'] = None
                    #     ActionChains(driver).move_by_offset(random.randint(-60, -50), 0).perform()
                    emote_count = comment.find_elements(By.XPATH, ".//div[contains(@aria-label, 'xem ai đã bày tỏ cảm xúc')]")
                    if emote_count:
                        emote_count = emote_count[0].text.strip()
                        comment_info['emote_count'] = emote_count
                    else:
                        comment_info['emote_count'] = 0
                    if comment_id not in comment_id_set:
                        comment_id_set.add(comment_id)
                        comment_list.append(comment_info)
            post['comments'] = comment_list
        except NoSuchElementException:
            print("Element not found")
            post['post_info'] = None
        posts.append(post)
        
    return posts
