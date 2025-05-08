from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException, ElementClickInterceptedException
import time
import re
import random
from utils import save_to_json, decode_comment_base64

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
            driver.execute_script("window.scrollBy(0, 600);")
            time.sleep(random.uniform(2.5, 3.5))
            
            all_links = driver.find_elements(By.XPATH, "//a[contains(@href, '/posts/') or contains(@href, '/videos/') or contains(@href, 'story_fbid')]")
    
            for link in all_links:
                try:
                    url = link.get_attribute("href")
                    if page_url not in url:
                        continue
                    else:
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
        if 'posts' in url:
            post['base58_id'] = url.split("/")[-1]
        if 'videos' in url:
            post['base58_id'] = url.split("/")[-2]
        if 'story_fbid' in url:
            post['base58_id'] = url.split("?")[0].split("&")[0].lstrip('story_fbid=')
            post['post_id'] = url.split("&")[-1].lstrip('id=')

        if 'posts' in url or 'story_fbid' in url:
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
 
                stagnant_round = 0  
                while True:
                    cur_total_commment = len(comment_id_set)
                    driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable_element[0])
                    time.sleep(3)

                    comment_elements = driver.find_elements(By.XPATH, ".//div[contains(@aria-label, 'Bình luận dưới tên')]")
                    for comment in comment_elements:
                        comment_info = dict()
                        comment_anchor = comment.find_elements(By.XPATH, ".//a[contains(@href, 'comment_id')]")
                        if comment_anchor:
                            anchor_infor = comment_anchor[0].get_attribute("href")
                            if 'profile.php' in anchor_infor:
                                user_url = anchor_infor.split("&")[0]
                                comment_id = anchor_infor.split("&")[1].lstrip('comment_id=')
                            else:
                                user_url = anchor_infor.split("?")[0]
                                comment_id = anchor_infor.split("?")[1].split("&")[0].lstrip('comment_id=')
                            post['post_id'], comment_id = decode_comment_base64(comment_id)

                            if comment_id in comment_id_set:
                                continue

                            comment_info['user_url'] = user_url
                            comment_info['comment_id'] = comment_id
                            comment_info['user_name'] = comment_anchor[1].text.strip()
                            comment_text = comment_anchor[1].find_elements(By.XPATH, "./ancestor::span[2]/following-sibling::div")
                            comment_info['comment_text'] = comment_text[0].text.strip() if comment_text else None

                            emote_count = comment.find_elements(By.XPATH, ".//div[contains(@aria-label, 'xem ai đã bày tỏ cảm xúc')]")
                            if emote_count:
                                emote_count = emote_count[0].get_attribute("aria-label").split(" ")[0]
                                comment_info['emote_count'] = emote_count
                            else:
                                comment_info['emote_count'] = 0

                            comment_id_set.add(comment_id)
                            save_to_json(comment_info, f"info/{time.strftime('%d%m%Y')}/post/comment", f"{post['base58_id']}_{comment_info['comment_id']}.json")
                            comment_list.append(comment_info)
                    if len(comment_id_set) == cur_total_commment:
                        stagnant_round += 1
                        if stagnant_round >= 2:
                            break
                    else:
                        stagnant_round = 0   
                save_to_json(post, f"info/{time.strftime('%d%m%Y')}/post/post_info", f"{post['base58_id']}.json") 
                post['comments'] = comment_list
                post['crawl_posts'] = len(comment_id_set)
            except Exception as e:
                print(f"Error: {e}")
                post['post_info'] = None
        if 'videos' in url:
            try:
                time.sleep(3)
                post['url'] = page_url
                post['id'] = page_url.split("/")[-2]

                watch_feed = driver.find_elements(By.XPATH, "//div[@id='watch_feed']")
                if watch_feed:
                    watch_feed = watch_feed[0]
                    context = watch_feed.find_elements(By.XPATH, "./div/div/div/div/div[1]")
                    if context:
                        context = context[0]
                        post['content'] = context.text.strip().split("\n")[1]
                        total_comment = context.find_elements(By.XPATH, ".//span[contains(text(), 'bình luận')]")
                        if total_comment:
                            post['total_comment'] = total_comment[0].text.strip()
                        else:
                            post['total_comment'] = None
                        total_view = context.find_elements(By.XPATH, ".//span[contains(text(), 'lượt xem')]")
                        if total_view:
                            post['total_view'] = total_view[0].text.strip()
                        else:
                            post['total_view'] = None
                        emote_button = context.find_elements(By.XPATH, ".//span[contains(@aria-label, 'Xem ai đã bày tỏ cảm xúc')]/following-sibling::div[1]")
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
                    else:
                        post['context'] = None

                    right_zone = watch_feed.find_elements(By.XPATH, "./div/div/div/div/div[2]")
                    if right_zone:
                        right_zone = right_zone[0]

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

                        first_comment = driver.find_elements(By.XPATH, ".//div[contains(@aria-label, 'Bình luận dưới tên')]")
                        if first_comment:
                            first_comment = first_comment[0]
                            time_anchor = first_comment.find_elements(By.XPATH, ".//a[contains(@href, 'videos') and contains(@href, 'comment_id')]")
                            if time_anchor:
                                ActionChains(driver).move_to_element(time_anchor[-1]).perform()
                                time.sleep(2.5)
                                post_time = driver.find_elements(By.XPATH, "//span[contains(text(), 'Tháng') and contains(text(), 'lúc')]")
                                if post_time:
                                    post['post_time'] = post_time[0].text.strip()
                                else:
                                    post['post_time'] = None

                        save_to_json(post, f"info/{time.strftime('%d%m%Y')}/video/post_info", f"{post['id']}.json")
                        comment_id_set = set()
                        comment_list = list()
                        while True:
                            more_comment_button = driver.find_elements(By.XPATH, ".//div[@role='button' and contains(., 'Xem thêm bình luận')]")
                            if more_comment_button:
                                driver.execute_script("arguments[0].click();", more_comment_button[0])
                                time.sleep(random.uniform(3, 3.5))
                            else:
                                break

                        time.sleep(2)
                        comment_elements = driver.find_elements(By.XPATH, ".//div[contains(@aria-label, 'Bình luận dưới tên')]")
                        for comment in comment_elements:
                            comment_info = dict()
                            comment_anchor = comment.find_elements(By.XPATH, ".//a[contains(@href, 'comment_id')]")
                            comment_id = None
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
                                video_id, comment_info['comment_id'] = decode_comment_base64(comment_id)
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
                            if comment_id not in comment_id_set:
                                comment_id_set.add(comment_id)
                                # comment_list.append(comment_info)
                                save_to_json(comment_info, f"info/{time.strftime('%d%m%Y')}/video/comment", f"{post['id']}_{comment_info['comment_id']}.json")
            except Exception as e:
                print(f"Error: {e}")
                post['post_info'] = None
        posts.append(post)

    return posts
