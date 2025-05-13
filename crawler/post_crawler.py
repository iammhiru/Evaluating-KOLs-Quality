from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
import time
import re
import random
from utils import save_to_json, decode_comment_base64

def crawl_posts(driver, page_url, page_id, num_of_scroll=50):
    driver.get(page_url)
    
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//div[@role='main']"))
        )
    except TimeoutException:
        print("Timeout waiting for main content")
        return []

    list_urls = set()
    driver.execute_script("window.scrollBy(0, 550);")

    for _ in range(num_of_scroll):
            driver.execute_script("""
                const offset = 0 + Math.random() * 400;
                window.scrollBy(0, 1800 + offset);
            """)
            time.sleep(random.uniform(3.5, 4.5))
            
            all_links = driver.find_elements(By.XPATH, "//a[contains(@href, '/posts/') or contains(@href, '/videos/') or contains(@href, 'story_fbid')]")
            for link in all_links:
                try:
                    url = link.get_attribute("href")
                    if page_url not in url and 'story_fbid' not in url and 'videos' not in url:
                        continue
                    else:
                        if 'story_fbid' in url:
                            url = url.split("&")
                            url = '&'.join(url[:2])
                        else:
                            url = url.split("?")[0]
                        print(url)
                        list_urls.add(url)

                except (Exception) as e:
                    print(f"Error processing link: {e}")
                    continue
    for list_url in list_urls:
        print(list_url)
    for url in list_urls:
        driver.get(url)
        time.sleep(random.uniform(3, 3.5))
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
                    time.sleep(random.uniform(2.5, 3.5))
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
                time.sleep(random.uniform(2, 2.5))
                total_comment = driver.find_elements(By.XPATH, ".//span[contains(text(), 'bình luận')]")
                if total_comment:
                    post['total_comment'] = total_comment[-1].text.strip()
                    print(post['total_comment'])
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
                    time.sleep(random.uniform(4, 4.5))
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
                    time.sleep(random.uniform(2.5, 3.5))
                    comment_buttons = driver.find_elements(By.XPATH, "//div[@role='menuitem' and contains(., 'Tất cả bình luận')]")
                    if comment_buttons:
                        comment_buttons[0].click()
                        time.sleep(random.uniform(3, 3.5))
                    else:
                        print("Comment buttons not found")
                comment_id_set = set()
 
                prev_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_element[0])

                ran = random.randint(150, 175)
                temp = 0
                while True and temp < ran:
                    driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable_element[0])
                    time.sleep(random.uniform(2.5, 3))
                    new_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_element[0])
                    if new_height == prev_height:
                        break
                    prev_height = new_height
                    temp += 1

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
                        save_to_json(comment_info, f"info/{time.strftime('%d%m%Y')}/post/comment", f"{page_id}_{post['post_id']}_{comment_info['comment_id']}.json")
                save_to_json(post, f"info/{time.strftime('%d%m%Y')}/post/post_info", f"{page_id}_{post['post_id']}.json") 
                post['crawl_posts'] = len(comment_id_set)
            except Exception as e:
                print(f"Error: {e}")
                post['post_info'] = None
        if 'videos' in url:
            try:
                time.sleep(3)
                post['url'] = url
                post['id'] = url.split("/")[-2]

                pause_button = driver.find_elements(By.XPATH, "//div[@role='button' and contains(@aria-label, 'Tạm dừng')]")
                if pause_button:
                    pause_button = pause_button[0]
                    ActionChains(driver).move_to_element(pause_button).click().perform()
                    time.sleep(random.uniform(2, 2.5))

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

                        save_to_json(post, f"info/{time.strftime('%d%m%Y')}/video/post_info", f"{page_id}_{post['id']}.json")
                        comment_id_set = set()

                        ran = random.randint(200, 350)
                        temp = 0
                        while True and temp < ran:
                            more_comment_button = driver.find_elements(By.XPATH, ".//div[@role='button' and contains(., 'Xem thêm bình luận')]")
                            if more_comment_button:
                                driver.execute_script("arguments[0].click();", more_comment_button[0])
                                time.sleep(random.uniform(2, 2.5))
                                temp += 1
                            else:
                                break

                        time.sleep(2)
                        comment_elements = driver.find_elements(By.XPATH, ".//div[contains(@aria-label, 'Bình luận dưới tên')]")

                        first_comment = comment_elements[-1]
                        if first_comment:
                            time_anchor = first_comment.find_elements(By.XPATH, ".//a[contains(@href, 'videos') and contains(@href, 'comment_id')]")
                            if time_anchor:
                                ActionChains(driver).move_to_element(time_anchor[-1]).perform()
                                time.sleep(2.5)
                                post_time = driver.find_elements(By.XPATH, "//span[contains(text(), 'Tháng') and contains(text(), 'lúc')]")
                                if post_time:
                                    post['post_time'] = post_time[0].text.strip()
                                else:
                                    post['post_time'] = None
                                    
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
                                save_to_json(comment_info, f"info/{time.strftime('%d%m%Y')}/video/comment", f"{page_id}_{post['id']}_{comment_info['comment_id']}.json")
            except Exception as e:
                print(f"Error: {e}")
                post['post_info'] = None

    return 1

def crawl_posts_fix_comment_count(driver, page_url, page_id, num_of_scroll=50):
    driver.get(page_url)
    
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//div[@role='main']"))
        )
    except TimeoutException:
        print("Timeout waiting for main content")
        return []

    list_urls = set()
    driver.execute_script("window.scrollBy(0, 550);")

    for _ in range(num_of_scroll):
            driver.execute_script("""
                const offset = 0 + Math.random() * 400;
                window.scrollBy(0, 2200 + offset);
            """)
            time.sleep(random.uniform(2.5, 3.5))
            
            all_links = driver.find_elements(By.XPATH, "//a[contains(@href, '/posts/') or contains(@href, '/videos/') or contains(@href, 'story_fbid')]")

            for link in all_links:
                try:
                    url = link.get_attribute("href")
                    if page_url not in url and 'story_fbid' not in url and 'videos' not in url:
                        continue
                    else:
                        if 'story_fbid' in url:
                            url = url.split("&")
                            url = '&'.join(url[:2])
                        else:
                            url = url.split("?")[0]
                        list_urls.add(url)

                except (Exception) as e:
                    print(f"Error processing link: {e}")
                    continue
        
    for url in list_urls:
        driver.get(url)
        time.sleep(random.uniform(3, 3.5))
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
                    time.sleep(random.uniform(2.5, 3.5))
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
                time.sleep(random.uniform(2, 2.5))
                total_comment = driver.find_elements(By.XPATH, ".//span[contains(text(), 'bình luận')]")
                if total_comment:
                    if len(total_comment) == 1:
                        post['total_comment'] = total_comment[0].text.strip()
                    else:
                        post['total_comment'] = total_comment[1].text.strip()
                    print(post['total_comment'])
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
                    time.sleep(random.uniform(4, 4.5))
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
                    time.sleep(random.uniform(2.5, 3.5))
                    comment_buttons = driver.find_elements(By.XPATH, "//div[@role='menuitem' and contains(., 'Tất cả bình luận')]")
                    if comment_buttons:
                        comment_buttons[0].click()
                        time.sleep(random.uniform(3, 3.5))
                    else:
                        print("Comment buttons not found")
                comment_id_set = set()
 
                prev_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_element[0])

                ran = 1
                temp = 0
                while True and temp < ran:
                    driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable_element[0])
                    time.sleep(random.uniform(2.5, 3))
                    new_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_element[0])
                    if new_height == prev_height:
                        break
                    prev_height = new_height
                    temp += 1

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
                        save_to_json(comment_info, f"info/{time.strftime('%d%m%Y')}/post/comment", f"{page_id}_{post['post_id']}_{comment_info['comment_id']}.json")
                save_to_json(post, f"info/{time.strftime('%d%m%Y')}/post/post_info", f"{page_id}_{post['post_id']}.json") 
                post['crawl_posts'] = len(comment_id_set)
            except Exception as e:
                print(f"Error: {e}")
                post['post_info'] = None

    return 1

