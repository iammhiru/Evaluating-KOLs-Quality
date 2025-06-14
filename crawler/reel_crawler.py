from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
import time
import random
from utils import save_to_json, decode_comment_base64
import traceback
import sys

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

def crawl_fanpage_reels(driver, page_url, page_id, num_of_scroll, current_timestamp):
    try:  
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

        for _ in range(num_of_scroll):
            reels_elements = driver.find_elements(By.XPATH, "//a[contains(@href, '/reel/')]")
            for reel in reels_elements:
                reel_id = reel.get_attribute("href").split('?')[0]
                reel_view = reel.text.strip()
                reels_view[reel_id.split("/")[-2]] = reel_view
                checked_reels_id.add(reel_id)
            driver.execute_script("window.scrollBy(0, 350);")
            time.sleep(random.uniform(3, 3.5))

        for reel in checked_reels_id:
            reel_info = dict()
            driver.get(reel)
            time.sleep(random.uniform(2.8, 3.6))
            reel_info['url'] = reel
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
            reel_info['likes'] = driver.find_element(By.XPATH, "//div[@aria-label=\"Thích\"]/../..").text.strip()
            reel_info['comments_count'] = driver.find_element(By.XPATH, "//div[@aria-label=\"Bình luận\"]/../..").text.strip()
            reel_info['shares'] = driver.find_element(By.XPATH, "//div[@aria-label=\"Chia sẻ\"]/../..").text.strip()

            reel_comment_button = driver.find_elements(By.XPATH, "//div[@role='button' and contains(@aria-label, 'Bình luận')]")
            comment_id_set = set()
            if reel_comment_button:
                reel_comment_button = reel_comment_button[0]
                ActionChains(driver).move_to_element(reel_comment_button).click().perform()
                time.sleep(3)
                change_comment_button = driver.find_elements(By.XPATH, ".//div[@role='button' and contains(., 'hợp nhất')]")
                if change_comment_button:
                    change_comment_button = change_comment_button[0]
                    ActionChains(driver).move_to_element(change_comment_button).click().perform()
                    time.sleep(4)
                    comment_buttons = driver.find_elements(By.XPATH, "//div[@role='menuitem' and contains(., 'cả bình luận')]")
                    if comment_buttons:
                        comment_buttons[0].click()
                        time.sleep(2)
                    else:
                        print("Comment buttons not found")
                time.sleep(2)

                scrollable_zone = driver.find_elements(By.XPATH, "//div[@role='complementary']")
                scrollable_element = scrollable_zone[0].find_elements(By.XPATH, "./div[1]/div[1]/div")
                if scrollable_element:
                    scrollable_element = scrollable_element[0]
                    ran = random.randint(50, 75)
                    temp = 0
                    while True and temp < ran:
                        more_comment_button = driver.find_elements(By.XPATH, ".//div[@role='button' and contains(., 'Xem thêm bình luận')]")
                        if more_comment_button:
                            driver.execute_script("arguments[0].click();", more_comment_button[0])
                            time.sleep(random.uniform(2, 3))
                            temp += 1
                        else:
                            break

                    driver.execute_script("arguments[0].scrollTop += 300", scrollable_element)
                    time.sleep(random.uniform(3, 4))
                    comment_elements = driver.find_elements(By.XPATH, ".//div[contains(@aria-label, 'Bình luận dưới tên')]")

                    if comment_elements:
                        first_comment = comment_elements[0]

                        if first_comment:
                            time_anchor = first_comment.find_elements(By.XPATH, ".//a[contains(@href, 'reel') and contains(@href, 'comment_id')]")
                            if time_anchor:
                                ActionChains(driver).move_to_element(time_anchor[-1]).perform()
                                time.sleep(random.uniform(3, 5))
                                post_time = driver.find_elements(By.XPATH, "//span[contains(text(), 'Tháng') and contains(text(), 'lúc')]")
                                if post_time:
                                    reel_info['post_time'] = post_time[0].text.strip()
                                else:
                                    reel_info['post_time'] = None

                    save_to_json(reel_info, f"info/{current_timestamp}/reel/reel_info", f"{page_id}_{reel_info['reel_id']}.json")            
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
                            reel_id, comment_info['comment_id'] = decode_comment_base64(comment_id)
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
                            save_to_json(comment_info, f"info/{current_timestamp}/reel/comment", f"{page_id}_{reel_info['reel_id']}_{comment_info['comment_id']}.json")
        return reels
    except Exception as e:
        print("Lỗi xảy ra:")
        traceback.print_exc()
        _, _, tb = sys.exc_info()
        print(f"Lỗi ở dòng: {tb.tb_lineno}")
        return {}
    

def crawl_reel_without_comment(driver, page_url, page_id, num_of_scroll=2):
    try:  
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

        for _ in range(num_of_scroll):
            reels_elements = driver.find_elements(By.XPATH, "//a[contains(@href, '/reel/')]")
            for reel in reels_elements:
                reel_id = reel.get_attribute("href").split('?')[0]
                reel_view = reel.text.strip()
                reels_view[reel_id.split("/")[-2]] = reel_view
                checked_reels_id.add(reel_id)
            driver.execute_script("window.scrollBy(0, 350);")
            time.sleep(random.uniform(3, 3.5))

        for reel in checked_reels_id:
            reel_info = dict()
            driver.get(reel)
            time.sleep(random.uniform(2.8, 3.6))
            reel_info['url'] = reel
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
            reel_info['comments_count'] = driver.find_element(By.XPATH, "//div[@aria-label=\"Bình luận\"]/../following-sibling::div[1]").text.strip()
            reel_info['shares'] = driver.find_element(By.XPATH, "//div[@aria-label=\"Chia sẻ\"]/../following-sibling::div[1]").text.strip()

            reel_comment_button = driver.find_elements(By.XPATH, "//div[@role='button' and contains(@aria-label, 'Bình luận')]")
            # comment_list = list()
            comment_id_set = set()
            if reel_comment_button:
                reel_comment_button = reel_comment_button[0]
                ActionChains(driver).move_to_element(reel_comment_button).click().perform()
                time.sleep(3)
                change_comment_button = driver.find_elements(By.XPATH, ".//div[@role='button' and contains(., 'hợp nhất')]")
                if change_comment_button:
                    change_comment_button = change_comment_button[0]
                    ActionChains(driver).move_to_element(change_comment_button).click().perform()
                    time.sleep(2)
                    comment_buttons = driver.find_elements(By.XPATH, "//div[@role='menuitem' and contains(., 'Tất cả bình luận')]")
                    if comment_buttons:
                        comment_buttons[0].click()
                        time.sleep(2)
                    else:
                        print("Comment buttons not found")
                time.sleep(2)

                scrollable_zone = driver.find_elements(By.XPATH, "//div[@role='complementary']")
                scrollable_element = scrollable_zone[0].find_elements(By.XPATH, "./div[1]/div[1]/div")
                if scrollable_element:
                    scrollable_element = scrollable_element[0]
                    ran = 10
                    temp = 0
                    while True and temp < ran:
                        more_comment_button = driver.find_elements(By.XPATH, ".//div[@role='button' and contains(., 'Xem thêm bình luận')]")
                        if more_comment_button:
                            driver.execute_script("arguments[0].click();", more_comment_button[0])
                            time.sleep(random.uniform(2, 3))
                            temp += 1
                        else:
                            break

                    driver.execute_script("arguments[0].scrollTop += 300", scrollable_element)
                    time.sleep(random.uniform(3, 4))
                    comment_elements = driver.find_elements(By.XPATH, ".//div[contains(@aria-label, 'Bình luận dưới tên')]")

                    if comment_elements:
                        first_comment = comment_elements[0]

                        if first_comment:
                            time_anchor = first_comment.find_elements(By.XPATH, ".//a[contains(@href, 'reel') and contains(@href, 'comment_id')]")
                            if time_anchor:
                                ActionChains(driver).move_to_element(time_anchor[-1]).perform()
                                time.sleep(random.uniform(3, 5))
                                post_time = driver.find_elements(By.XPATH, "//span[contains(text(), 'Tháng') and contains(text(), 'lúc')]")
                                if post_time:
                                    reel_info['post_time'] = post_time[0].text.strip()
                                else:
                                    reel_info['post_time'] = None

                    save_to_json(reel_info, f"info/{time.strftime('%d%m%Y')}/reel/reel_info", f"{page_id}_{reel_info['reel_id']}.json")            
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
                            reel_id, comment_info['comment_id'] = decode_comment_base64(comment_id)
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
                            save_to_json(comment_info, f"info/{time.strftime('%d%m%Y')}/reel/comment", f"{page_id}_{reel_info['reel_id']}_{comment_info['comment_id']}.json")
            # reel_info['comments_count_crawl'] = len(comment_list)
            # reel_info['comments'] = comment_list
            # reels.append(reel_info)

        return reels
    except Exception as e:
        print("Lỗi xảy ra:")
        traceback.print_exc()
        _, _, tb = sys.exc_info()
        print(f"Lỗi ở dòng: {tb.tb_lineno}")
        return {}