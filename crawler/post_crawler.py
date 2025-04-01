from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
import time

def crawl_recent_posts(driver, days_limit=3):
    cutoff_date = datetime.now() - timedelta(days=days_limit)
    posts_data = []
    visited_texts = set()

    scroll_count = 0
    while True:
        articles = driver.find_elements(By.XPATH, '//div[@role="article"]')
        for article in articles:
            try:
                text = article.text.strip()
                if text in visited_texts or text == "":
                    continue
                visited_texts.add(text)

                try:
                    time_tag = article.find_element(By.XPATH, './/a[contains(@href,"/posts/") or contains(@href,"/videos/") or contains(@href,"/photos/")]')
                    tooltip = time_tag.get_attribute('aria-label') or time_tag.get_attribute('title')
                    if tooltip:
                        try:
                            post_date = datetime.strptime(tooltip, "%A, %B %d, %Y at %I:%M %p")
                        except:
                            post_date = datetime.now() 
                    else:
                        post_date = datetime.now()
                except:
                    post_date = datetime.now()

                if post_date < cutoff_date:
                    return posts_data

                posts_data.append({
                    "text": text,
                    "date": post_date.strftime("%Y-%m-%d %H:%M")
                })

            except Exception as e:
                continue

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        scroll_count += 1
        if scroll_count > 10: 
            break

    return posts_data
