from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import time

def crawl_recent_posts(driver, days_limit=3):
    from datetime import datetime, timedelta
    import re
    import time

    now = datetime.now()
    cutoff_date = now - timedelta(days=days_limit)

    # Scroll để load thêm bài viết
    scroll_count = 0
    last_height = driver.execute_script("return document.body.scrollHeight")
    while scroll_count < 10:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
        scroll_count += 1

    # Dùng BeautifulSoup để parse lại HTML sau khi đã scroll
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    articles = soup.find_all("div", attrs={"role": "article"})

    posts_data = []
    for article in articles:
        try:
            text = article.get_text(separator="\n").strip()
            lines = text.split("\n")

            # Tìm thời gian đăng bài (ví dụ: "13h", "2d")
            post_time_str = None
            for line in lines:
                if re.match(r"^\d+[hd]$", line.strip()):
                    post_time_str = line.strip()
                    break

            if not post_time_str:
                continue

            if "h" in post_time_str:
                hours = int(post_time_str.replace("h", ""))
                post_time = now - timedelta(hours=hours)
            elif "d" in post_time_str:
                days = int(post_time_str.replace("d", ""))
                post_time = now - timedelta(days=days)
            else:
                post_time = now  # fallback

            if post_time < cutoff_date:
                continue

            # Tìm reactions
            reaction_index = -1
            for i, line in enumerate(lines):
                if "all reactions" in line.lower():
                    reaction_index = i
                    break

            reactions = 0
            if reaction_index != -1:
                for rline in lines[reaction_index + 1:]:
                    digits = re.findall(r"\d+", rline.replace(",", ""))
                    for d in digits:
                        reactions += int(d)

            # Nội dung bài viết chính
            content = ""
            for line in lines:
                if (
                    not any(keyword in line.lower() for keyword in ["verified", "public", "reaction", "comment", "share", "view", "like"])
                    and len(line.strip()) > 10
                ):
                    content = line.strip()
                    break

            posts_data.append({
                "text": content,
                "time_posted": post_time.strftime("%Y-%m-%d %H:%M"),
                "reactions": reactions
            })

        except Exception:
            continue

    return posts_data