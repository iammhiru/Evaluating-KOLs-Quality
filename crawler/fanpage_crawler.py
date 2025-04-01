import time
from bs4 import BeautifulSoup

from bs4 import BeautifulSoup

def crawl_fanpage_info(driver, page_url):
    driver.get(page_url)
    time.sleep(5)

    page_html = driver.page_source
    soup = BeautifulSoup(page_html, 'html.parser')

    data = {"url": page_url}

    # Tên fanpage
    title_tag = soup.find('meta', attrs={'property': 'og:title'})
    data["name"] = title_tag['content'] if title_tag else None

    # Ảnh đại diện
    image_tag = soup.find('meta', attrs={'property': 'og:image'})
    data["profile_image"] = image_tag['content'] if image_tag else None

    # Bio & Likes/Followers
    desc_tag = soup.find('meta', attrs={'name': 'description'})
    if desc_tag:
        desc = desc_tag['content']
        data["bio"] = ""
        data["likes"] = None
        data["followers"] = None
        data["external_link"] = None

        # Phân tích chuỗi: "[Tên fanpage]. 3,865,969 likes · 3,192 talking about this. [bio]\n[link]"
        parts = desc.split('. ')
        if len(parts) > 1:
            stats_part = parts[1]  # "3,865,969 likes · 3,192 talking about this"
            if "likes" in stats_part:
                data["likes"] = stats_part.split(" likes")[0].strip()
            if "talking about this" in stats_part:
                talking = stats_part.split('·')
                if len(talking) > 1:
                    data["followers"] = talking[1].replace("talking about this", "").strip()

        # Phần còn lại là bio
        if len(parts) > 2:
            data["bio"] = parts[2].split('\n')[0].strip()

        # Lấy link (nếu có)
        if "https://" in desc:
            link_start = desc.find("https://")
            link_end = desc.find("\n", link_start)
            link = desc[link_start:] if link_end == -1 else desc[link_start:link_end]
            data["external_link"] = link.strip()

    return data
