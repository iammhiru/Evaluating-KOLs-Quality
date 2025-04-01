import time
from selenium.webdriver.common.by import By

def crawl_fanpage_info(driver, page_url):
    driver.get(page_url)
    time.sleep(5)

    data = {"url": page_url}

    try:
        name = driver.find_element(By.XPATH, '//h1').text
        data["name"] = name
    except:
        data["name"] = None

    try:
        bio = driver.find_element(By.XPATH, '//div[contains(text(),"Intro") or contains(text(),"Giới thiệu")]/following-sibling::div').text
        data["bio"] = bio
    except:
        data["bio"] = None

    try:
        followers = driver.find_element(By.XPATH, '//div[contains(text(),"followers") or contains(text(),"người theo dõi")]').text
        data["followers"] = followers
    except:
        data["followers"] = None

    try:
        likes = driver.find_element(By.XPATH, '//div[contains(text(),"likes") or contains(text(),"người thích trang này")]').text
        data["likes"] = likes
    except:
        data["likes"] = None

    try:
        links = driver.find_elements(By.XPATH, '//a[contains(@href, "instagram.com") or contains(@href, "http")]')
        data["external_links"] = list(set([l.get_attribute('href') for l in links]))
    except:
        data["external_links"] = []

    return data
