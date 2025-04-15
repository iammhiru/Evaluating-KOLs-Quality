from bs4 import BeautifulSoup

with open("info/fanpage_chaubuiofficial.html", "r", encoding="utf-8") as f:
    html = f.read()

soup = BeautifulSoup(html, "html.parser")
print(soup.prettify())  # In ra toàn bộ HTML, format sạch
