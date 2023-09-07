import requests
from bs4 import BeautifulSoup
import re

url = f"https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16333.16334.16335.16336.16337"
page = requests.get(url)

soup = BeautifulSoup(page.content, "html.parser")

table = soup.find("table", class_="wf-table mod-pr-global")

table_rows = table.find_all("tr")

# for th in table_rows[0]:
#     has_img = th.find("img")
#     if has_img:
#         print(has_img)

agents_img = []

for th in table_rows[0]:
    has_img = th.find("img")
    if has_img and has_img != -1:
        agents_img.append(has_img.get("src"))

agents_name = []
pattern = r'/(\w+)\.png'

for file_name in agents_img:
    match = re.search(pattern, file_name)
    if match:
        agents_name.append(match.group(1))


# agents_img = [th.find("img").get("src") for th in table_rows[0] if th.find("img") and th.find("img") != -1]

# agents_name = [re.search(pattern, file_name).group(1) for file_name in agents_img if re.search(pattern, file_name)]



print(agents_name)

# print(table_rows[0])
