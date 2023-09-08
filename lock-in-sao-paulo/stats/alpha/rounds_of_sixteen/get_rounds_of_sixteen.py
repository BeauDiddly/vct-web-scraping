import requests
from bs4 import BeautifulSoup, Tag
import re

url = f"https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16333.16334.16335.16336.16337"
page = requests.get(url)

soup = BeautifulSoup(page.content, "html.parser")

map_stats = soup.find("table", class_="wf-table mod-pr-global").select('td.mod-right, td:not([class]), td[style*="white-space: nowrap; padding-top: 0; padding-bottom: 0;"]')

agent_pictures = soup.find("table", class_="wf-table mod-pr-global").find_all("th", style=" vertical-align: middle; padding-top: 0; padding-bottom: 0; width: 65px;")

agents_names = []
pattern = r'/(\w+)\.png'
for th in agent_pictures:
    file_name = th.find("img").get("src")
    match = re.search(pattern, file_name)
    agent_name = match.group(1)
    agents_names.append(agent_name)

# print(agents_names)
    


# print(agent_pictures)

# table_rows = table.find_all("tr")

# for th in table_rows[0]:
#     has_img = th.find("img")
#     if has_img:
#         print(has_img)

# agents_img = []

# for th in table_rows[0]:
#     has_img = th.find("img")
#     if has_img and has_img != -1:
#         agents_img.append(has_img.get("src"))

# agents_name = []
# pattern = r'/(\w+)\.png'

# for file_name in agents_img:
#     match = re.search(pattern, file_name)
#     if match:
#         agents_name.append(match.group(1))
# map_stats = {}

# print(agents_name)

# print(soup.select('td.mod-right, td:not([class]), td[style*="white-space: nowrap; padding-top: 0; padding-bottom: 0;"]'))

# for tr in table_rows[1:]:
#         map_name = tr.find("td", style="white-space: nowrap; padding-top: 0; padding-bottom: 0;").text
#         stats = tr.find_all("td", class_=["mod-right"]).text
#         if map_name == "":
#              map_name = "all_maps"
        
        # if isinstance(td, Tag) and not td.find_all():
        #     stats.append(td.text)
    #     if isinstance(data, Tag) and not data.find_all():
    #         stats.append(data.getText())
    # map_stats.append(stats)

# print(map_stats)
    # print(f'{index}: {tr}')

# print(table_rows[1])

# agents_img = [th.find("img").get("src") for th in table_rows[0] if th.find("img") and th.find("img") != -1]

# agents_name = [re.search(pattern, file_name).group(1) for file_name in agents_img if re.search(pattern, file_name)]



# print(agents_name)

# print(table_rows[0])
