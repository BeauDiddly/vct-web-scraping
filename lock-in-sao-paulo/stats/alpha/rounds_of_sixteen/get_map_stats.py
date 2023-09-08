import requests
from bs4 import BeautifulSoup, Tag
import re

url = f"https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16333.16334.16335.16336.16337"
page = requests.get(url)

soup = BeautifulSoup(page.content, "html.parser")

maps_stats = soup.find("table", class_="wf-table mod-pr-global").select('td.mod-right, td:not([class]), td[style*="white-space: nowrap; padding-top: 0; padding-bottom: 0;"]')

max_rows = len(soup.find("table", class_="wf-table mod-pr-global").find_all("tr", class_="pr-global-row"))

# print(max_rows)

for th in maps_stats:
    has_span = th.find("span")
    if has_span:
        has_span.extract()

if not maps_stats[0].string.get_text(strip=True):
    maps_stats[0].string = "all_maps"

for i in maps_stats:
    print(i.text.strip())

# print(maps_stats)

maps_stats_dict = {}

index = 0

for i in range(0, max_rows):
    map_name = maps_stats[index].text.strip()
    map_counts = maps_stats[index + 1].text.strip()
    atk_win_percentage = maps_stats[index + 2].text.strip()
    def_win_percentage = maps_stats[index + 3].text.strip()
    maps_stats_dict[map_name] = {"map_counts": map_counts, "atk_win": atk_win_percentage, "def_win": def_win_percentage}
    index += 4

print(maps_stats_dict)

    