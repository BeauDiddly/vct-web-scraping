import requests
from bs4 import BeautifulSoup, Tag
import re
import time

urls = {"all_stages": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo",
        "semi_finals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16339.16332.16333.16334.16335.16336.16337",
        "grand_finals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16332.16333.16334.16335.16336.16337",
        "playoffs": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16332.16333.16334.16335.16336.16337",
        "alpha_round_of_sixteen": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16333.16334.16335.16336.16337",
        "alpha_quarterfinals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16334.16335.16336.16337",
        "alpha_semifinals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16335.16336.16337",
        "alpha": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16335.16336.16337",
        "omega_round_of_sixteen": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334.16336.16337",
        "omega_quarterfinals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334.16335.16337",
        "omega_semifinals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334.16335.16336",
        "omega": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334",
        "bracket_stage": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339"}
# page = requests.get(url)

# soup = BeautifulSoup(page.content, "html.parser")

# maps_stats_rows = soup.find("table", class_="wf-table mod-pr-global").select('td.mod-right, td:not([class]), td[style*="white-space: nowrap; padding-top: 0; padding-bottom: 0;"]')

# max_rows = len(soup.find("table", class_="wf-table mod-pr-global").find_all("tr", class_="pr-global-row"))

# # print(max_rows)

# for th in maps_stats_rows:
#     has_span = th.find("span")
#     if has_span:
#         has_span.extract()


# maps_stats_rows[0].string = "All Maps"

# steps = 4

# maps_stats = []

# for i in range(0, len(maps_stats_rows), steps):
#     start = i
#     end = i + steps
#     sub_list = maps_stats_rows[start: end]
#     maps_stats.append(sub_list)

# for i in maps_stats:
#     print(i.text.strip())

# print(maps_stats)

maps_stats_dict = {}

for stage, url in urls.items():
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    maps_stats_rows = soup.find("table", class_="wf-table mod-pr-global").select('td.mod-right, td:not([class]), td[style*="white-space: nowrap; padding-top: 0; padding-bottom: 0;"]')

    max_rows = len(soup.find("table", class_="wf-table mod-pr-global").find_all("tr", class_="pr-global-row"))

    for th in maps_stats_rows:
        has_span = th.find("span")
        if has_span:
            has_span.extract()


    maps_stats_rows[0].string = "All Maps"

    steps = 4

    maps_stats = []

    for i in range(0, len(maps_stats_rows), steps):
        start = i
        end = i + steps
        sub_list = maps_stats_rows[start: end]
        maps_stats.append(sub_list)

    maps_stats_dict[stage] = {}

    for i in range(0, max_rows):
        map_name = maps_stats[i][0].text.strip()
        map_counts = maps_stats[i][1].text.strip()
        atk_win_percentage = maps_stats[i][2].text.strip()
        def_win_percentage = maps_stats[i][3].text.strip()
        maps_stats_dict[stage][map_name] = {"map_counts": map_counts, "atk_win": atk_win_percentage, "def_win": def_win_percentage}
    # print(maps_stats_dict)
    time.sleep(1.5)
# index = 0

# for i in range(0, max_rows):
#     map_name = maps_stats[i][0].text.strip()
#     map_counts = maps_stats[i][1].text.strip()
#     atk_win_percentage = maps_stats[i][2].text.strip()
#     def_win_percentage = maps_stats[i][3].text.strip()
#     maps_stats_dict[map_name] = {"map_counts": map_counts, "atk_win": atk_win_percentage, "def_win": def_win_percentage}

# print(maps_stats_dict)

    