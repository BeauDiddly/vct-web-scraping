import requests
from bs4 import BeautifulSoup, Tag
import re
import time
# url = f"https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16334.16335.16336.16337"

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

# url = "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo"
# page = requests.get(url)

# soup = BeautifulSoup(page.content, "html.parser")

# agent_pictures = soup.find("table", class_="wf-table mod-pr-global").find_all("th", style=" vertical-align: middle; padding-top: 0; padding-bottom: 0; width: 65px;")

# maps = soup.find("table", class_="wf-table mod-pr-global").find_all("td", style="white-space: nowrap; padding-top: 0; padding-bottom: 0;")

# pick_rates = soup.find("table", class_="wf-table mod-pr-global").find_all("div", class_="color-sq")

# max_rows = len(soup.find("table", class_="wf-table mod-pr-global").find_all("tr", class_="pr-global-row"))



# for th in maps:
#     has_span = th.find("span")
#     if has_span:
#         has_span.extract()


# maps[0].string = "All Maps"

# print(maps)

# maps_name = []

# for name in maps:
#     maps_name.append(name.text.strip())

# print(maps_name)
 

# agents_names = []
# pattern = r'/(\w+)\.png'
# for th in agent_pictures:
#     file_name = th.find("img").get("src")
#     match = re.search(pattern, file_name)
#     agent_name = match.group(1)
#     agents_names.append(agent_name)

# print(agents_names)

# for i in range(len(pick_rates)):
#     pick_rates[i] = pick_rates[i].text.strip()

# steps = len(agents_names)

# agents_pick_rates = []

# for i in range(0, len(pick_rates), steps):
#     start = i
#     end = i + steps
#     sub_list = pick_rates[start: end]
#     agents_pick_rates.append(sub_list)
# for pick_rate in pick_rates:
#     agents_pick_rates.append(pick_rate.text.strip())

# print(agents_pick_rates)

# print(agents_pick_rates)

pattern = r'/(\w+)\.png'
agents_pick_rates_with_maps = {}

for stage, url in urls.items():
    print(stage, url)
    page = requests.get(url)

    soup = BeautifulSoup(page.content, "html.parser")

    agent_pictures = soup.find("table", class_="wf-table mod-pr-global").find_all("th", style=" vertical-align: middle; padding-top: 0; padding-bottom: 0; width: 65px;")

    maps = soup.find("table", class_="wf-table mod-pr-global").find_all("td", style="white-space: nowrap; padding-top: 0; padding-bottom: 0;")

    pick_rates = soup.find("table", class_="wf-table mod-pr-global").find_all("div", class_="color-sq")

    max_rows = len(soup.find("table", class_="wf-table mod-pr-global").find_all("tr", class_="pr-global-row"))

    for th in maps:
        has_span = th.find("span")
        if has_span:
            has_span.extract()


    agents_names = []
    pattern = r'/(\w+)\.png'
    for th in agent_pictures:
        file_name = th.find("img").get("src")
        match = re.search(pattern, file_name)
        agent_name = match.group(1)
        agents_names.append(agent_name)

    maps[0].string = "All Maps"

    # print(maps)

    maps_name = []

    for name in maps:
        maps_name.append(name.text.strip())
    # print(maps_name)

    for i in range(len(pick_rates)):
        pick_rates[i] = pick_rates[i].text.strip()

    steps = len(agents_names)

    agents_pick_rates = []

    for i in range(0, len(pick_rates), steps):
        start = i
        end = i + steps
        sub_list = pick_rates[start: end]
        agents_pick_rates.append(sub_list)

    agents_pick_rates_with_maps[stage] = {}

    for i in range(0, max_rows):
        map_name = maps_name[i]
        agents_pick_rates_with_maps[stage][map_name] = {}
        for index, name in enumerate(agents_names):
            pick_rate = agents_pick_rates[i][index] 
            agents_pick_rates_with_maps[stage][map_name][name] = pick_rate

# for i in range(0, max_rows):
#     map_name = maps_name[i]
#     agents_pick_rates_with_maps[map_name] = {}
#     for index, name in enumerate(agents_names):
#         pick_rate = agents_pick_rates[i][index] 
#         agents_pick_rates_with_maps[map_name][name] = pick_rate
    # map_name = maps_stats[index].text.strip()
    # map_counts = maps_stats[index + 1].text.strip()
    # atk_win_percentage = maps_stats[index + 2].text.strip()
    # def_win_percentage = maps_stats[index + 3].text.strip()
    # maps_stats_dict[map_name] = {"map_counts": map_counts, "atk_win": atk_win_percentage, "def_win": def_win_percentage}
    # index += 4

# print(agents_pick_rates_with_maps)