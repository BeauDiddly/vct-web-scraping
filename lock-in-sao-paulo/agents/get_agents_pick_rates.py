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

url = "https://www.vlr.gg/vct-2023"
page = requests.get(url)
soup = BeautifulSoup(page.content, "html.parser")

urls = {}

tournament_cards = soup.find_all("a", class_="wf-card mod-flex event-item")

for card in tournament_cards:
    href = card.get("href")
    agent_url = "https://www.vlr.gg" + href.replace("/event/", "/event/agents/")
    tournament = card.find("div", class_="event-item-title").text.strip().split(": ")
    if len(tournament) == 2:
        tournament_name = tournament[1]
    else:
        tournament_name = tournament[0]
    if tournament_name == "LOCK//IN São Paulo":
        tournament_name = "Lock-In Sao Paulo"

    urls[tournament_name] = agent_url

stages_filter = {}

for tournament, url in urls.items():
    page = requests.get(url)

    soup = BeautifulSoup(page.content, "html.parser")

    all_stages = soup.find("div", class_="wf-card mod-dark mod-scroll stats-filter").find("div").find_all("div", recursive=False)
    tournament_dict = stages_filter.setdefault(tournament, {})
    tournament_dict["All"] = ""
    for stage in all_stages:
        # print(stage.find_all("div", recursive=False))
        stage_name_div, match_types_div = stage.find_all("div", recursive=False)
        stage_name = stage_name_div.find("div").text.strip()
        match_types_div = match_types_div.find_all("div")
        stage_dict = tournament_dict.setdefault(stage_name, {})
        for match_type in match_types_div:
            match_type_name = match_type.text.strip()
            id = match_type.get("data-subseries-id")
            stage_dict[match_type_name] = id



    break

# for tournament, stages in stages_filter.items():
#     for stage_name, match_types in stages.items():

# agents_pages = {}
# agents_pages["All"] = {"url": }

# pattern = r'/(\w+)\.png'
# agents_pick_rates_with_maps = {}

# for stage, url in urls.items():
#     print(stage, url)
#     page = requests.get(url)

#     soup = BeautifulSoup(page.content, "html.parser")

#     agent_pictures = soup.find("table", class_="wf-table mod-pr-global").find_all("th", style=" vertical-align: middle; padding-top: 0; padding-bottom: 0; width: 65px;")

#     maps = soup.find("table", class_="wf-table mod-pr-global").find_all("td", style="white-space: nowrap; padding-top: 0; padding-bottom: 0;")

#     pick_rates = soup.find("table", class_="wf-table mod-pr-global").find_all("div", class_="color-sq")

#     max_rows = len(soup.find("table", class_="wf-table mod-pr-global").find_all("tr", class_="pr-global-row"))

#     for th in maps:
#         has_span = th.find("span")
#         if has_span:
#             has_span.extract()


#     agents_names = []
#     pattern = r'/(\w+)\.png'
#     for th in agent_pictures:
#         file_name = th.find("img").get("src")
#         match = re.search(pattern, file_name)
#         agent_name = match.group(1)
#         agents_names.append(agent_name)

#     maps[0].string = "All Maps"


#     maps_name = []

#     for name in maps:
#         maps_name.append(name.text.strip())

#     for i in range(len(pick_rates)):
#         pick_rates[i] = pick_rates[i].text.strip()

#     steps = len(agents_names)

#     agents_pick_rates = []

#     for i in range(0, len(pick_rates), steps):
#         start = i
#         end = i + steps
#         sub_list = pick_rates[start: end]
#         agents_pick_rates.append(sub_list)

#     agents_pick_rates_with_maps[stage] = {}

#     for i in range(0, max_rows):
#         map_name = maps_name[i]
#         agents_pick_rates_with_maps[stage][map_name] = {}
#         for index, name in enumerate(agents_names):
#             pick_rate = agents_pick_rates[i][index] 
#             agents_pick_rates_with_maps[stage][map_name][name] = pick_rate