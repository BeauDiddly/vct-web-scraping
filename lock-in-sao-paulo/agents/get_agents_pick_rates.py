import requests
from bs4 import BeautifulSoup, Tag, NavigableString
import re
import time
import csv
# url = f"https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16334.16335.16336.16337"

# urls = {"all_stages": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo",
#         "semi_finals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16339.16332.16333.16334.16335.16336.16337",
#         "grand_finals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16332.16333.16334.16335.16336.16337",
#         "playoffs": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16332.16333.16334.16335.16336.16337",
#         "alpha_round_of_sixteen": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16333.16334.16335.16336.16337",
#         "alpha_quarterfinals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16334.16335.16336.16337",
#         "alpha_semifinals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16335.16336.16337",
#         "alpha": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16335.16336.16337",
#         "omega_round_of_sixteen": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334.16336.16337",
#         "omega_quarterfinals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334.16335.16337",
#         "omega_semifinals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334.16335.16336",
#         "omega": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334",
#         "bracket_stage": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339"}

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
    all_ids = ""
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
            all_ids += f"{id}."
    all_ids = all_ids.strip(".").split(".")
    for stage_name, match_types in tournament_dict.items():
        for match_type, id in match_types.items():
            excluded_ids = ".".join(exclude_id for exclude_id in all_ids if exclude_id != id)
            filter_url = f"{url}?exclude={excluded_ids}"
            tournament_dict[stage_name][match_type] = filter_url
    tournament_dict["All"] = {}
    tournament_dict["All"]["All"] = f"{url}?exclude"
    
    break
pattern = r'/(\w+)\.png'
pick_rates = {}
global_table_titles = ["Map", "Total Played", "Attacker Side Win", "Defender Side Win"]

for tournament_name, stages in stages_filter.items():
    tournament_dict = pick_rates.setdefault(tournament_name, {})
    for stage_name, match_types in stages.items():
        stage_dict = tournament_dict.setdefault(stage_name, {})
        for match_type_name, url in match_types.items():
            match_type_dict = stage_dict.setdefault(match_type_name, {})
            maps_stats_dict = match_type_dict.setdefault("Maps Stats", {})
            agents_pick_rates_dict = match_type_dict.setdefault("Agents Pick Rates", {})
            teams_pick_rates_dict = match_type_dict.setdefault("Teams Pick Rates", {})

            page = requests.get(url)
            soup = BeautifulSoup(page.content, "html.parser")
            global_maps_table = soup.find("table", class_="wf-table mod-pr-global")
            agent_pictures = global_maps_table.find_all("th", style=" vertical-align: middle; padding-top: 0; padding-bottom: 0; width: 65px;")



            for th in agent_pictures:
                src = th.find("img").get("src")
                match = re.search(pattern, src)
                agent = match.group(1)
                global_table_titles.append(agent)
            
            table_stats_tr = global_maps_table.find_all("tr")[1:]

            for tr in table_stats_tr:
                all_tds = tr.find_all("td")
                filtered_tds = [td for td in all_tds if isinstance(td, Tag)]
                for index, td in enumerate(filtered_tds):
                    td_class = td.get("class") or ""
                    class_name = " ".join(td_class)
                    if not class_name:
                        map = td.text.strip().replace("\t", "")
                        if not map:
                            map = "All"
                        else:
                            logo, map = map.split("\n")
                        map_stats_dict = maps_stats_dict.setdefault(map, {})
                        agent_pick_rate_dict = agents_pick_rates_dict.setdefault(map, {})
                    elif class_name == "mod-right":
                        stat = td.text.strip()
                        title = global_table_titles[index]
                        map_stats_dict[title] = stat
                    elif class_name == "mod-center":
                        stat = td.text.strip()
                        agent = global_table_titles[index]
                        agent_pick_rate_dict[agent] = stat
            
            teams_tables = soup.select('table.wf-table:not([class*=" "])')
            table_titles = ["", ""] + global_table_titles[4:]

            for table in teams_tables:
                all_tr = table.find_all("tr")
                logo, map = table.find("tr").find("th").text.replace("\t", "").split()
                map_dict = teams_pick_rates_dict.setdefault(map, {})
                teams_pick_rate_tr = table.find_all("tr")[1:]
                for tr in teams_pick_rate_tr:
                    all_tds = tr.find_all("td")
                    filtered_tds = [td for td in all_tds if isinstance(td, Tag)]
                    contained_any_agents = any(td.has_attr('class') and ('mod-picked' in td['class']) for td in filtered_tds)
                    if contained_any_agents:
                        for index, td in enumerate(filtered_tds):
                            td_class = td.get("class") or ""
                            a_tag = td.find("a")
                            if a_tag and a_tag.find_all("img"):
                                team = a_tag.text.strip()
                                print(team)
                                team_dict = map_dict.setdefault(team, set())
                            elif len(td_class) == 1:
                                agent = table_titles[index]
                                print(agent)
                                team_dict.add(agent)
                break
            break
        print(teams_pick_rates_dict)
        break
    break


with open("maps_stats.csv", "w", newline="") as maps_stats_file, open("agents_pick_rates.csv", "w", newline="") as agents_pick_rates_file, \
     open("teams_pick_rates.csv", "w", newline="") as teams_pick_rates_file:
    maps_stats_writer = csv.writer(maps_stats_file)
    agents_pick_rates_writer = csv.writer(agents_pick_rates_file)
    teams_pick_rates_writer = csv.writer(teams_pick_rates_file)
    maps_stats_writer.writerow(["Tournament", "Stage", "Match Type", "Map", "Total Played", "Attacker Side Win Percentage", "Defender Side Win Percentage"])
    agents_pick_rates_writer.writerow(["Tournament", "Stage", "Match Type", "Map", "Agent", "Pick Rate"])
    teams_pick_rates_writer.writerow(["Tournament", "Stage", "Match Type", "Map", "Team", "Agent"])
    

    for tournament_name, stages in pick_rates.items():
        for stage_name, match_types in stages.items():
            for match_type, title in match_types.items():
                maps_stats = title["Maps Stats"]
                agents_pick_rates = title["Agents Pick Rates"]
                teams_pick_rates = title["Teams Pick Rates"]

                for map_name, stats in maps_stats.items():
                    maps_stats_writer.writerow([tournament_name, stage_name, match_type, map] + list(stats.values()))
                
                for map_name, agents in agents_pick_rates.items():
                    for agent_name, pick_rate in agents.items():
                        agents_pick_rates_writer.writerow([tournament, stage_name, match_type, map, agent_name, pick_rate])
                
                for map_name, teams in teams_pick_rates.items():
                    for team_name, agents in teams.items():
                        for agent in agents:
                            teams_pick_rates_writer.writerow([tournament, stage_name, match_type, map, team_name, agent])


                        


                # for index, td in enumerate(teams_pick_rate_tr):
                #     print(index, isinstance(td))
                # break

                





# print(pick_rates["maps_stats"])
# print(pick_rates["agents_pick_rates"])



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