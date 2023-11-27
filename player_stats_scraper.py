import requests
from bs4 import BeautifulSoup, Tag, NavigableString
import re
import time
import csv


pattern = r'/(\w+)\.png'




all_agents = ["astra", "breach", "brimstone", "chamber", "cypher", "deadlock", "fade", "gekko", "harbor", "iso", "jett", "kayo",
              "killjoy", "neon", "omen", "phoenix", "raze", "reyna", "sage", "skye", "sova", "viper", "yoru", "all"]


stats_titles = []


url = "https://www.vlr.gg/vct-2023"
page = requests.get(url)
soup = BeautifulSoup(page.content, "html.parser")

urls = {}

filtered_urls = {}

players_stats = {}

global_players_agents = {}

stats_titles = ["", "", "Rounds Played", "Rating", "Average Combat Score", "Kills:Deaths", "Kill, Assist, Trade, Survive %",
                "Average Damage per Round", "Kills Per Round", "Assists Per Round", "First Kills Per Round", "First Deaths Per Round", 
                "Headshot %", "Clutch Success %", "Clutches (won/played)", "Maximum Kills in a Single Map", "Kills", "Deaths", "Assists",
                "First Kills", "First Deaths"]
print(len(stats_titles))
tournament_cards = soup.find_all("a", class_="wf-card mod-flex event-item")

for card in tournament_cards:
    href = card.get("href")
    stats_url = "https://www.vlr.gg" + href.replace("/event/", "/event/stats/")
    tournament = card.find("div", class_="event-item-title").text.strip().split(": ")
    if len(tournament) == 2:
        tournament_name = tournament[1]
    else:
        tournament_name = tournament[0]
    if tournament_name == "LOCK//IN São Paulo":
        tournament_name = "Lock-In Sao Paulo"

    urls[tournament_name] = stats_url

for tournament, url in urls.items():
    page = requests.get(url)

    soup = BeautifulSoup(page.content, "html.parser")

    all_stages = soup.find("div", class_="wf-card mod-dark mod-scroll stats-filter").find("div").find_all("div", recursive=False)
    tournament_dict = filtered_urls.setdefault(tournament, {})
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
    tournament_dict["All"]["All"] = f"{url}"

for tournament_name, stages in filtered_urls.items():
    tournament_dict = players_stats.setdefault(tournament_name, {})
    for stage_name, match_types in stages.items():
        stage_dict = tournament_dict.setdefault(stage_name, {})
        for match_type_name, url in match_types.items():
            match_type_dict = stage_dict.setdefault(match_type_name, {})
            players_agents = {}
            for agent in all_agents:
                page = requests.get(f"{url}&min_rounds=0&agent={agent}")

                soup = BeautifulSoup(page.content, "html.parser")
                stats_trs = soup.find_all("tr")[1:]

                if len(stats_trs) == 1:
                    continue

                for tr in stats_trs:
                    all_tds = tr.find_all("td")
                    filtered_tds = [td for td in all_tds if isinstance(td, Tag)]
                    for index, td in enumerate(filtered_tds):
                        td_class = td.get("class") or ""
                        class_name = " ".join(td_class)
                        if class_name == "mod-player mod-a":
                            player_info = td.find("div").find_all("div")
                            player, team = player_info[0].text, player_info[1].text
                            team_dict = match_type_dict.setdefault(team, {})
                            player_dict = team_dict.setdefault(player, {})
                        elif class_name == "mod-agents":
                            imgs = td.find("div").find_all("img")
                            agents = ""
                            player_agents_set = players_agents.setdefault(player, set())
                            global_players_agents_set = global_players_agents.setdefault(player, set())
                            if agent == "all" and len(players_agents[player]) > 1:
                                agents = ", ".join(players_agents[player]).strip(", ")
                            elif agent == "all" and len(players_agents[player]) == 1:
                                break
                            elif stage_name == "All" and match_type_name == "All" and agent == "all":
                                agents = ", ".join(global_players_agents[player]).strip(", ")
                            else:
                                for img in imgs:
                                    src = img.get("src")
                                    match = re.search(pattern, src)
                                    agent_name = match.group(1)
                                    player_agents_set.add(agent_name)
                                    agents += f"{agent}, "
                                agents = agents.strip(", ")
                            agents_dict = player_dict.setdefault(agents, {})
                        elif class_name == "mod-rnd" or class_name == "mod-cl" or class_name == "":
                            stat = td.text.strip()
                            stat_name = stats_titles[index]
                            if stat == "":
                                stat = "-1"
                            agents_dict[stat_name] = stat
                        elif class_name == "mod-color-sq mod-acs" or class_name ==  "mod-color-sq":
                            stat = td.find("div").find("span").text.strip()
                            stat_name = stats_titles[index]
                            if stat == "":
                                stat = "-1"
                            agents_dict[stat_name] = stat
                        elif class_name == "mod-a mod-kmax":
                            stat = td.find("a").text.strip()
                            stat_name = stats_titles[index]
                            if stat == "":
                                stat = "-1"
                            agents_dict[stat_name] = stat
                    global_players_agents[player] = global_players_agents[player] | players_agents[player]
            break
        break
    break


with open("players_stats.csv", "w", newline="") as players_stats_file:
    players_stats_writer = csv.writer(players_stats_file)
    players_stats_writer.writerow(["Tournament", "Stage", "Match Type", "Player", "Team", "Agents", "Rounds Played",
                            "Rating", "Average Combat Score", "Kills:Deaths", "Kill, Assist, Trade, Survive %",
                            "Average Damage per Round", "Kills Per Round", "Assists Per Round", "First Kills Per Round",
                            "First Deaths Per Round", "Headshot %", "Clutch Success %", "Clutches (won/played)",
                            "Maximum Kills in a Single Map", "Kills", "Deaths", "Assists", "First Kills", "First Deaths"])
    for tournament_name, stages in players_stats.items():
        for stage_name, match_types in stages.items():
            for match_type, teams in match_types.items():
                for team_name, players in teams.items():
                    for player_name, agents in players.items():
                        for agents_played, stats in agents.items():
                            players_stats_writer.writerow([tournament_name, stage_name, match_type, player_name, team_name, agents_played] + list(stats.values()))



                    
