import pandas as pd
import re

overview_stats_titles = ["", "", "Rating", "Average Combat Score", "Kills", "Deaths", "Assists", "Kills - Deaths (KD)",
                        "Kill, Assist, Trade, Survive %", "Average Damage per Round", "Headshot %", "First Kills",
                        "First Deaths", "Kills - Deaths (FKD)"]

non_latin_pattern = re.compile(r'[^a-zA-Z]')

def extract_maps_id(maps_id_divs, dict):
    for div in maps_id_divs:
        if div.get("data-game-id") and div.get("data-disabled") == "0":
            id = div.get("data-game-id")
            map = re.sub(r"\d+|\t|\n", "", div.text.strip())
            dict[id] = map

def extract_maps_notes(maps_notes, result, team_mapping, list):
    tournament_name = list[0]
    stage_name = list[1]
    match_type_name = list[2]
    match_name = list[3]

    try:
        if ";" in maps_notes[-1].text:
            maps_notes = maps_notes[-1].text.strip().split("; ")
            for note in maps_notes:
                if "ban" in note or "pick" in note:
                    team, action, map = note.split()
                    team = team_mapping[team]
                    result["draft_phase"].append([tournament_name, stage_name, match_type_name, match_name, team, action, map])
                
        else:
            print(f"For {tournament_name}, {stage_name}, {match_type_name}, {match_name}, its notes regarding the draft phase is empty")
    except IndexError:
        print(f"For {tournament_name}, {stage_name}, {match_type_name}, {match_name}, its notes regarding the draft phase is empty")

def extract_maps_headers(maps_headers, result, team_a, team_b, list):
    tournament_name = list[0]
    stage_name = list[1]
    match_type_name = list[2]
    match_name = list[3]
    for header in maps_headers:
        left_team_header, map_header, right_team_header = header.find_all(recursive=False)
        lt_score = left_team_header.find("div", class_="score").text.strip()
        lt_rounds_scores = left_team_header.find_all("span")
        map_info = map_header.text.strip().split()
        rt_score = right_team_header.find("div", class_="score").text.strip()
        rt_rounds_scores = right_team_header.find_all("span")
        map = map_info[0]

        lt_attacker_score, lt_defender_score = lt_rounds_scores[0].text.strip(), lt_rounds_scores[1].text.strip()
        rt_attacker_score, rt_defender_score = rt_rounds_scores[1].text.strip(), rt_rounds_scores[0].text.strip()
        try:
            lt_overtime_score = lt_rounds_scores[2].text.strip()
        except IndexError:
            lt_overtime_score = pd.NA
        try:
            rt_overtime_score = rt_rounds_scores[2].text.strip()
        except IndexError:
            rt_overtime_score = pd.NA
        try:
            duration = map_info[2]
        except IndexError:
            duration = pd.NA                


        result["maps_scores"].append([tournament_name, stage_name, match_type_name, match_name,
                                        map, team_a, lt_score, lt_attacker_score,
                                        lt_defender_score, lt_overtime_score,team_b,
                                        rt_score, rt_attacker_score, rt_defender_score,
                                        rt_overtime_score, duration])
        
def extract_overview_stats(overview_stats, maps_id, player_to_team, team_mapping, missing_team, result, list):
    tournament_name = list[0]
    stage_name = list[1]
    match_type_name = list[2]
    match_name = list[3]
    team_a = list[4]
    team_b = list[5]
    overview_dict = {}
    for index, stats in enumerate(overview_stats):
        id = stats.get("data-game-id")
        map = maps_id[id]
        map_dict = overview_dict.setdefault(map, {})
        stats_tables = stats.find_all("table")
        for table in stats_tables:
            trs = table.find("tbody").find_all("tr")
            for tr in trs:
                tds = tr.find_all("td")
                for index, td in enumerate(tds):
                    td_class = td.get("class") or ""
                    class_name = " ".join(td_class)
                    if class_name == "mod-player":
                        result = td.find("a").find_all("div")
                        player = result[0].text.strip()
                        try:
                            team = result[1].text.strip()
                        except IndexError:
                            if not bool(non_latin_pattern.search(team_a)):
                                team = team_a
                                missing_team = team
                            elif not bool(non_latin_pattern.search(team_b)):
                                team = team_b
                                missing_team = team
                        # player, team = td.find("a").find_all("div")
                        # player, team =  player.text.strip(), team.text.strip()
                        team = team_mapping[team]
                        player_to_team[player] = team
                        team_dict = map_dict.setdefault(team, {})
                        player_dict = team_dict.setdefault(player, {})
                    elif class_name == "mod-agents":
                        imgs = td.find_all("img")
                        agents_played = []
                        for img in imgs:
                            agent = img.get("alt")
                            agents_played.append(agent)
                        agents = ", ".join(agents_played)
                        player_dict["agents"] = agents
                    elif class_name in ["mod-stat mod-vlr-kills", "mod-stat", "mod-stat mod-vlr-assists", "mod-stat mod-kd-diff",
                                        "mod-stat mod-fb", "mod-stat mod-fd", "mod-stat mod-fk-diff"]:
                        stats = td.find("span").find_all("span")
                        if len(stats) == 3:
                            all_stat, attack_stat, defend_stat = stats
                            all_stat, attack_stat, defend_stat = all_stat.text.strip(), attack_stat.text.strip(), defend_stat.text.strip()
                            stat_name = overview_stats_titles[index % len(overview_stats_titles)]
                            if not all_stat and not attack_stat and not defend_stat:
                                all_stat, attack_stat, defend_stat = pd.NA, pd.NA, pd.NA
                            player_dict[stat_name] = {"both": all_stat, "attack": attack_stat, "defend": defend_stat}
                        else:
                            all_stat = stats[0]
                            all_stat = all_stat.text.strip()
                            stat_name = overview_stats_titles[index % len(overview_stats_titles)]
                            player_dict[stat_name] = {"both": all_stat, "attack": pd.NA, "defend": pd.NA}
                    elif class_name == "mod-stat mod-vlr-deaths":
                        stats = td.find("span").find_all("span")[1].find_all("span")
                        if len(stats) == 3:
                            all_stat, attack_stat, defend_stat = td.find("span").find_all("span")[1].find_all("span")
                            all_stat, attack_stat, defend_stat = all_stat.text.strip(), attack_stat.text.strip(), defend_stat.text.strip()
                            stat_name = overview_stats_titles[index % len(overview_stats_titles)]
                            player_dict[stat_name] = {"both": all_stat, "attack": attack_stat, "defend": defend_stat}
                        else:
                            all_stat = stats[0]
                            all_stat = all_stat.text.strip()
                            stat_name = overview_stats_titles[index % len(overview_stats_titles)]
                            player_dict[stat_name] = {"both": all_stat, "attack": pd.NA, "defend": pd.NA}
    sides = ["both", "attack", "defend"]
    for map_name, team in overview_dict.items():
        for team_name, player in team.items():
            for player_name, data in player.items():
                    agents = data["agents"]
                    rating = data["Rating"]
                    acs = data["Average Combat Score"]
                    kills = data["Kills"]
                    deaths = data["Deaths"]
                    assists = data["Assists"]
                    kills_deaths_fd = data["Kills - Deaths (KD)"]
                    kats = data["Kill, Assist, Trade, Survive %"]
                    adr = data["Average Damage per Round"]
                    headshot = data["Headshot %"]
                    first_kills = data["First Kills"]
                    first_deaths = data["First Deaths"]
                    kills_deaths_fkd = data["Kills - Deaths (FKD)"]
                    for side in sides:
                        result["overview"].append([tournament_name, stage_name, match_type_name, match_name, map_name, player_name, team_name, agents, rating[side],
                                                acs[side], kills[side], deaths[side], assists[side], kills_deaths_fd[side],
                                                kats[side], adr[side], headshot[side], first_kills[side], first_deaths[side],
                                                kills_deaths_fkd[side], side])