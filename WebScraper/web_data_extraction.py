import pandas as pd
import re
import traceback
import Levenshtein
from bs4 import Tag, Comment, BeautifulSoup
import sys
overview_stats_titles = ["", "", "Rating", "Average Combat Score", "Kills", "Deaths", "Assists", "Kills - Deaths (KD)",
                        "Kill, Assist, Trade, Survive %", "Average Damage per Round", "Headshot %", "First Kills",
                        "First Deaths", "Kills - Deaths (FKD)"]

performance_stats_title = ["", "", "2k", "3k", "4k", "5k", "1v1", "1v2", "1v3", "1v4", "1v5", "Economy", "Spike Plants", "Spike Defuse"]
economy_stats_title = ["", "Pistol Won", "Eco (won)", "$ (won)", "$$ (won)", "$$$ (won)"]
overview, performance, economy = "Overview", "Performance", "Economy"
specific_kills_name = ["All Kills", "First Kills", "Op Kills"]
eco_types = {"": "Eco: 0-5k", "$": "Semi-eco: 5-10k", "$$": "Semi-buy: 10-20k", "$$$": "Full buy: 20k+"}
# all_agents = ["astra", "breach", "brimstone", "chamber", "cypher", "deadlock", "fade", "gekko", "harbor", "iso", "jett", "kayo",
#               "killjoy", "neon", "omen", "phoenix", "raze", "reyna", "sage", "skye", "sova", "viper", "yoru", "all"]
stats_titles = ["", "", "Rounds Played", "Rating", "Average Combat Score", "Kills:Deaths", "Kill, Assist, Trade, Survive %",
                "Average Damage per Round", "Kills Per Round", "Assists Per Round", "First Kills Per Round", "First Deaths Per Round", 
                "Headshot %", "Clutch Success %", "Clutches (won/played)", "Maximum Kills in a Single Map", "Kills", "Deaths", "Assists",
                "First Kills", "First Deaths"]
methods = {"elim": "Elimination", "boom": "Detonated", "defuse": "Defused", "time": "Time Expiry (No Plant)"}

cjk_pattern = re.compile(r'[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\uff66-\uff9f]')
pattern = r'/(\w+)\.png'

def extract_games_id(games_id_divs, games_id, results, list):
    tournament_name = list[0]
    stage_name = list[1]
    match_type_name = list[2]
    match_name = list[3]
    tournament_id = list[4]
    stage_id = list[5]
    match_id = list[6]
    for div in games_id_divs:
        if div.get("data-game-id") and div.get("data-disabled") == "0":
            id = div.get("data-game-id")
            map = re.sub(r"\d+|\t|\n", "", div.text.strip())
            if map != "N/A":
                games_id[id] = map
    for id, map in games_id.items():
        if map != "All Maps":
            results["maps_played"].append([tournament_name, stage_name, match_type_name, match_name, map])
            results["tournaments_stages_matches_games_ids"].append([tournament_name, tournament_id, stage_name, stage_id, match_type_name, match_name, match_id, map, id])

def extract_maps_notes(maps_notes, results, team_mapping, list):
    tournament_name = list[0]
    stage_name = list[1]
    match_type_name = list[2]
    match_name = list[3]
    try:
        if ";" in maps_notes[-1].text:
            maps_notes = maps_notes[-1].text.strip().split("; ")
            for note in maps_notes:
                if "ban" in note or "pick" in note:
                    result = note.split()
                    map = result.pop()
                    action = result.pop()
                    team = " ".join(result)
                    try:
                        team = team_mapping[team]
                    except KeyError as e:
                        # print(f"{tournament_name}, {stage_name}, {match_type_name}, {match_name} could not be consistent with the abbrievated name")
                        # print(f"The notes used the full team name {e} instead of the abbreviated team name")
                        team = min(team_mapping.values(), key=lambda x: Levenshtein.distance(team, x))
                    results["draft_phase"].append([tournament_name, stage_name, match_type_name, match_name, team, action, map])
                
        else:
            print(f"For {tournament_name}, {stage_name}, {match_type_name}, {match_name}, its notes regarding the draft phase is empty")
    except IndexError:
        print(f"For {tournament_name}, {stage_name}, {match_type_name}, {match_name}, its notes regarding the draft phase is empty")

def extract_maps_headers(maps_headers, results, list):
    tournament_name = list[0]
    stage_name = list[1]
    match_type_name = list[2]
    match_name = list[3]
    team_a = list[4]
    team_b = list[5]
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


        results["maps_scores"].append([tournament_name, stage_name, match_type_name, match_name,
                                        map, team_a, lt_score, lt_attacker_score,
                                        lt_defender_score, lt_overtime_score,team_b,
                                        rt_score, rt_attacker_score, rt_defender_score,
                                        rt_overtime_score, duration])
    

def extract_methods(overview_stats, games_id, results, list):
    tournament_name = list[0]
    stage_name = list[1]
    match_type_name = list[2]
    match_name = list[3]
    team_a = list[4]
    team_b = list[5]
    for info in overview_stats:

        id = info.get("data-game-id")
        try:
            map = games_id[id]
        except KeyError:
            continue
        if map == "All Maps":
            continue
        rounds = info.find_all("div", class_="vlr-rounds-row")
        outcomes = {team_a: {"elim": 0, "boom": 0, "defuse": 0, "time": 0, "eliminated": 0, "failed_to_defuse": 0, "failed_to_detonate": 0, "failed_to_plant": 0},
                    team_b: {"elim": 0, "boom": 0, "defuse": 0, "time": 0, "eliminated": 0, "failed_to_defuse": 0, "failed_to_detonate": 0, "failed_to_plant": 0}}
        for round in rounds:
            rows = round.find_all("div", class_="vlr-rounds-row-col")
            for row in rows:
                if not row.find_all("div", class_="team") and row.find("img"):
                    round_number, team_a_outcome, team_b_outcome = row.find_all("div")
                    round_number = round_number.text.strip()
                    if team_a_outcome.find("img"):
                        outcome = team_a_outcome.find("img").get("src").split("/")[-1].split(".")[0]
                        enemy_outcome = ""
                        outcomes[team_a][outcome] += 1

                        if outcome == "elim":
                            outcomes[team_b]["eliminated"] += 1
                            enemy_outcome = "Eliminated"
                        elif outcome == "boom":
                            outcomes[team_b]["failed_to_defuse"] += 1
                            enemy_outcome = "Failed Defused"
                        elif outcome == "defuse":
                            outcomes[team_b]["failed_to_detonate"] += 1
                            enemy_outcome = "Detonated Denied"
                        elif outcome == "time":
                            outcomes[team_b]["failed_to_plant"] += 1
                            enemy_outcome = "Time Expiry (Failed to Plant)"
                        results["win_loss_methods_round_number"].append([tournament_name, stage_name, match_type_name, match_name, map, round_number, team_a, methods[outcome], "Win"])
                        results["win_loss_methods_round_number"].append([tournament_name, stage_name, match_type_name, match_name, map, round_number, team_b, enemy_outcome, "Loss"])
                    # else:
                    #     results["win_methods_round_number"].append([tournament_name, stage_name, match_type_name, match_name, map, round_number, team_a, "Eliminated", "Loss"])
                        # outcomes[team_a]["eliminated"] += 1
                    elif team_b_outcome.find("img"):
                        outcome = team_b_outcome.find("img").get("src").split("/")[-1].split(".")[0]
                        enemy_outcome = ""
                        outcomes[team_b][outcome] += 1

                        if outcome == "elim":
                            outcomes[team_a]["eliminated"] += 1
                            enemy_outcome = "Eliminated"
                        elif outcome == "boom":
                            outcomes[team_a]["failed_to_defuse"] += 1
                            enemy_outcome = "Failed Defused"
                        elif outcome == "defuse":
                            outcomes[team_a]["failed_to_detonate"] += 1
                            enemy_outcome = "Detonated Denied"
                        elif outcome == "time":
                            outcomes[team_a]["failed_to_plant"] += 1
                            enemy_outcome = "Time Expiry (Failed to Plant)"
                        results["win_loss_methods_round_number"].append([tournament_name, stage_name, match_type_name, match_name, map, round_number, team_b, methods[outcome], "Win"])
                        results["win_loss_methods_round_number"].append([tournament_name, stage_name, match_type_name, match_name, map, round_number, team_a, enemy_outcome, "Loss"])

                    # else:
                    #     results["win_methods_round_number"].append([tournament_name, stage_name, match_type_name, match_name, map, round_number, team_b, "Eliminated", "Loss"])
                    #     outcomes[team_b]["eliminated"] += 1
        results["win_loss_methods_count"].append([tournament_name, stage_name, match_type_name, match_name, map,
                                             team_a, outcomes[team_a]["elim"], outcomes[team_a]["boom"],
                                             outcomes[team_a]["defuse"], outcomes[team_a]["time"], outcomes[team_a]["eliminated"],
                                             outcomes[team_a]["failed_to_defuse"], outcomes[team_a]["failed_to_detonate"],
                                             outcomes[team_a]["failed_to_plant"]])
        results["win_loss_methods_count"].append([tournament_name, stage_name, match_type_name, match_name, map,
                                             team_b, outcomes[team_b]["elim"], outcomes[team_b]["boom"],
                                             outcomes[team_b]["defuse"], outcomes[team_b]["time"], outcomes[team_b]["eliminated"],
                                             outcomes[team_b]["failed_to_defuse"], outcomes[team_b]["failed_to_detonate"],
                                             outcomes[team_b]["failed_to_plant"]])
                    


        
def extract_overview_stats(overview_stats, games_id, team_mapping, results, list):
    tournament_name = list[0]
    stage_name = list[1]
    match_type_name = list[2]
    match_name = list[3]
    team_a = list[4]
    team_b = list[5]
    overview_dict = {}
    player_to_team = {}
    for index, stats in enumerate(overview_stats):
        id = stats.get("data-game-id")
        try:
            map = games_id[id]
        except KeyError: #match is BO1
            continue
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
                        a_tag = td.find("a")
                        if a_tag:
                            player_id = (a_tag.get("href")).split("/")[2]
                        else:
                            player_id = pd.NA
                        try:
                            result = td.find("a").find_all("div")
                        except AttributeError: #This row did not contain the player and team name
                            break
                        player = result[0].text.strip()
                        team = result[1].text.strip()
                        try:
                            team = team_mapping[team]
                        except KeyError: #Either the team name does not contain latin characters or they did not used the abbrievated name
                            if not team:
                                if cjk_pattern.search(team_a):
                                    team = team_a
                                elif cjk_pattern.search(team_b):
                                    team = team_b
                                else:
                                    team = min(team_mapping.values(), key=lambda x: Levenshtein.distance(team, x))
                        results["players_ids"][player] = player_id
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
                        results["overview"].append([tournament_name, stage_name, match_type_name, match_name, map_name,
                                                    player_name, team_name, agents,
                                                    rating[side], acs[side], kills[side], deaths[side], assists[side],
                                                    kills_deaths_fd[side], kats[side], adr[side], headshot[side],
                                                    first_kills[side], first_deaths[side], kills_deaths_fkd[side], side])
    return player_to_team

def extract_kills_stats(performance_stats_div, games_id, team_mapping, player_to_team, results, list):
        tournament_name = list[0]
        stage_name = list[1]
        match_type_name = list[2]
        match_name = list[3]
        team_a = list[4]
        team_b = list[5]
        try:
            team_b_div = performance_stats_div[0].find("div").find("tr").find_all("div", class_="team")
            team_b_players = [""]
            for player in team_b_div:
                try:
                    player, team = player.text.strip().replace("\t", "").split("\n")
                except ValueError: #The player name was not on the row but I set it to NULL so I can have a "5x5 table" when I extract the kill data
                    player = pd.NA
                team_b_players.append(player)
            players_to_players_kills = {}
            players_kills = {}

            for div in performance_stats_div:
                kills_table = div.find("table", "wf-table-inset mod-adv-stats")
                if kills_table != None:
                    id = div.get("data-game-id")
                    players_to_players_kills[id] = []
                    players_kills[id] = []
                    players_to_players_kills_tables = div.find("div").find_all("table")
                    kills_trs = kills_table.find_all("tr")[1:]
                    for table in players_to_players_kills_tables:
                        trs = table.find_all("tr")[1:]
                        for tr in trs:
                            tds = tr.find_all("td")
                            players_to_players_kills[id].append(tds)
                    for tr in kills_trs:
                        tds = tr.find_all("td")
                        players_kills[id].append(tds)
                else:
                    continue
            

            for id, tds_lists in players_to_players_kills.items():
                try:
                    map = games_id[id]
                except KeyError: #match is BO1
                    continue
                for index, td_list in enumerate(tds_lists):
                    for team_b_player_index, td in enumerate(td_list):
                        if td.find("img") != None:
                            result = td.text.strip().replace("\t", "").split("\n")
                            team = result.pop()
                            try:
                                team = team_mapping[team]
                            except KeyError: #The team name can't be map because it is empty or they did not use the abbrievated name
                                if not team:
                                    if cjk_pattern.search(team_a):
                                        team = team_a
                                    elif cjk_pattern.search(team_b):
                                        team = team_b
                                else:
                                    team = min(team_mapping.values(), key=lambda x: Levenshtein.distance(team, x))
                            try:
                                player = result.pop()
                            except IndexError: #The player name is missing
                                player = pd.NA
                            kill_name = specific_kills_name[index // (len(team_b_players) - 1)]
                        else:
                            kills_div = td.find("div").find_all("div")
                            player_a_kills, player_b_kills, difference = kills_div[0].text.strip(), kills_div[1].text.strip(), kills_div[2].text.strip()
                            player_b = team_b_players[team_b_player_index]
                            if not player_a_kills and not player_b_kills and not difference:
                                player_a_kills, player_b_kills, difference = pd.NA, pd.NA, pd.NA
                            results["kills"].append([tournament_name, stage_name, match_type_name, match_name, map, team, player,
                                                        team_b, player_b, player_a_kills, player_b_kills, difference,
                                                        kill_name])
            

            for id, tds_lists in players_kills.items():
                try:
                    map = games_id[id]
                except KeyError: #The map name is "N/A" or it is BO1
                    continue
                for tds in tds_lists:
                    values = [tournament_name, stage_name, match_type_name, match_name, map]
                    for index, td in enumerate(tds):
                        img = td.find("img")
                        if img != None:
                            class_name = " ".join(td.find("div").get("class"))
                            if class_name == "team":
                                result = td.text.strip().replace("\t", "").split("\n")
                                team = result.pop()
                                try:
                                    team = team_mapping[team]
                                except KeyError: #The team name was missing or they did not use the abbrievated name
                                    if not team:
                                        if cjk_pattern.search(team_a):
                                            team = team_a
                                        elif cjk_pattern.search(team_b):
                                            team = team_b
                                    else:
                                        team = min(team_mapping.keys(), key=lambda x: Levenshtein.distance(team, x))
                                try:
                                    player = result.pop()
                                except IndexError: #The player was missing in this row.
                                    player = pd.NA
                                values.append(team)
                                values.append(player)
                            elif class_name == "stats-sq":
                                src = img.get("src")
                                agent = re.search(r'/(\w+)\.png', src).group(1)
                                values.append(agent)
                            else:
                                stat = td.text.split()[0]
                                stat_name = performance_stats_title[index % len(performance_stats_title)]
                                rounds_divs = td.find("div").find("div").find("div").find_all("div")
                                values.append(stat)
                                for round_div in rounds_divs:
                                    kills_div = round_div.find_all("div")
                                    for div in kills_div:
                                        img = div.find("img")
                                        if img == None:
                                            round_stat = div.text.strip()
                                        else:
                                            src = img.get("src")
                                            eliminated_agent = re.search(r'/(\w+)\.png', src).group(1)
                                            eliminated = div.text.strip()
                                            if not eliminated:
                                                eliminated = pd.NA
                                                eliminated_team = pd.NA
                                            else:
                                                eliminated_team = player_to_team[eliminated]
                                            results["rounds_kills"].append([tournament_name, stage_name, match_type_name, match_name, map, round_stat,
                                                                            team, player, agent, eliminated_team, eliminated, eliminated_agent, stat_name])
                        else:
                            stat = td.text.strip()
                            stat_name = performance_stats_title[index % len(performance_stats_title)]
                            if not stat:
                                stat = pd.NA
                            values.append(stat)
                    results["kills_stats"].append(values)

        except AttributeError as e:
            print(f"{tournament_name}, {stage_name}, {match_type_name}, {match_name}, does not contain any data under their performance page.")
        except Exception as e:
            print(f"ERROR COMING FROM SCRAPING THE PERFORMANCE PAGE")
            print(f"{tournament_name}, {stage_name}, {match_type_name}, {match_name}")
            traceback.print_exc()

def extract_economy_stats_div(economy_stats_div):
    eco_stats = {}
    eco_rounds_stats = {}
    for div in economy_stats_div:
        id = div.get("data-game-id")
        stats_div = div.find_all(recursive=False)
        if len(stats_div) == 3:
            eco_stats[id] = []
            eco_rounds_stats[id] = []
            eco_stats_trs = stats_div[0].find_all("tr")[1:]
            eco_rounds_trs = stats_div[2].find_all("tr")
            for tr in eco_stats_trs:
                tds = tr.find_all("td")
                eco_stats[id].extend(tds)
            for tr in eco_rounds_trs:
                tds = tr.find_all("td")
                eco_rounds_stats[id].extend(tds)
        
        elif len(stats_div) == 2:
            eco_stats[id] = []
            eco_rounds_stats[id] = []
            eco_stats_trs = stats_div[0].find_all("tr")[1:]
            for tr in eco_stats_trs:
                tds = tr.find_all("td")
                eco_stats[id].extend(tds)
    return eco_stats, eco_rounds_stats

def extract_economy_stats(eco_stats, eco_rounds_stats, games_id, team_mapping, results, list):
    tournament_name = list[0]
    stage_name = list[1]
    match_type_name = list[2]
    match_name = list[3]
    team_a = list[4]
    team_b = list[5]
    if eco_stats:     
        
        for id, td_list in eco_stats.items():
            try:
                map = games_id[id]
            except KeyError: #match is BO1
                continue
            for index, td in enumerate(td_list):
                class_name = td.find("div").get("class")[0]
                if class_name == "team":
                    team = td.text.strip()
                    try:
                        team = team_mapping[team]
                    except KeyError: #They uppercased the team name
                        if team.lower() == team_a.lower():
                            team = team_a
                        elif team.lower() == team_b.lower():
                            team = team_b
                else:
                    stats = td.text.strip().replace("(", "").replace(")", "").split()
                    if len(stats) > 1:
                        initiated, won = stats[0], stats[1]
                    else:
                        initiated, won = pd.NA, stats[0]
                    stat_name = economy_stats_title[index % len(economy_stats_title)]
                    results["eco_stats"].append([tournament_name, stage_name, match_type_name, match_name,
                                                    map, team, stat_name, initiated, won])
        # for id, td_list in eco_rounds_stats.items():
        #     try:
        #         map = games_id[id]
        #     except KeyError: #match is BO1
        #         continue
        #     for index, td in enumerate(td_list):
        #         teams = td.find_all("div", class_="team")
        #         if teams:
        #             try:
        #                 team_1 = team_mapping[teams[0]]
        #             except KeyError: #They did not use the abbrievated team name
        #                 team_1 = team_a
        #             try:
        #                 team_2 = team_mapping[teams[1]]
        #             except KeyError: #They did not use the abbrievated team name
        #                 team_2 = team_b

        #         else:
        #             stats = td.find_all("div")
        #             commented_divs = td.find_all(string=lambda text: isinstance(text, Comment) and '<div>' in text and '</div>' in text)
        #             divs_contents = [div.extract() for comment in commented_divs for div in BeautifulSoup(comment, 'html.parser').find_all('div')]
        #             loadout_values = [value.text.strip() for value in divs_contents]
        #             round = stats[0].text.strip()
        #             team_a_loadout_value = loadout_values[0]
        #             team_a_remaining_credits = stats[1].text.strip()
        #             team_b_loadout_value = loadout_values[1]
        #             team_b_remaining_credits = stats[4].text.strip()
        #             team_a_eco_type = eco_types[stats[2].text.strip()]
        #             team_b_eco_type = eco_types[stats[3].text.strip()]
        #             if "mod-win" in stats[2]["class"]:
        #                 team_a_outcome = "Win"
        #                 team_b_outcome = "Loss"
        #             else:
        #                 team_a_outcome = "Loss"
        #                 team_b_outcome = "Win"
        #             results["eco_rounds"].append([tournament_name, stage_name, match_type_name, match_name, map,
        #                                         round, team_1, team_a_loadout_value, team_a_remaining_credits, team_a_eco_type, team_a_outcome])
        #             results["eco_rounds"].append([tournament_name, stage_name, match_type_name, match_name, map,
        #                                         round, team_2, team_b_loadout_value, team_b_remaining_credits, team_b_eco_type, team_b_outcome])
                
                
    else:
        print(tournament_name, stage_name, match_type_name, match_name, "does not contain any data under their economy page")

def extract_agent_pictures(agent_pictures, global_table_titles):
    for th in agent_pictures:
        src = th.find("img").get("src")
        match = re.search(pattern, src)
        agent = match.group(1)
        global_table_titles.append(agent)

def extract_pick_rates(table_stats_tr, maps_stats_dict, agents_pick_rates_dict, global_table_titles):
    for tr in table_stats_tr:
        all_tds = tr.find_all("td")
        filtered_tds = [td for td in all_tds if isinstance(td, Tag)]
        for index, td in enumerate(filtered_tds):
            td_class = td.get("class") or ""
            class_name = " ".join(td_class)
            if not class_name:
                map = td.text.strip().replace("\t", "")
                if not map:
                    map = "All Maps"
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

def extract_team_picked_agents(teams_tables, teams_pick_rates_dict, table_titles):
    for table in teams_tables:
        logo, map = table.find("tr").find("th").text.replace("\t", "").split()
        map_dict = teams_pick_rates_dict.setdefault(map, {})
        teams_pick_rate_tr = table.find_all("tr")[1:]
        for tr in teams_pick_rate_tr:
            tr_class = tr.get("class")
            class_name = " ".join(tr_class)
            if class_name == "pr-matrix-row":
                all_tds = tr.find_all("td")
                filtered_tds = [td for td in all_tds if isinstance(td, Tag)]
                contained_any_agents = any(td.has_attr('class') and ('mod-picked' in td['class']) for td in filtered_tds)
                if contained_any_agents:
                    a_tag = filtered_tds[0].find("a")
                    team = a_tag.text.strip()
            elif "mod-dropdown" in class_name:
                all_tds = tr.find_all("td")
                filtered_tds = [td for td in all_tds if isinstance(td, Tag)]
                for index, td in enumerate(filtered_tds):
                    td_class = td.get("class") or ""
                    class_name = "".join(td_class)
                    if class_name == "mod-loss" or class_name == "mod-win":
                        outcome = class_name.split("-")[-1]
                    elif class_name == "mod-picked-lite":
                        agent = table_titles[index]
                        
                        team_dict = map_dict.setdefault(team, {"Total Maps Played": 0, "Total Outcomes": {}})
                        agent_dict = team_dict["Total Outcomes"].setdefault(agent, {"win": 0, "loss": 0})
                        agent_dict[outcome] += 1
                team_dict["Total Maps Played"] += 1

def extract_players_stats(stats_trs, match_type_dict, global_players_agents, players_agents, df, tournament_name, stage_name, match_type_name, agent):
    for tr in stats_trs:
        all_tds = tr.find_all("td")
        filtered_tds = [td for td in all_tds if isinstance(td, Tag)]
        for index, td in enumerate(filtered_tds):
            td_class = td.get("class") or ""
            class_name = " ".join(td_class)
            if class_name == "mod-player mod-a":
                player_info = td.find("div").find_all("div")
                player, team = player_info[0].text, player_info[1].text
                if stage_name == "All Stages" and match_type_name == "All Match Types":
                    filtered = df[(df["Player"] == player) & (df["Tournament"] == tournament_name)]
                    try:
                        team = ", ".join(filtered["Team"].unique()).strip(", ")
                    except TypeError:
                        print("ERROR")
                        print(player, team, tournament_name, stage_name, match_type_name, agent, " is causing this issue")
                        print("Their team was missing during the scraping ")
                else:
                    filtered = df[(df["Player"] == player) & (df["Tournament"] == tournament_name) & (df["Stage"] == stage_name) & (df["Match Type"] == match_type_name)]
                    try:
                        team = ", ".join(filtered["Team"].unique()).strip(", ")
                    except:
                        print("ERROR")
                        print(player, team, tournament_name, stage_name, match_type_name, agent, " is causing this issue")
                        sys.exit(1)
                team_dict = match_type_dict.setdefault(team, {})
                player_dict = team_dict.setdefault(player, {})
            elif class_name == "mod-agents":
                imgs = td.find("div").find_all("img")
                agents = ""
                player_agents_set = players_agents.setdefault(player, set())
                global_players_agents_set = global_players_agents.setdefault(player, set())
                if stage_name == "All" and match_type_name == "All": 
                    agents = ", ".join(global_players_agents_set).strip(", ")
                elif agent == "all" and len(player_agents_set) > 1:
                    agents = ", ".join(player_agents_set).strip(", ")
                elif agent == "all" and len(player_agents_set) == 1:
                    break
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
                    stat = pd.NA
                agents_dict[stat_name] = stat
            elif class_name == "mod-color-sq mod-acs" or class_name ==  "mod-color-sq":
                stat = td.find("div").find("span").text.strip()
                stat_name = stats_titles[index]
                if stat == "":
                    stat = pd.NA
                agents_dict[stat_name] = stat
            elif class_name == "mod-a mod-kmax":
                stat = td.find("a").text.strip()
                stat_name = stats_titles[index]
                if stat == "":
                    stat = pd.NA
                agents_dict[stat_name] = stat
        global_players_agents[player] = global_players_agents[player] | players_agents[player]