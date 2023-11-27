import asyncio
import sys
from bs4 import BeautifulSoup
from MaxReentriesReached.max_reentries_reached import MaxReentriesReached
import pandas as pd
import re

overview_stats_titles = ["", "", "Rating", "Average Combat Score", "Kills", "Deaths", "Assists", "Kills - Deaths (KD)",
                        "Kill, Assist, Trade, Survive %", "Average Damage per Round", "Headshot %", "First Kills",
                        "First Deaths", "Kills - Deaths (FKD)"]
performance_stats_title = ["", "", "2k", "3k", "4k", "5k", "1v1", "1v2", "1v3", "1v4", "1v5", "Economy", "Spike Plants", "Spike Defuse"]
economy_stats_title = ["Pistol Won", "Eco (won)", "$ (won)", "$$ (won)", "$$$ (won)"]
overview, performance, economy = "Overview", "Performance", "Economy"
specific_kills_name = ["All Kills", "First Kills", "Op Kills"]
eco_types = {"": "Eco: 0-5k", "$": "Semi-eco: 5-10k", "$$": "Semi-buy: 10-20k", "$$$": "Full buy: 20k+"}


async def fetch(url, session):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with session.get(url, timeout=30) as response:
                if response.status == 200:
                    return await response.text()
        except asyncio.TimeoutError:
            print(f"Timeout error for URL: {url}, retrying.....")
        await asyncio.sleep(2 ** attempt)
    else:
        # print(f"Max retries reached for URL: {url}")
        raise MaxReentriesReached(f"Max retries reached for URL: {url}")
    

    
async def generate_urls_combination(tournament_name, url, stages_filter, session):
    page = await fetch(url, session)
    soup = BeautifulSoup(page.content, "html.parser")

    all_stages = soup.find("div", class_="wf-card mod-dark mod-scroll stats-filter").find("div").find_all("div", recursive=False)
    tournament_dict = stages_filter.setdefault(tournament_name, {})
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

async def scraping_matches_data(tournament_name, cards, session):
    result = {"scores": [],
              "maps_played": [],
              "maps_scores": [],
              "draft_phase": [],
              "overview": [],
              "kills": [],
              "kills_stats": [],
              "rounds_kills": [],
              "eco_stats": [],
              "eco_rounds": []}
    team_mapping = {}
    for module in cards:
        match_type_name, stage_name = module.find("div", class_="match-item-event text-of").text.strip().splitlines()
        match_type_name = match_type_name.strip("\t")
        stage_name = stage_name.strip("\t")
        if tournament_name != "Pacific League":
            break
        if match_type_name == "Showmatch":
            continue
        else:
            loser, loser_flag, loser_score = module.find("div", class_="match-item-vs").select('div.match-item-vs-team:not([class*=" "])')[0].find_all("div")
            loser = loser.text.strip("\n").strip("\t")
            loser_score = loser_score.text.strip("\n").strip("\t")

            winner, winner_flag, winner_score = module.find("div", class_="match-item-vs").find("div", class_="match-item-vs-team mod-winner").find_all("div")
            winner = winner.text.strip("\n").strip("\t")
            winner_score = winner_score.text.strip("\n").strip("\t")

            teams = module.find("div", class_="match-item-vs").find_all(recursive=False)

            team_a = teams[0].find("div").text.strip("\n").strip("\t")

            team_b = teams[1].find("div").text.strip("\n").strip("\t")


            match_name = f"{team_a} vs {team_b}"

            result["scores"].append([tournament_name, stage_name, match_type_name, match_name, winner,loser, winner_score, loser_score])
            print("Starting collecting for ",tournament_name, stage_name, match_type_name, match_name)
            url = module.get("href")
            try:
                match_page = await fetch(f'https://vlr.gg{url}', session)
            except MaxReentriesReached as e:
                print(f"Error: {e}")
                sys.exit(1)
            match_soup = BeautifulSoup(match_page, "html.parser")

            maps_id = {}
            
            maps_id_divs = match_soup.find("div", class_="vm-stats-gamesnav").find_all("div")
            for div in maps_id_divs:
                if div.get("data-game-id") and div.get("data-disabled") == "0":
                    id = div.get("data-game-id")
                    map = re.sub(r"\d+|\t|\n", "", div.text.strip())
                    maps_id[id] = map

            for id, map in maps_id.items():
                if map != "All Maps":
                    result["maps_played"].append([tournament_name, stage_name, match_type_name, match_name, map])


            overview_stats = match_soup.find_all("div", class_="vm-stats-game")

            overview_tables = overview_stats[0].find_all("table")

            team_a_abbriev = overview_tables[0].find("tbody").find("tr").find("td").find("a").find_all("div")[-1].text.strip()

            team_b_abbriev = overview_tables[1].find("tbody").find("tr").find("td").find("a").find_all("div")[-1].text.strip()

            maps_headers = match_soup.find_all("div", class_="vm-stats-game-header")

            if team_a not in team_mapping:
                team_mapping[team_a_abbriev] = team_a
            
            if team_b not in team_mapping:
                team_mapping[team_b_abbriev] = team_b

            maps_notes = match_soup.find_all("div", class_="match-header-note")
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
                except:
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
                                player, team = td.find("a").find_all("div")
                                player, team =  player.text.strip(), team.text.strip()
                                team = team_mapping[team]
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
            try:
                performance_page = await fetch(f'https://vlr.gg{url}/?game=all&tab=performance', session)
            except MaxReentriesReached as e:
                print(f"Error: {e}")
                sys.exit(1)
            performance_soup = BeautifulSoup(performance_page, "html.parser")
            performance_stats_div = performance_soup.find_all("div", class_="vm-stats-game")

            
            try:
                team_b_div = performance_stats_div[0].find("div").find("tr").find_all("div", class_="team")
                team_b_players = [""]
                team_b_lookup = {}
                team_a_lookup = {}
                for player in team_b_div:
                    player, team = player.text.strip().replace("\t", "").split("\n")
                    team_b_lookup[player] = team_b
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
                    map = maps_id[id]
                    for index, td_list in enumerate(tds_lists):
                        for team_b_player_index, td in enumerate(td_list):
                            if td.find("img") != None:
                                player, team = td.text.strip().replace("\t", "").split("\n")
                                kill_name = specific_kills_name[index // (len(team_b_players) - 1)]
                                team = team_mapping[team]
                                team_a_lookup[player] = team
                            else:
                                kills_div = td.find("div").find_all("div")
                                player_a_kills, player_b_kills, difference = kills_div[0].text.strip(), kills_div[1].text.strip(), kills_div[2].text.strip()
                                player_b = team_b_players[team_b_player_index]
                                if not player_a_kills and not player_b_kills and not difference:
                                    player_a_kills, player_b_kills, difference = pd.NA, pd.NA, pd.NA
                                result["kills"].append([tournament_name, stage_name, match_type_name, match_name, map, team, player,
                                                         team_b, player_b, player_a_kills, player_b_kills, difference,
                                                         kill_name])
                               
                for id, tds_lists in players_kills.items():
                    map = maps_id[id]
                    for tds in tds_lists:
                        values = [tournament_name, stage_name, match_type_name, match_name, map]
                        for index, td in enumerate(tds):
                            img = td.find("img")
                            if img != None:
                                class_name = " ".join(td.find("div").get("class"))
                                if class_name == "team":
                                    player, team = td.text.strip().replace("\t", "").split("\n")
                                    team = team_mapping[team]
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
                                                agent = re.search(r'/(\w+)\.png', src).group(1)
                                                victim = div.text.strip()
                                                team = team_a_lookup.get(victim) or team_b_lookup.get(victim)
                                                result["rounds_kills"].append([tournament_name, stage_name, match_type_name, match_name, map, round_stat,
                                                                                team, player, agent, team, victim, agent, stat_name])
                            else:
                                stat = td.text.strip()
                                stat_name = performance_stats_title[index % len(performance_stats_title)]
                                if not stat:
                                    stat = pd.NA
                                values.append(stat)
                        result["kills_stats"].append(values)

            except Exception as e:
                print(e)
                print(tournament_name, stage_name, match_type_name, match_name, "does not contain any data under their performance page. Either their page was empty or something went wrong during the scraping")
            try:
                economy_page = await fetch(f'https://vlr.gg{url}/?game=all&tab=economy', session)
            except MaxReentriesReached as e:
                print(f"Error: {e}")
                sys.exit(1)
            economy_soup = BeautifulSoup(economy_page, "html.parser")

            economy_stats_div = economy_soup.find_all("div", class_="vm-stats-game")

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
            
            if eco_stats:     
                
                for id, td_list in eco_stats.items():
                    map = maps_id[id]
                    for index, td in enumerate(td_list):
                        class_name = td.find("div").get("class")[0]
                        if class_name == "team":
                            team = td.text.strip()
                            team = team_mapping[team]
                        else:
                            stats = td.text.strip().replace("(", "").replace(")", "").split()
                            if len(stats) > 1:
                                initiated, won = stats[0], stats[1]
                            else:
                                initiated, won = pd.NA, stats[0]
                            stat_name = economy_stats_title[index % len(economy_stats_title)]
                            result["eco_stats"].append([tournament_name, stage_name, match_type_name, match_name,
                                                         map, team, stat_name, initiated, won])
                for id, td_list in eco_rounds_stats.items():
                    map = maps_id[id]
                    for index, td in enumerate(td_list):
                        teams = td.find_all("div", class_="team")
                        if teams:
                            team_a, team_b = teams[0].text.strip(), teams[1].text.strip()
                            team_a = team_mapping[team_a]
                            team_b = team_mapping[team_b]
                        else:
                            stats = td.find_all("div")
                            round = stats[0].text.strip()
                            team_a_bank = stats[1].text.strip()
                            team_a_eco_type = eco_types[stats[2].text.strip()]
                            team_b_eco_type = eco_types[stats[3].text.strip()]
                            team_b_bank = stats[4].text.strip()
                            if "mod-win" in stats[2]["class"]:
                                team_a_outcome = "Win"
                                team_b_outcome = "Lost"
                            else:
                                team_a_outcome = "Lost"
                                team_b_outcome = "Win"
                            result["eco_rounds"].append([tournament_name, stage_name, match_type_name, match_name, map,
                                                        round, team_a, team_a_bank, team_a_eco_type, team_a_outcome])
                            result["eco_rounds"].append([tournament_name, stage_name, match_type_name, match_name, map,
                                                        round, team_b, team_b_bank, team_b_eco_type, team_b_outcome])
                        
                        
            else:
                print(tournament_name, stage_name, match_type_name, match_name, "does not contain any data under their economy page")
    return result

async def scraping_agents_data(tournament_name, stages, session):
    global_table_titles = ["Map", "Total Played", "Attacker Side Win", "Defender Side Win"]
    pattern = r'/(\w+)\.png'
    results = {}
    tournament_dict = results.setdefault(tournament_name, {})
    for stage_name, match_types in stages.items():
        stage_dict = tournament_dict.setdefault(stage_name, {})
        for match_type_name, url in match_types.items():
            match_type_dict = stage_dict.setdefault(match_type_name, {})
            maps_stats_dict = match_type_dict.setdefault("Maps Stats", {})
            agents_pick_rates_dict = match_type_dict.setdefault("Agents Pick Rates", {})
            teams_pick_rates_dict = match_type_dict.setdefault("Teams Pick Rates", {})

            page = await fetch(url, session)
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