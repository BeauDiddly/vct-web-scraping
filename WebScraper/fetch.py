import asyncio
import sys
from bs4 import BeautifulSoup, Tag
from MaxReentriesReached.max_reentries_reached import MaxReentriesReached
from WebScraper.web_data_extraction import *
import pandas as pd
import re
import random
import logging

logging.basicConfig(level=logging.INFO)  # Set the desired logging level

# Define a logger
logger = logging.getLogger(__name__)

headers = {
    "User-Agent": ""
}

semaphore_count = 2

async def fetch(url, session):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with session.get(url, timeout=30) as response:
                if response.status == 200:
                    return await response.text()
        except asyncio.TimeoutError:
            logger.warning(f"Timeout error for URL: {url}, retrying..... (Attempt {attempt + 1})")
        await asyncio.sleep(2 ** attempt)
    else:
        # print(f"Max retries reached for URL: {url}")
        logger.error(f"Max retries reached for URL: {url}")
        raise MaxReentriesReached(f"Max retries reached for URL: {url}")
    

    
async def generate_urls_combination(tournament_name, url, stages_filter, session):
    try:
        page = await fetch(url, session)
    except MaxReentriesReached as e:
        print(f"Error: {e}")
        sys.exit(1)
    soup = BeautifulSoup(page, "html.parser")

    all_stages = soup.find("div", class_="wf-card mod-dark mod-scroll stats-filter").find("div").find_all("div", recursive=False)
    tournament_dict = stages_filter.setdefault(tournament_name, {})
    all_ids = ""
    showmatch_id = ""
    for stage in all_stages:
        stage_name_div, match_types_div = stage.find_all("div", recursive=False)
        stage_name = stage_name_div.find("div").text.strip()
        if stage_name == "Showmatch":
            showmatch_id = match_types_div.find("div").get("data-subseries-id")
            continue
        match_types = match_types_div.find_all("div")
        stage_dict = tournament_dict.setdefault(stage_name, {})
        for match_type in match_types:
            match_type_name = match_type.text.strip()
            id = match_type.get("data-subseries-id")
            stage_dict[match_type_name] = id
            all_ids += f"{id}."
    all_ids = all_ids.strip(".").split(".")
    for stage_name, match_types in tournament_dict.items():
        for match_type, id in match_types.items():
            excluded_ids = ".".join(exclude_id for exclude_id in all_ids if exclude_id != id)
            filter_url = f"{url}?exclude={excluded_ids}.{showmatch_id}"
            tournament_dict[stage_name][match_type] = filter_url
    tournament_dict["All Stages"] = {}
    tournament_dict["All Stages"]["All Match Types"] = f"{url}?exclude={showmatch_id}"

async def scraping_card_data(tournament_name, card, session, card_semaphore):
    async with card_semaphore:
        await asyncio.sleep(random.uniform(1,2))
        match_type_name, stage_name = card.find("div", class_="match-item-event text-of").text.strip().splitlines()
        match_type_name = match_type_name.strip("\t")
        stage_name = stage_name.strip("\t")
        if match_type_name == "Showmatch" or tournament_name != "Road to VCT 2022":
            return {}
        else:
            results = {"scores": [],
                "maps_played": [],
                "maps_scores": [],
                "draft_phase": [],
                "overview": [],
                "kills": [],
                "kills_stats": [],
                "rounds_kills": [],
                "eco_stats": [],
                "eco_rounds": []}
            
            teams = card.find("div", class_="match-item-vs").find_all(recursive=False)


            team_a = teams[0].find("div").text.strip("\n").strip("\t")

            team_b = teams[1].find("div").text.strip("\n").strip("\t")

            match_name = f"{team_a} vs {team_b}"

            team_a_score = int(teams[0].find("div", class_="match-item-vs-team-score js-spoiler").text.strip())
            team_b_score = int(teams[1].find("div", class_="match-item-vs-team-score js-spoiler").text.strip())
            if team_a_score > team_b_score:
                winner = team_a
                winner_score = team_a_score
                loser = team_b
                loser_score = team_b_score
            else:
                winner = team_b
                winner_score = team_b_score
                loser = team_a
                loser_score = team_a_score


            team_mapping = {}

            results["scores"].append([tournament_name, stage_name, match_type_name, match_name, winner,loser, winner_score, loser_score])
            print("Starting collecting for ",tournament_name, stage_name, match_type_name, match_name)
            url = card.get("href")
            try:
                match_page = await fetch(f'https://vlr.gg{url}', session)
            except MaxReentriesReached as e:
                print(f"Error: {e}")
                sys.exit(1)
            match_soup = BeautifulSoup(match_page, "html.parser")

            try:
                overview_stats = match_soup.find_all("div", class_="vm-stats-game")
                overview_tables = overview_stats[0].find_all("table")

                team_a_abbriev = overview_tables[0].find("tbody").find("tr").find("td").find("a").find_all("div")[-1].text.strip()

                if not team_a_abbriev:
                    team_a_abbriev = team_a

                team_b_abbriev = overview_tables[1].find("tbody").find("tr").find("td").find("a").find_all("div")[-1].text.strip()

                if not team_b_abbriev:
                    team_b_abbriev = team_b

                if team_a_abbriev not in team_mapping:
                    team_mapping[team_a_abbriev] = team_a
                
                if team_b_abbriev not in team_mapping:
                    team_mapping[team_b_abbriev] = team_b


                maps_id = {}
                maps_id_divs = match_soup.find("div", class_="vm-stats-gamesnav").find_all("div")
                extract_maps_id(maps_id_divs, maps_id, results, [tournament_name, stage_name, match_type_name, match_name])


                maps_notes = match_soup.find_all("div", class_="match-header-note")
                extract_maps_notes(maps_notes, results, team_mapping, [tournament_name, stage_name, match_type_name, match_name, f'https://vlr.gg{url}'])

                maps_headers = match_soup.find_all("div", class_="vm-stats-game-header")
                extract_maps_headers(maps_headers, results, team_a, team_b, [tournament_name, stage_name, match_type_name, match_type_name])

                player_to_team, missing_team = extract_overview_stats(overview_stats, maps_id, team_mapping, results, [tournament_name, stage_name, match_type_name, match_name, team_a, team_b, f'https://vlr.gg{url}'])
            except IndexError:
                print(f"ERROR FROM SCRAPING OVERVIEW PAGE")
                print(f"{tournament_name}, {stage_name}, {match_type_name}, {match_name}, the match was forfeited")
                return {}

            await asyncio.sleep(random.uniform(1,2))

            try:
                performance_page = await fetch(f'https://vlr.gg{url}/?game=all&tab=performance', session)
            except MaxReentriesReached as e:
                print(f"Error: {e}")
                sys.exit(1)
            performance_soup = BeautifulSoup(performance_page, "html.parser")
            performance_stats_div = performance_soup.find_all("div", class_="vm-stats-game")

            extract_kills_stats(performance_stats_div, maps_id, team_mapping, player_to_team, missing_team, results, [tournament_name, stage_name, match_type_name, match_name, team_b, f'https://vlr.gg{url}/?game=all&tab=performance'])

            await asyncio.sleep(random.uniform(1,2))
                
            try:
                economy_page = await fetch(f'https://vlr.gg{url}/?game=all&tab=economy', session)
            except MaxReentriesReached as e:
                print(f"Error: {e}")
                sys.exit(1)
            economy_soup = BeautifulSoup(economy_page, "html.parser")

            economy_stats_div = economy_soup.find_all("div", class_="vm-stats-game")

            eco_stats, eco_rounds_stats = extract_economy_stats_div(economy_stats_div)
            extract_economy_stats(eco_stats, eco_rounds_stats, maps_id, team_mapping, results, [tournament_name, stage_name, match_type_name, match_name, team_a, team_b])

        results["team_mapping"] = team_mapping
        await asyncio.sleep(random.uniform(1,2))
        return results



async def scraping_matches_data(tournament_name, cards, session):
    card_semaphore = asyncio.Semaphore(semaphore_count)
    tasks = [scraping_card_data(tournament_name, card, session, card_semaphore) for card in cards]
    results = await asyncio.gather(*tasks)
    return results


async def scraping_agents_data(tournament_name, stages, session): 
    global_table_titles = ["Map", "Total Played", "Attacker Side Win", "Defender Side Win"]
    pattern = r'/(\w+)\.png'
    result = {}
    tournament_dict = result.setdefault(tournament_name, {})
    for stage_name, match_types in stages.items():
        stage_dict = tournament_dict.setdefault(stage_name, {})
        for match_type_name, url in match_types.items():
            match_type_dict = stage_dict.setdefault(match_type_name, {})
            maps_stats_dict = match_type_dict.setdefault("Maps Stats", {})
            agents_pick_rates_dict = match_type_dict.setdefault("Agents Pick Rates", {})
            teams_pick_rates_dict = match_type_dict.setdefault("Teams Pick Rates", {})
            print(f"Collecting data for {tournament_name}, {stage_name}, {match_type_name}")
            page = await fetch(url, session)
            soup = BeautifulSoup(page, "html.parser")
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
            
            teams_tables = soup.select('table.wf-table:not([class*=" "])')
            table_titles = ["", ""] + global_table_titles[4:]

            for table in teams_tables:
                all_tr = table.find_all("tr")
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
                    
    return result

async def scraping_players_stats(tournament_name, stages, team_napping, session):
    result = {}
    global_players_agents = {}
    pattern = r'/(\w+)\.png'
    tournament_dict = result.setdefault(tournament_name, {})
    for stage_name, match_types in stages.items():
        stage_dict = tournament_dict.setdefault(stage_name, {})
        for match_type_name, url in match_types.items():
            match_type_dict = stage_dict.setdefault(match_type_name, {})
            players_agents = {}
            for agent in all_agents:
                print(f"Collecting data for {agent} {tournament_name}, {stage_name}, {match_type_name}")
                try:
                    page = await fetch(f"{url}&min_rounds=0&agent={agent}", session)
                except MaxReentriesReached as e:
                    print(f"Error: {e}")
                    sys.exit(1)
                await asyncio.sleep(random.uniform(0, 1))
                soup = BeautifulSoup(page, "html.parser")
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
                            team = team_napping[team]
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
                                continue
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
    return result