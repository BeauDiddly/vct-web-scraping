import asyncio
import sys
from bs4 import BeautifulSoup, Tag
from MaxReentriesReached.max_reentries_reached import MaxReentriesReached
from WebScraper.web_data_extraction import *
import pandas as pd
import re
import random
import logging
import traceback

logging.basicConfig(level=logging.INFO)  # Set the desired logging level

# Define a logger
logger = logging.getLogger(__name__)

headers = {
    "User-Agent": ""
}

all_agents = ["astra", "breach", "brimstone", "chamber", "cypher", "deadlock", "fade", "gekko", "harbor", "iso", "jett", "kayo",
              "killjoy", "neon", "omen", "phoenix", "raze", "reyna", "sage", "skye", "sova", "viper", "yoru", "all"]

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
    

    
async def generate_urls_combination(tournament_name, stages_ids, match_types_ids, url, stages_filter, semaphore, session):
    async with semaphore:
        try:
            page = await fetch(url, session)
        except MaxReentriesReached as e:
            print(f"Error: {e}")
            sys.exit(1)
        soup = BeautifulSoup(page, "html.parser")

        all_stages = soup.find("div", class_="wf-card mod-dark mod-scroll stats-filter").find("div").find_all("div", recursive=False)
        tournament_dict = stages_filter.setdefault(tournament_name, {})
        stages_to_ids = stages_ids.setdefault(tournament_name, {})
        stages_to_match_types = match_types_ids.setdefault(tournament_name, {})
        all_ids = ""
        showmatch_id = ""
        for stage in all_stages:
            stage_div, match_types_div = stage.find_all("div", recursive=False)
            stage_name = stage_div.find("div").text.strip()
            if stage_name == "Showmatch":
                showmatch_id = match_types_div.find("div").get("data-subseries-id")
                continue
            stage_id = stage_div.find("a").get("data-series-id")
            stages_to_ids[stage_name] = stage_id
            match_types = match_types_div.find_all("div")
            stage_dict = tournament_dict.setdefault(stage_name, {})
            match_types_to_ids = stages_to_match_types.setdefault(stage_name, {})
            for match_type in match_types:
                match_type_name = match_type.text.strip()
                id = match_type.get("data-subseries-id")
                match_types_to_ids[match_type_name] = id
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
        stages_to_match_types["All Stages"] = {}
        stages_to_match_types["All Stages"]["All Match Types"] = pd.NA
        stages_to_ids["All Stages"] = pd.NA


async def scraping_card_data(tournament_name, card, tournaments_ids, stages_ids, session, semaphore):
    async with semaphore:
        await asyncio.sleep(random.uniform(1,2))
        match_type_name, stage_name = card.find("div", class_="match-item-event text-of").text.strip().splitlines()
        match_type_name = match_type_name.strip("\t")
        stage_name = stage_name.strip("\t")
        if match_type_name == "Showmatch":
            return {}
        else:
            results = {"scores": [],
                "maps_played": [],
                "maps_scores": [],
                "draft_phase": [],
                "win_loss_methods_count": [],
                "win_loss_methods_round_number": [],
                "overview": [],
                "kills": [],
                "kills_stats": [],
                "rounds_kills": [],
                "eco_stats": [],
                "eco_rounds": [],
                "teams_ids": {},
                "players_ids": {},
                "tournaments_stages_matches_games_ids": []}

            try:
                tournament_id = tournaments_ids[tournament_name]
            except KeyError:
                tournament_id = 0000
                
            try:    
                stage_id = stages_ids[tournament_name][stage_name]
            except KeyError:
                stage_id = 0000
            teams = card.find("div", class_="match-item-vs").find_all(recursive=False)


            team_a = teams[0].find("div").text.strip("\n").strip("\t").strip()

            if not team_a:
                team_a = "TBD"

            team_b = teams[1].find("div").text.strip("\n").strip("\t").strip()

            if not team_b:
                team_b = "TBD"
                
            match_name = f"{team_a} vs {team_b}"

            
            try:
                team_a_score = int(teams[0].find("div", class_="match-item-vs-team-score js-spoiler").text.strip())
            except AttributeError: #match was foreited
                print(f"N/A SCORE FROM {team_a}")
                print(f"{tournament_name}, {stage_name}, {match_type_name}, {match_name}, match was foreited")
                return {}
            try:
                team_b_score = int(teams[1].find("div", class_="match-item-vs-team-score js-spoiler").text.strip())
            except AttributeError: #match was foreited
                print(f"N/A SCORE FROM {team_b}")
                print(f"{tournament_name}, {stage_name}, {match_type_name}, {match_name}, match was foreited")
                return {}
            match_result = f""
            if team_a_score > team_b_score:
                match_result = f"{team_a} won"
            elif team_b_score > team_a_score:
                match_result = f"{team_b} won"
            elif team_a_score == 0 and team_b_score == 0:
                print(f"No score from {team_a} and {team_b} Score: {team_a_score} - {team_b_score}")
                print(f"{tournament_name}, {stage_name}, {match_type_name}, {match_name}, match was foreited")
                return {}

            elif team_a_score == team_b_score:
                if "mod-winner" in teams[0].get("class", []):
                    match_result = f"{team_a} won"
                elif "mod-winner" in teams[1].get("class", []):
                    match_result = f"{team_b} won"
                else:
                    match_result = f"Draw"
            
            team_mapping = {}
            try:
                results["scores"].append([tournament_name, stage_name, match_type_name, match_name, team_a, team_b, team_a_score, team_b_score, match_result])
            except UnboundLocalError:
                print(f"{team_a} {team_a_score} {team_b} {team_b_score}")
                print(f"{tournament_name} {stage_name} {match_type_name} {match_name}")
            print("Starting collecting for ",tournament_name, stage_name, match_type_name, match_name)
            url = card.get("href")
            match_id = url.split("/")[1]
            try:
                match_page = await fetch(f'https://vlr.gg{url}', session)
            except MaxReentriesReached as e:
                print(f"Error: {e}")
                sys.exit(1)
            match_soup = BeautifulSoup(match_page, "html.parser")
            team_ids_div = match_soup.find("div", class_="match-header-vs").find_all("a")

            team_a_id = team_ids_div[0].get("href")

            if team_a_id:
                team_a_id = team_a_id.split("/")[2]
            else:
                team_a_id = pd.NA

            team_b_id = team_ids_div[1].get("href")

            if team_b_id:
                team_b_id = team_b_id.split("/")[2]
            else:
                team_b_id = pd.NA
            # print(team_a, team_a_id)
            # print(team_b, team_b_id)
            results["teams_ids"][team_a] = team_a_id
            results["teams_ids"][team_b] = team_b_id


            try:
                overview_stats = match_soup.find_all("div", class_="vm-stats-game")
                overview_tables = overview_stats[1].find_all("table")
                try:
                    team_a_abbriev = overview_tables[0].find("tbody").find("tr").find("td").find("a").find_all("div")[-1].text.strip()
                except AttributeError: #abbrievated name for team a was not found which means the players name will be missing
                    try:
                        team_a_abbriev = overview_tables[0].find("tbody").find("tr").find("td").find("div").find("div").find_all("div")[-1].text.strip()
                    except AttributeError:
                        print(f"Couldn't find the abbrievated name for {team_a}")
                        print(f"{tournament_name}, {stage_name}, {match_type_name}, {match_name}")
                        team_a_abbriev = team_a

                
                try:
                    team_b_abbriev = overview_tables[1].find("tbody").find("tr").find("td").find("a").find_all("div")[-1].text.strip()
                except AttributeError: #abbrievated name for team b was not found which means the players name will be missing
                    try:
                        team_b_abbriev = overview_tables[1].find("tbody").find("tr").find("td").find("div").find("div").find_all("div")[-1].text.strip()
                    except AttributeError:
                        print(f"Couldn't find the abbrievated name for {team_b}")
                        print(f"{tournament_name}, {stage_name}, {match_type_name}, {match_name}")
                        team_b_abbriev = team_b


                if team_a_abbriev not in team_mapping:
                    team_mapping[team_a_abbriev] = team_a
                
                if team_b_abbriev not in team_mapping:
                    team_mapping[team_b_abbriev] = team_b
                games_id = {}
                try:
                    games_id_divs = match_soup.find("div", class_="vm-stats-gamesnav").find_all("div")
                    extract_games_id(games_id_divs, games_id, results, [tournament_name, stage_name, match_type_name, match_name, tournament_id, stage_id, match_id])
                except AttributeError: #only 1 map played
                    map_name = overview_stats[0].find("div", class_="map").text.strip().split("\t")[0]
                    if not map_name or map_name == "TBD":
                        map_name = pd.NA
                    id = overview_stats[0].get("data-game-id")
                    games_id[id] = map_name
                    results["maps_played"].append([tournament_name, stage_name, match_type_name, match_name, map_name])
                    results["tournaments_stages_matches_games_ids"].append([tournament_name, tournament_id, stage_name, stage_id, match_type_name, match_name, match_id, map_name, id])
                # print(games_id)
                maps_notes = match_soup.find_all("div", class_="match-header-note")
                extract_maps_notes(maps_notes, results, team_mapping, [tournament_name, stage_name, match_type_name, match_name])
                
                extract_methods(overview_stats, games_id, results, [tournament_name, stage_name, match_type_name, match_name, team_a, team_b])


                maps_headers = match_soup.find_all("div", class_="vm-stats-game-header")
                extract_maps_headers(maps_headers, results, [tournament_name, stage_name, match_type_name, match_name, team_a, team_b])

                player_to_team = extract_overview_stats(overview_stats, games_id, team_mapping, results, [tournament_name, stage_name, match_type_name, match_name, team_a, team_b])
            except IndexError:
                print(f"ERROR FROM SCRAPING OVERVIEW PAGE")
                print(f"{tournament_name}, {stage_name}, {match_type_name}, {match_name}, the match was forfeited")
                traceback_info = traceback.format_exc()
                print(traceback_info)
                return {}

            await asyncio.sleep(random.uniform(1,2))

            try:
                performance_page = await fetch(f'https://vlr.gg{url}/?game=all&tab=performance', session)
            except MaxReentriesReached as e:
                print(f"Error: {e}")
                sys.exit(1)
            performance_soup = BeautifulSoup(performance_page, "html.parser")
            performance_stats_div = performance_soup.find_all("div", class_="vm-stats-game")

            extract_kills_stats(performance_stats_div, games_id, team_mapping, player_to_team, results, [tournament_name, stage_name, match_type_name, match_name, team_a, team_b])

            await asyncio.sleep(random.uniform(1,2))
                
            try:
                economy_page = await fetch(f'https://vlr.gg{url}/?game=all&tab=economy', session)
            except MaxReentriesReached as e:
                print(f"Error: {e}")
                sys.exit(1)
            economy_soup = BeautifulSoup(economy_page, "html.parser")

            economy_stats_div = economy_soup.find_all("div", class_="vm-stats-game")

            eco_stats, eco_rounds_stats = extract_economy_stats_div(economy_stats_div)
            extract_economy_stats(eco_stats, eco_rounds_stats, games_id, team_mapping, results, [tournament_name, stage_name, match_type_name, match_name, team_a, team_b])

        results["team_mapping"] = team_mapping
        await asyncio.sleep(random.uniform(1,2))
        return results



async def scraping_matches_data(tournament_name, cards, tournaments_ids, stages_ids, semaphore, session):
    # card_semaphore = asyncio.Semaphore(semaphore_count)
    tasks = [scraping_card_data(tournament_name, card, tournaments_ids, stages_ids, session, semaphore) for card in cards]
    results = await asyncio.gather(*tasks)
    return results


async def scraping_agents_data_match_type_helper(tournament_name, stage_name, match_type_name, url, semaphore, session):
    async with semaphore:
        global_table_titles = ["Map", "Total Played", "Attacker Side Win", "Defender Side Win"]
        result = {}
        tournament_dict = result.setdefault(tournament_name, {})
        stage_dict = tournament_dict.setdefault(stage_name, {})
        match_type_dict = stage_dict.setdefault(match_type_name, {})
        maps_stats_dict = match_type_dict.setdefault("Maps Stats", {})
        agents_pick_rates_dict = match_type_dict.setdefault("Agents Pick Rates", {})
        teams_pick_rates_dict = match_type_dict.setdefault("Teams Pick Rates", {})
        print(f"Collecting data for {tournament_name}, {stage_name}, {match_type_name}")
        try:
            page = await fetch(url, session)
        except MaxReentriesReached as e:
            print(f"Error: {e}")
            sys.exit(1)
        soup = BeautifulSoup(page, "html.parser")
        global_maps_table = soup.find("table", class_="wf-table mod-pr-global")
        agent_pictures = global_maps_table.find_all("th", style=" vertical-align: middle; padding-top: 0; padding-bottom: 0; width: 65px;")

        extract_agent_pictures(agent_pictures, global_table_titles)
        table_stats_tr = global_maps_table.find_all("tr")[1:]
        extract_pick_rates(table_stats_tr, maps_stats_dict, agents_pick_rates_dict, global_table_titles)

        
        teams_tables = soup.select('table.wf-table:not([class*=" "])')
        table_titles = ["", ""] + global_table_titles[4:]

        extract_team_picked_agents(teams_tables, teams_pick_rates_dict, table_titles)
                
        return result


async def scraping_agents_data_stage_helper(tournament_name, stage_name, match_types, semaphore, session):
    tasks = [scraping_agents_data_match_type_helper(tournament_name, stage_name, match_type_name, url, semaphore, session) for match_type_name, url in match_types.items()]
    results = await asyncio.gather(*tasks)
    return results

async def scraping_agents_data(tournament_name, stages, semaphore, session): 
    tasks = [scraping_agents_data_stage_helper(tournament_name, stage_name, match_types, semaphore, session) for stage_name, match_types in stages.items()]
    results = await asyncio.gather(*tasks)
    return results



# async def processing_each_agents(match_type_dict, global_players_agents, players_agents, tournament_name, stage_name, match_type_name, url, df, agent, session ):
#     async with asyncio.Semaphore(2):
#         print(f"Collecting data for {agent} {tournament_name}, {stage_name}, {match_type_name}")
#         try:
#             page = await fetch(f"{url}&min_rounds=0&agent={agent}", session)
#         except MaxReentriesReached as e:
#             print(f"Error: {e}")
#             sys.exit(1)
#         await asyncio.sleep(random.uniform(0, 1))
#         soup = BeautifulSoup(page, "html.parser")
#         stats_trs = soup.find_all("tr")[1:]

#         if len(stats_trs) >= 1 and stats_trs[0].find("td"):
#             extract_players_stats(stats_trs, match_type_dict, global_players_agents, players_agents, df, tournament_name, stage_name, match_type_name, agent)

async def scraping_player_stats_match_type_helper(tournament_name, stage_name, match_type_name, url, df, semaphore, session):
    async with semaphore:
        result = {}
        global_players_agents = {}
        tournament_dict = result.setdefault(tournament_name, {})
        stage_dict = tournament_dict.setdefault(stage_name, {})
        match_type_dict = stage_dict.setdefault(match_type_name, {})
        players_agents = {}
        # tasks = [processing_each_agents(match_type_dict, global_players_agents, players_agents, tournament_name, stage_name,
        #                             match_type_name, url, df, agent, session) for agent in all_agents[:-1]]
        # await asyncio.gather(*tasks)

        # await processing_each_agents(match_type_dict, global_players_agents, players_agents, tournament_name, stage_name,
        #                             match_type_name, url, df, all_agents[-1], session)
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

            if len(stats_trs) == 1 and not stats_trs[0].find("td"):
                continue

            extract_players_stats(stats_trs, match_type_dict, global_players_agents, players_agents, df, tournament_name, stage_name, match_type_name, agent)
        return result

async def scraping_player_stats_stage_helper(tournament_name, stage_name, match_types, df, semaphore, session):
    tasks = [scraping_player_stats_match_type_helper(tournament_name, stage_name, match_type_name, url, df, semaphore, session) for match_type_name, url in match_types.items()]
    results = await asyncio.gather(*tasks)
    return results

async def scraping_players_stats(tournament_name, stages, df, semaphore, session):
    tasks = [scraping_player_stats_stage_helper(tournament_name, stage_name, match_types, df, semaphore, session) for stage_name, match_types in stages.items()]
    results = await asyncio.gather(*tasks)
    return results

