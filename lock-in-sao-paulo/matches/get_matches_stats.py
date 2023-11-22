import requests
from bs4 import BeautifulSoup, Tag, NavigableString, Comment
import re
import time
import pprint
import csv
from datetime import datetime
import pandas as pd
import traceback
start_time = time.time()

now = datetime.now()

current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)

url = "https://www.vlr.gg/vct-2023"
page = requests.get(url)
soup = BeautifulSoup(page.content, "html.parser")

urls = {}

tournament_cards = soup.find_all("a", class_="wf-card mod-flex event-item")


for card in tournament_cards:
    href = card.get("href")
    matches_url = "https://www.vlr.gg" + href.replace("/event/", "/event/matches/")
    tournament = card.find("div", class_="event-item-title").text.strip().split(": ")
    if len(tournament) == 2:
        tournament_name = tournament[1]
    else:
        tournament_name = tournament[0]
    if tournament_name == "LOCK//IN São Paulo":
        tournament_name = "Lock-In Sao Paulo"

    urls[tournament_name] = matches_url

matches_cards = {}

for tournament, url in urls.items():
    page = requests.get(url)

    soup = BeautifulSoup(page.content, "html.parser")

    all_cards = soup.select('div.wf-card:not([class*=" "])')
    modules = []
    # print(tournament ,len(all_cards))
    for cards in all_cards:
        all_modules = cards.find_all("a")
        modules.extend(all_modules)
    matches_cards[tournament] = modules


matches_stats = {}
team_mapping = {}
overview_stats_titles = ["", "", "Rating", "Average Combat Score", "Kills", "Deaths", "Assists", "Kills - Deaths (KD)",
                         "Kill, Assist, Trade, Survive %", "Average Damage per Round", "Headshot %", "First Kills",
                         "First Deaths", "Kills - Deaths (FKD)"]
performance_stats_title = ["", "", "2k", "3k", "4k", "5k", "1v1", "1v2", "1v3", "1v4", "1v5", "Economy", "Spike Plants", "Spike Defuse"]
economy_stats_title = ["Pistol Won", "Eco (won)", "$ (won)", "$$ (won)", "$$$ (won)"]
overview, performance, economy = "Overview", "Performance", "Economy"
specific_kills_name = ["All Kills", "First Kills", "Op Kills"]
eco_types = {"": "Eco: 0-5k", "$": "Semi-eco: 5-10k", "$$": "Semi-buy: 10-20k", "$$$": "Full buy: 20k+"}

scores_df = pd.DataFrame(columns=["Tournament", "Stage", "Match Type", "Winner", "Loser", "Winner's Score", "Loser's Score"])
draft_phase_df = pd.DataFrame(columns=["Tournament", "Stage", "Match Type", "Team", "Action", "Map"])
overview_df = pd.DataFrame(columns=["Tournament", "Stage", "Match Type", "Player", "Team", "Agents", "Rating", "Average Combat Score",
                     "Kills", "Deaths", "Assists", "Kill - Deaths (KD)", "Kill, Assist, Trade, Survive %", "Average Damage per Round",
                     "Headshot %", "First Kills", "First Deaths", "Kills - Deaths (FKD)", "Side"])
kills_df = pd.DataFrame(columns=["Tournament", "Stage", "Match Type", "Map", "Player's Team", "Player", "Enemy's Team",
                        "Enemy", "Player's Kills", "Enemy's Kills", "Difference", "Kill Type"])
kills_stats_df = pd.DataFrame(columns=["Tournament", "Stage", "Match Type", "Map", "Team", "Player", "Agent", "2K", "3k", "4k", "5k", "1v1",
                                 "1v2", "1v3", "1v4", "1v5", "Econ", "Spike Plants", "Spike Defuse"])
rounds_kills_df = pd.DataFrame(columns=["Tournament", "Stage", "Match Type", "Map", "Round Number", "Eliminator's Team", "Eliminator", "Eliminator's Agent",
                                    "Eliminated Team", "Eliminated", "Eliminated's Agent", "Kill Type"])
eco_stats_df = pd.DataFrame(columns=["Tournament", "Stage", "Match Type", "Map", "Team", "Type", "Initiated", "Won"])
eco_rounds_df = pd.DataFrame(columns=["Tournament", "Stage", "Match Type", "Map", "Round Number", "Team", "Credits", "Type", "Outcome"])

for tournament_name, cards in matches_cards.items():
    for module in cards:
        match_type_name, stage_name = module.find("div", class_="match-item-event text-of").text.strip().splitlines()
        match_type_name = match_type_name.strip("\t")
        stage_name = stage_name.strip("\t")
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

            scores_df.loc[len(scores_df)] = [tournament_name, stage_name, match_type_name, winner, loser, winner_score, loser_score]

            print("Starting collecting for ", tournament_name, stage_name, match_type_name, match_name)
            url = module.get("href")
            match_page = requests.get(f'https://vlr.gg{url}', timeout=10)
            match_soup = BeautifulSoup(match_page.content, "html.parser")

            maps_id = {}
            
            maps_id_divs = match_soup.find("div", class_="vm-stats-gamesnav").find_all("div")
            for div in maps_id_divs:
                if div.get("data-game-id"):
                    id = div.get("data-game-id")
                else:
                    id = ""
                map = re.sub(r"\d+|\t|\n", "", div.text.strip())
                maps_id[id] = map

            
            overview_stats = match_soup.find_all("div", class_="vm-stats-game")

            overview_tables = overview_stats[0].find_all("table")

            team_a_abbriev = overview_tables[0].find("tbody").find("tr").find("td").find("a").find_all("div")[-1].text.strip()

            team_b_abbriev = overview_tables[1].find("tbody").find("tr").find("td").find("a").find_all("div")[-1].text.strip()

            if team_a not in team_mapping:
                team_mapping[team_a_abbriev] = team_a
            
            if team_b not in team_mapping:
                team_mapping[team_b_abbriev] = team_b

            maps_notes = match_soup.find("div", class_="match-header-note").text.strip().split("; ")
            if maps_notes:
                for note in maps_notes:
                    if "ban" in note or "pick" in note:
                        team, action, map = note.split()
                        team = team_mapping[team]
                        draft_phase_df.loc[len(draft_phase_df)] = [tournament_name, stage_name, match_type_name, team, action, map]

            for stats in overview_stats:
                id = stats.get("data-game-id")
                map = maps_id[id]

                stats_tables = stats.find_all("table")
                for table in stats_tables:
                    trs = table.find("tbody").find_all("tr")
                    for tr in trs:
                        tds = tr.find_all("td")
                        values = [[tournament_name, stage_name, match_type_name, map],
                                 [tournament_name, stage_name, match_type_name, map],
                                 [tournament_name, stage_name, match_type_name, map]]
                        for index, td in enumerate(tds):
                            td_class = td.get("class") or ""
                            class_name = " ".join(td_class)
                            if class_name == "mod-player":
                                player, team = td.find("a").find_all("div")
                                player, team =  player.text.strip(), team.text.strip()
                                team = team_mapping[team]
                                for list in values:
                                    list.append(player)
                                    list.append(team)
                            elif class_name == "mod-agents":
                                imgs = td.find_all("img")
                                agents_played = []
                                for img in imgs:
                                    agent = img.get("alt")
                                    agents_played.append(agent)
                                agents = ", ".join(agents_played)
                                for list in values:
                                    list.append(agents)
                            elif class_name in ["mod-stat mod-vlr-kills", "mod-stat", "mod-stat mod-vlr-assists", "mod-stat mod-kd-diff",
                                                "mod-stat mod-fb", "mod-stat mod-fd", "mod-stat mod-fk-diff"]:
                                stats = td.find("span").find_all("span")
                                if len(stats) == 3:
                                    all_stat, attack_stat, defend_stat = stats
                                    all_stat, attack_stat, defend_stat = all_stat.text.strip(), attack_stat.text.strip(), defend_stat.text.strip()
                                    stat_name = overview_stats_titles[index % len(overview_stats_titles)]
                                    if not all_stat and not attack_stat and not defend_stat:
                                        all_stat, attack_stat, defend_stat = "-1", "-1", "-1"
                                    values[0].append(all_stat)
                                    values[1].append(attack_stat)
                                    values[2].append(defend_stat)
                                else:
                                    all_stat = stats[0]
                                    all_stat = all_stat.text.strip()
                                    stat_name = overview_stats_titles[index % len(overview_stats_titles)]
                                    values[0].append(all_stat)
                                    values[1].append(-1)
                                    values[2].append(-1)
                            elif class_name == "mod-stat mod-vlr-deaths":
                                stats = td.find("span").find_all("span")[1].find_all("span")
                                if len(stats) == 3:
                                    all_stat, attack_stat, defend_stat = td.find("span").find_all("span")[1].find_all("span")
                                    all_stat, attack_stat, defend_stat = all_stat.text.strip(), attack_stat.text.strip(), defend_stat.text.strip()
                                    stat_name = overview_stats_titles[index % len(overview_stats_titles)]
                                    values[0].append(all_stat)
                                    values[1].append(attack_stat)
                                    values[2].append(defend_stat)
                                else:
                                    all_stat = stats[0]
                                    all_stat = all_stat.text.strip()
                                    stat_name = overview_stats_titles[index % len(overview_stats_titles)]
                                    values[0].append(all_stat)
                                    values[1].append(-1)
                                    values[2].append(-1)

                        overview_df.loc[len(overview_df)] = values[0]
                        overview_df.loc[len(overview_df)] = values[1]
                        overview_df.loc[len(overview_df)] = values[2]

            performance_page = requests.get(f'https://vlr.gg{url}/?game=all&tab=performance', timeout=10)
            performance_soup = BeautifulSoup(performance_page.content, "html.parser")
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
                                    player_a_kills, player_b_kills, difference = "-1", "-1" , "-1"
                                kills_df.loc[len(kills_df)] = [tournament_name, stage_name, match_type_name, map, team, player, team_b, player_b, player_a_kills, player_b_kills, difference, kill_name]

                for id, tds_lists in players_kills.items():
                    map = maps_id[id]
                    for tds in tds_lists:
                        values = [tournament_name, stage_name, match_type_name, map]
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
                                                rounds_kills_df.loc[len(rounds_kills_df)] = [tournament_name, stage_name, match_type_name, map, round_stat, team, player, agent,
                                                                       team, victim, agent, stat_name]

                            else:
                                stat = td.text.strip()
                                stat_name = performance_stats_title[index % len(performance_stats_title)]
                                if not stat:
                                    stat = "-1"
                                values.append(stat)
                        kills_stats_df.loc[len(kills_stats_df)] = values

            except Exception as e:
                print(tournament_name, stage_name, match_type_name, match_name, "does not contain any data under their performance page")

            
            economy_page = requests.get(f'https://vlr.gg{url}/?game=all&tab=economy', timeout=10)
            economy_soup = BeautifulSoup(economy_page.content, "html.parser")

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
                                initiated, won = "-1", stats[0]
                            stat_name = economy_stats_title[index % len(economy_stats_title)]
                            eco_stats_df.loc[len(eco_stats_df)] = [tournament_name, stage_name, match_type_name, map, team, stat_name, initiated, won]

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
                            eco_rounds_df.loc[len(eco_rounds_df)] = [tournament_name, stage_name, match_type_name, map, round, team_a, team_a_bank, team_a_eco_type, team_a_outcome]
                            eco_rounds_df.loc[len(eco_rounds_df)] = [tournament_name, stage_name, match_type_name, map, round, team_b, team_b_bank, team_b_eco_type, team_b_outcome]
                        
            else:
                print(tournament_name, stage_name, match_type_name, match_name, "does not contain any data under their economy page")
    time.sleep(0.1)

end_time = time.time()

print(f"Datascraping time: {end_time - start_time} seconds")


start_time = time.time()

sides = ["all", "attack", "defend"]

# print(scores_df)
# print(draft_phase_df)
# print(overview_df)
# print(kills_df)
# print(kills_stats_df)
# print(rounds_kills_df)
# print(eco_stats_df)
# print(eco_rounds_df)

scores_df.to_csv("scores.csv", encoding="utf-8", index=False)
draft_phase_df.to_csv("draft_phase.csv", encoding="utf-8", index=False)
overview_df.to_csv("overview.csv", encoding="utf-8", index=False)
kills_df.to_csv("kills.csv", encoding="utf-8", index=False)
kills_stats_df.to_csv("kills_stats.csv", encoding="utf-8", index=False)
rounds_kills_df.to_csv("rounds_kills.csv", encoding="utf-8", index=False)
eco_stats_df.to_csv("eco_stats.csv", encoding="utf-8", index=False)
eco_rounds_df.to_csv("eco_rounds.csv", encoding="utf-8", index=False)


end_time = time.time()

print(f"Data to CSV time: {end_time - start_time} seconds")

now = datetime.now()

current_time = now.strftime("%H:%M:%S")
print("End Time =", current_time)