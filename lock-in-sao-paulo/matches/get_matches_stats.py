import requests
from bs4 import BeautifulSoup, Tag, NavigableString, Comment
import re
import time

def remove_special_characters(input_string, pattern):
    
    # Use the sub() method to replace all matched characters with an empty string
    cleaned_string = re.sub(pattern, '', input_string)
    return cleaned_string

url = "https://www.vlr.gg/event/matches/1188/champions-tour-2023-lock-in-s-o-paulo"

matches_url = {}
matches_stats = {}
tournament = "Lock-In Sao Paulo"
matches_stats[tournament] = {}
pattern = r'[^a-zA-Z0-9_/]+'
page = requests.get(url)

soup = BeautifulSoup(page.content, "html.parser")

all_cards = soup.select('div.wf-card:not([class*=" "])')

modules = []

for cards in all_cards:
    all_modules = cards.find_all("a")
    modules.extend(all_modules)
# for card in cards[::-1]:
#     match_type = remove_special_characters(card.find_all("div")[-1].text)
#     print(match_type)
#     if match_type == "Showmatch":
#         cards.remove(card)
#         break

#it is only getting the first card
stats_titles = []
all, attack, defend = "all", "attack", "defend"
overview, performance, economy = "overview", "performance", "economy"

for index, module in enumerate(modules):
    match_type, stage = module.find("div", class_="match-item-event text-of").text.strip().splitlines()
    match_type = match_type.strip("\t")
    stage = stage.strip("\t")
    if match_type == "Showmatch":
        continue
    else:
        loser, loser_flag, loser_score = module.find("div", class_="match-item-vs").select('div.match-item-vs-team:not([class*=" "])')[0].find_all("div")
        loser = loser.text.strip("\n").strip("\t")
        loser_score = loser_score.text.strip("\n").strip("\t")

        winner, winner_flag, winner_score = module.find("div", class_="match-item-vs").find("div", class_="match-item-vs-team mod-winner").find_all("div")
        winner = winner.text.strip("\n").strip("\t")
        winner_score = winner_score.text.strip("\n").strip("\t")

        match_name = f"{loser} vs {winner}"

        stage_dict = matches_stats[tournament].setdefault(stage, {})

        match_type_dict = stage_dict.setdefault(match_type, {})
        match_type_dict[match_name] = {}

        url = module.get("href")
        match_page = requests.get(f'https://vlr.gg{url}')
        match_soup = BeautifulSoup(match_page.content, "html.parser")

        if not stats_titles:
            stats_titles = ["", ""]

            all_ths = match_soup.find("tr").find_all("th")[2:]
            for th in all_ths:
                title = th.get("title")
                stats_titles.append(title)
        
        match_maps = match_soup.find("div", class_="vm-stats-gamesnav noselect").select('div[data-disabled="0"]')
        for index, map in enumerate(match_maps):
            # map = re.sub(r'\d+|\t|\n', '',map.text.strip())
            comments = map.find(string=lambda text: isinstance(text, Comment))
            comment_soup = BeautifulSoup(comments, "html.parser")
            who_picked = comment_soup.find("div", class_="pick ge-text-light").text.strip()
            if who_picked:
                who_picked = who_picked.split(":")[1].strip()
            # print(who_picked)
            match_maps[index] = (re.sub(r'\d+|\t|\n', '',map.text.strip()), who_picked)
        
        maps_id = {}
        
        maps_id_divs = match_soup.find("div", class_="vm-stats-gamesnav noselect").find_all("div")
        for div in maps_id_divs:
            id = div.get("data-game-id")
            print(id)
            map = re.sub(r"\d+|\t|\n", "", div.text.strip())
            maps_id[id] = map
        overview_stats = match_soup.find_all("div", class_="vm-stats-game")
        overview_stats[0], overview_stats[1] = overview_stats[1], overview_stats[0]
        trs = []
        # for index, main_div in enumerate(overview_stats):
        #     team_1_table, team_2_table = main_div.find_all("div")

        print(len(trs))
        
        # trs = match_soup.find_all("tr")
        # for tr in trs:
        #     for td in tr:
        #         player = ""
        #         team = ""
        #         agents = ""
        #         try:
        #             td_class = td.get('class') or ""
        #             class_name = " ".join(td_class)
        #             # print(class_name)
        #             if class_name == "mod-player":
        #                 player_info = td.find("div").find("a").find_all("div")
        #                 player = player_info[0].text
        #                 team = player_info[1].text
        #                 match_type_dict[match_name][] = {} 
        #             elif class_name == "mod-agents":
        #                 imgs = td.find("div").find_all("img")
        #                 agents_played = []
        #                 for img in imgs:
        #                     file_name = img.get("src")
        #                     match = re.search(pattern, file_name)
        #                     agent_name = match.group(1)
        #                     agents_played.append(agent_name)
        #                 if len(agents_played) == 1:
        #                     agents = agents_played[0]
        #                     players_with_one_agent_played.add(player)
        #                     players_stats[stage][player][agents] = {}
        #                     players_stats[stage][player][agents]["team"] = team
        #                 else:
        #                     agents = "multiple agents"
        #                     players_stats[stage][player][agents] = {}
        #                     players_stats[stage][player][agents]["team"] = team
        #             elif class_name == "mod-rnd" or class_name == "mod-cl" or class_name == "":
        #                 stat = remove_special_characters(td.text)
        #                 stat_name = stats_titles[index]
        #                 players_stats[stage][player][agents][stat_name] = stat
        #             elif class_name == "mod-color-sq mod-acs" or class_name ==  "mod-color-sq":
        #                 stat = td.find("div").find("span").text
        #                 stat_name = stats_titles[index]
        #                 players_stats[stage][player][agents][stat_name] = stat
        #             elif class_name == "mod-a mod-kmax":
        #                 stat = remove_special_characters(td.find("a").text)
        #                 stat_name = stats_titles[index]
        #                 players_stats[stage][player][agents][stat_name] = stat
        #         except AttributeError:
        #             continue
        break
print(match_maps)

    