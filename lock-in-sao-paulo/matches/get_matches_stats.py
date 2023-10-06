import requests
from bs4 import BeautifulSoup, Tag, NavigableString
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
        print(match_soup)
        if not stats_titles:
            stats_titles = ["", ""]

            all_ths = match_soup.find_all("th")[2:]
            for th in all_ths:
                title = th.get("title")
                stats_titles.append(title)
            break

print(stats_titles)
    