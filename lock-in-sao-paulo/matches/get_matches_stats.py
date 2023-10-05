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

cards = soup.find_all("a", class_="wf-module-item match-item mod-color mod-left mod-bg-after-striped_redyellow mod-first")

# for card in cards[::-1]:
#     match_type = remove_special_characters(card.find_all("div")[-1].text)
#     print(match_type)
#     if match_type == "Showmatch":
#         cards.remove(card)
#         break



for card in cards:
    match_type, stage = card.find("div", class_="match-item-event text-of").text.strip().splitlines()
    match_type = match_type.strip("\t")
    stage = stage.strip("\t")
    if match_type == "Showmatch":
        continue
    else:
        loser, loser_flag, loser_score = card.find("div", class_="match-item-vs").find("div", class_="match-item-vs-team").find_all("div")
        # loser = remove_special_characters(input_string=loser.text, pattern=r'[^a-zA-Z0-9_/]+')
        loser = loser.text.strip("\n").strip("\t")
        loser_score = loser_score.text.strip("\n").strip("\t")
        # loser_score = remove_special_characters(input_string=loser_score.text, pattern=r'[^a-zA-Z0-9_/]+')
        winner, winner_flag, winner_score = card.find("div", class_="match-item-vs").find("div", class_="match-item-vs-team mod-winner").find_all("div")
        winner = winner.text.strip("\n").strip("\t")
        winner_score = winner_score.text.strip("\n").strip("\t")
        # winner = remove_special_characters(input_string=winner.text, pattern=r'[^a-zA-Z0-9_/]+')
        # winner_score = remove_special_characters(input_string=winner_score.text, pattern=r'[^a-zA-Z0-9_/]+')
        # test, test1, test2 = card.find("div", class_="match-item-vs").find("div", class_="match-item-vs-team").find_all("div")
        match_name = f"{loser} vs {winner}"
        # stage_dict = matches_stats[tournament].get(stage, {})
        # matches_stats[tournament] = stage_dict
        stage_dict = matches_stats[tournament].setdefault(stage, {})
        match_type_dict = stage_dict.setdefault(match_type, {})

        # match_type_dict = stage_dict.get(match_type, {})
        # stage_dict[match_type] = match_type_dict

print(matches_stats)
    