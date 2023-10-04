import requests
from bs4 import BeautifulSoup, Tag, NavigableString
import re
import time

def remove_special_characters(input_string):
    pattern = r'[^a-zA-Z0-9_/]+'
    
    # Use the sub() method to replace all matched characters with an empty string
    cleaned_string = re.sub(pattern, '', input_string)
    return cleaned_string

url = "https://www.vlr.gg/event/matches/1188/champions-tour-2023-lock-in-s-o-paulo"

matches_url = {}
matches_stats = {}
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
    loser, loser_flag, loser_score = card.find("div", class_="match-item-vs").find("div", class_="match-item-vs-team").find_all("div")
    loser = remove_special_characters(loser.text)
    loser_score = remove_special_characters(loser_score.text)
    winner, winner_flag, winner_score = card.find("div", class_="match-item-vs").find("div", class_="match-item-vs-team mod-winner").find_all("div")
    winner = remove_special_characters(winner.text)
    winner_score = remove_special_characters(winner_score.text)
    # test, test1, test2 = card.find("div", class_="match-item-vs").find("div", class_="match-item-vs-team").find_all("div")
    match_type = card.find("div", class_="match-item-event text-of").text
    print(winner_score)
