import requests
from bs4 import BeautifulSoup, Tag
import re

url = f"https://www.vlr.gg/event/stats/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16333.16334.16335.16336.16337&min_rounds=0&agent=all"
page = requests.get(url)

soup = BeautifulSoup(page.content, "html.parser")

# select_element = soup.find("select", {"name": "agent"})

# all_agents = select_element.find_all('option')[1:]

table = soup.find("table", {"class": "wf-table mod-stats mod-scroll"})

# print(table)

all_agents = set()

all_agents_img = soup.find_all("td", {"class": "mod-agents"})

# print(all_agents_img)

pattern = r'/(\w+)\.png'

for td in all_agents_img:
    file_name = td.find("div").find("img").get("src")
    match = re.search(pattern, file_name)
    agent_name = match.group(1)
    all_agents.add(agent_name)

all_agents = sorted(list(all_agents))

# print(all_agents)


all_ths = soup.find_all("th")[2:]

stats_titles = ["", ""]

for th in all_ths:
    title = th.get("title")
    stats_titles.append(title)

# print(stats_titles)


all_trs = soup.find_all("tr")[1:]

for tr in all_trs[0]:
    player_name = ""
    player_team = ""
    agents_played = []
    stats = {}
    for td in tr:
        print(td)
        td_class = td.get("class")
        print(td_class)
        if "mod-player mod-a" in td_class:
            player_name, player_team = td.find("div")
            player_name, player_team = player_name.text, player_team.text
            print(player_name, player_team)

        # elif "mod-agents" in td_class:
        # elif ["mod-rmd", "mod-color-sq", "mod-color-sq mod-acs", "mod-cl", "mod-a mod-kmax"] in td_class or not td_class:
        

