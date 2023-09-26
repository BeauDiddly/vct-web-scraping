import requests
from bs4 import BeautifulSoup, Tag, NavigableString
import re

def remove_special_characters(input_string):
    pattern = r'[^a-zA-Z0-9]+'
    
    # Use the sub() method to replace all matched characters with an empty string
    cleaned_string = re.sub(pattern, '', input_string)
    return cleaned_string
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


for tr in all_trs:
    for td in tr:
        if not isinstance(td, Tag) and isinstance(td, NavigableString):
            td.extract()
# print(len(all_trs))

# print(len(all_trs[0:2]))

pattern = r'/(\w+)\.png'

for index,td in enumerate(all_trs[0]):
    try:
        td_class = td.get('class') or ""
        class_name = " ".join(td_class)
        if class_name == "mod-player mod-a":
            player_info = td.find("div").find_all("div")
            name = player_info[0].text
            team = player_info[1].text 
            print(name, team, index)
        elif class_name == "mod-agents":
            file_name = td.find("div").find("img").get("src")
            match = re.search(pattern, file_name)
            agent_name = match.group(1)
            print(agent_name, index)
        elif class_name == "mod-rnd" or class_name == "mod-cl" or class_name == "":
            stat = remove_special_characters(td.text)
            print(stat, index)
        elif class_name == "mod-color-sq mod-acs" or class_name ==  "mod-color-sq":
            stat = td.find("div").find("span").text
            print(stat, index)
        elif class_name == "mod-a mod-kmax":
            stat = remove_special_characters(td.find("a").text)
            print(stat, index)
    except AttributeError:
        continue
    # for index,td in enumerate(tr):
    #     # print(td)
    #     if isinstance(td, Tag) and td.name == 'a':
    #         td_class = td.get('class')
    #         print(td_class)
        # print(td)
        # try:
        #     td_class = td.get("class") if td.get("class") else ""
        #     print(td_class)
        #     # print(td)
        #     if "mod-player mod-a" in td_class:
        #         player_name, player_team = td.find("div")
        #         player_name, player_team = player_name.text, player_team.text
        #         print(player_name, player_team)
        #     else:
        #         continue

        # elif "mod-agents" in td_class:
        # elif ["mod-rmd", "mod-color-sq", "mod-color-sq mod-acs", "mod-cl", "mod-a mod-kmax"] in td_class or not td_class:
        
        # except AttributeError:
        #     continue

