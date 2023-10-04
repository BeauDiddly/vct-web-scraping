import requests
from bs4 import BeautifulSoup, Tag, NavigableString
import re
import time

def remove_special_characters(input_string):
    pattern = r'[^a-zA-Z0-9_/]+'
    
    # Use the sub() method to replace all matched characters with an empty string
    cleaned_string = re.sub(pattern, '', input_string)
    return cleaned_string

def remove_empty_strings(all_trs):
    for tr in all_trs:
        for td in tr:
            if not isinstance(td, Tag) and isinstance(td, NavigableString):
                td.extract()

urls = {"all_stages": "https://www.vlr.gg/event/stats/1188/champions-tour-2023-lock-in-s-o-paulo",
        "semi_finals": "https://www.vlr.gg/event/stats/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16339.16332.16333.16334.16335.16336.16337&min_rounds=0&agent=all",
        "grand_finals": "https://www.vlr.gg/event/stats/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16332.16333.16334.16335.16336.16337&min_rounds=0&agent=all",
        "playoffs": "https://www.vlr.gg/event/stats/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16332.16333.16334.16335.16336.16337&min_rounds=0&agent=all",
        "alpha_round_of_sixteen": "https://www.vlr.gg/event/stats/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16333.16334.16335.16336.16337&min_rounds=0&agent=all",
        "alpha_quarterfinals": "https://www.vlr.gg/event/stats/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16334.16335.16336.16337&min_rounds=0&agent=all",
        "alpha_semifinals": "https://www.vlr.gg/event/stats/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16335.16336.16337&min_rounds=0&agent=all",
        "alpha": "https://www.vlr.gg/event/stats/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16335.16336.16337&min_rounds=0&agent=all",
        "omega_round_of_sixteen": "https://www.vlr.gg/event/stats/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334.16336.16337&min_rounds=0&agent=all",
        "omega_quarterfinals": "https://www.vlr.gg/event/stats/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334.16335.16337&min_rounds=0&agent=all",
        "omega_semifinals": "https://www.vlr.gg/event/stats/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334.16335.16336&min_rounds=0&agent=all",
        "omega": "https://www.vlr.gg/event/stats/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334&min_rounds=0&agent=all",
        "bracket_stage": "https://www.vlr.gg/event/stats/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339&min_rounds=0&agent=all"}


# select_element = soup.find("select", {"name": "agent"})

# all_agents = select_element.find_all('option')[1:]


# print(table)


# print(all_agents_img)

pattern = r'/(\w+)\.png'




# print(all_agents)

# print(stats_titles)


# print(len(all_trs))

# print(len(all_trs[0:2]))

players_stats = {}

players_with_one_agent_played = set()

stats_titles = []

for stage, url in urls.items():
    page = requests.get(url)

    soup = BeautifulSoup(page.content, "html.parser")

    # select_element = soup.find("select", {"name": "agent"})

    # all_agents = select_element.find_all('option')[1:]

    table = soup.find("table", {"class": "wf-table mod-stats mod-scroll"})

    all_agents = set()
    all_agents_img = soup.find_all("td", {"class": "mod-agents"})
    for td in all_agents_img:
        file_name = td.find("div").find("img").get("src")
        match = re.search(pattern, file_name)
        agent_name = match.group(1)
        all_agents.add(agent_name)
    all_agents = sorted(list(all_agents))
    
    if not stats_titles:
        stats_titles = ["", ""]

        all_ths = soup.find_all("th")[2:]

        for th in all_ths:
            title = th.get("title")
            stats_titles.append(title)
    
    players_stats[stage] = {}
    all_trs = soup.find_all("tr")[1:]

    remove_empty_strings(all_trs)
    
    for tr in all_trs:
        player = ""
        team = ""
        agents = ""
        for index, td in enumerate(tr):
            # print(index)
            try:
                td_class = td.get('class') or ""
                class_name = " ".join(td_class)
                # print(class_name)
                if class_name == "mod-player mod-a":
                    player_info = td.find("div").find_all("div")
                    player = player_info[0].text
                    team = player_info[1].text
                    players_stats[stage][player] = {} 
                elif class_name == "mod-agents":
                    imgs = td.find("div").find_all("img")
                    agents_played = []
                    for img in imgs:
                        file_name = img.get("src")
                        match = re.search(pattern, file_name)
                        agent_name = match.group(1)
                        agents_played.append(agent_name)
                    if len(agents_played) == 1:
                        agents = agents_played[0]
                        players_with_one_agent_played.add(player)
                        players_stats[stage][player][agents] = {}
                        players_stats[stage][player][agents]["team"] = team
                    else:
                        agents = "multiple agents"
                        players_stats[stage][player][agents] = {}
                        players_stats[stage][player][agents]["team"] = team
                elif class_name == "mod-rnd" or class_name == "mod-cl" or class_name == "":
                    stat = remove_special_characters(td.text)
                    stat_name = stats_titles[index]
                    players_stats[stage][player][agents][stat_name] = stat
                elif class_name == "mod-color-sq mod-acs" or class_name ==  "mod-color-sq":
                    stat = td.find("div").find("span").text
                    stat_name = stats_titles[index]
                    players_stats[stage][player][agents][stat_name] = stat
                elif class_name == "mod-a mod-kmax":
                    stat = remove_special_characters(td.find("a").text)
                    stat_name = stats_titles[index]
                    players_stats[stage][player][agents][stat_name] = stat
            except AttributeError:
                continue
        # print(players_stats[stage][player])
        # time.sleep(2)
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

# print(players_stats["leaf"])
# print(players_with_one_agents_played)

# print(all_agents)

    all_trs = []

    for agent in all_agents:
        page = requests.get(f'{url}{agent}')
        soup = BeautifulSoup(page.content, "html.parser")
        trs = soup.find_all("tr")[1:]
        for tr in trs:
            player = tr.find("td").find("div").find("div").text
            if player not in players_with_one_agent_played:
                all_trs.append(tr)
        time.sleep(.05)

    remove_empty_strings(all_trs)


    for tr in all_trs:
        player = ""
        team = ""
        agent = ""
        for index, td in enumerate(tr):
            # print(index)
            try:
                td_class = td.get('class') or ""
                class_name = " ".join(td_class)
                # print(class_name)
                if class_name == "mod-player mod-a":
                    player_info = td.find("div").find_all("div")
                    player = player_info[0].text
                    team = player_info[1].text
                elif class_name == "mod-agents":
                    img = td.find("div").find("img")
                    file_name = img.get("src")
                    match = re.search(pattern, file_name)
                    agent = match.group(1)
                    # print(f'{td}\n{agent} {class_name}')
                    players_stats[stage][player][agent] = {}
                    players_stats[stage][player][agent]["team"] = team
                elif class_name == "mod-rnd" or class_name == "mod-cl" or class_name == "":
                    stat = remove_special_characters(td.text)
                    stat_name = stats_titles[index]
                    players_stats[stage][player][agent][stat_name] = stat
                elif class_name == "mod-color-sq mod-acs" or class_name ==  "mod-color-sq":
                    stat = td.find("div").find("span").text
                    stat_name = stats_titles[index]
                    players_stats[stage][player][agent][stat_name] = stat
                elif class_name == "mod-a mod-kmax":
                    stat = remove_special_characters(td.find("a").text)
                    stat_name = stats_titles[index]
                    players_stats[stage][player][agent][stat_name] = stat
            except AttributeError:
                continue