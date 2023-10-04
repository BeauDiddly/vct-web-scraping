import requests
from bs4 import BeautifulSoup, Tag, NavigableString
import re
import time

def map_tables(tag):
    return tag.name == "table" and "class" in tag.attrs and tag["class"] == ["wf-table"]

urls = {"all_stages": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo",
        "semi_finals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16339.16332.16333.16334.16335.16336.16337",
        "grand_finals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16332.16333.16334.16335.16336.16337",
        "playoffs": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16332.16333.16334.16335.16336.16337",
        "alpha_round_of_sixteen": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16333.16334.16335.16336.16337",
        "alpha_quarterfinals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16334.16335.16336.16337",
        "alpha_semifinals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16335.16336.16337",
        "alpha": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16335.16336.16337",
        "omega_round_of_sixteen": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334.16336.16337",
        "omega_quarterfinals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334.16335.16337",
        "omega_semifinals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334.16335.16336",
        "omega": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334",
        "bracket_stage": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339"}

# agent_pictures = soup.find("table", class_="wf-table mod-pr-global").find_all("th", style=" vertical-align: middle; padding-top: 0; padding-bottom: 0; width: 65px;")

# maps = soup.find("table", class_="wf-table mod-pr-global").find_all("td", style="white-space: nowrap; padding-top: 0; padding-bottom: 0;")

# pick_rates = soup.find("table", class_="wf-table mod-pr-global").find_all("div", class_="color-sq")

# max_rows = len(soup.find("table", class_="wf-table mod-pr-global").find_all("tr", class_="pr-global-row"))

# first_map_tables = soup.find(map_tables)

# all_teams = first_map_tables.find_all("span", class_="text-of")

# all_maps = soup.find_all('th', style="padding: 0; padding-left: 15px; line-height: 0; vertical-align: middle;")

# all_pr_matrix_rows = soup.select('tr.pr-matrix-row:not([class*=" "])')

# agents_names = []
# pattern = r'/(\w+)\.png'
# for th in agent_pictures:
#     file_name = th.find("img").get("src")
#     match = re.search(pattern, file_name)
#     agent_name = match.group(1)
#     agents_names.append(agent_name)


# teams_names = []
# for team in all_teams:
#     teams_names.append(team.text.strip())

# maps_names = []

# for map in all_maps:
#     map_name = map.find_all(string=True, recursive=False)[-1].strip()
#     maps_names.append(map_name)

# for row in all_pr_matrix_rows:
#     children_to_remove = row.find_all("td")[:2]
#     for child in children_to_remove:
#         child.extract()

# for row in all_pr_matrix_rows:
#     for td in row:
#         # if not isinstance(td, Tag) and isinstance(td, NavigableString):
#         #     td.extract()
#         if not isinstance(td, Tag) or td.name != "td":
#             td.extract()

# for row in all_pr_matrix_rows:
#     for td in row:
#         if not isinstance(td, Tag) and isinstance(td, NavigableString):
#             td.extract()

# steps = len(teams_names)

# print(len(all_pr_matrix_rows))

# grouping_pr_matrix_rows = []

# for i in range(0, len(all_pr_matrix_rows), steps):
#     start = i
#     end = i + steps
#     sub_list = all_pr_matrix_rows[start: end]
#     grouping_pr_matrix_rows.append(sub_list)

# for item in grouping_pr_matrix_rows:
#     print(item, )

# print(all_pr_matrix_rows)



# print(maps_names)

# for table in individual_map_tables:
#     map_name = table.find('th', style="padding: 0; padding-left: 15px; line-height: 0; vertical-align: middle;") \
#                     .find_all(string=True, recursive=False)[-1].strip()
#     maps_names.append(map_name)


# print(maps_names)
# all_maps = individual_map_tables.find_all('th', style="padding: 0; padding-left: 15px; line-height: 0; vertical-align: middle;") \
#         .find_all(string=True, recursive=False)[-1].strip()

# print(all_maps)

# print(all_teams)


# print(teams_names)

# agents_names = []
# pattern = r'/(\w+)\.png'
# for th in agent_pictures:
#     file_name = th.find("img").get("src")
#     match = re.search(pattern, file_name)
#     agent_name = match.group(1)
#     agents_names.append(agent_name)

# print(teams_names)
# print(len(individual_map_tables))
pattern = r'/(\w+)\.png'
teams_pick_rates = {}

for stage, url in urls.items():
    page = requests.get(url)

    soup = BeautifulSoup(page.content, "html.parser")
    agent_pictures = soup.find("table", class_="wf-table mod-pr-global").find_all("th", style=" vertical-align: middle; padding-top: 0; padding-bottom: 0; width: 65px;")

    first_map_tables = soup.find(map_tables)

    all_teams = first_map_tables.find_all("span", class_="text-of")

    all_maps = soup.find_all('th', style="padding: 0; padding-left: 15px; line-height: 0; vertical-align: middle;")

    all_pr_matrix_rows = soup.select('tr.pr-matrix-row:not([class*=" "])')

    agents_names = []
    for th in agent_pictures:
        file_name = th.find("img").get("src")
        match = re.search(pattern, file_name)
        agent_name = match.group(1)
        agents_names.append(agent_name)


    teams_names = []
    for team in all_teams:
        teams_names.append(team.text.strip())

    maps_names = []

    for map in all_maps:
        map_name = map.find_all(string=True, recursive=False)[-1].strip()
        maps_names.append(map_name)

    for row in all_pr_matrix_rows:
        children_to_remove = row.find_all("td")[:2]
        for child in children_to_remove:
            child.extract()

    for row in all_pr_matrix_rows:
        for td in row:
            # if not isinstance(td, Tag) and isinstance(td, NavigableString):
            #     td.extract()
            if not isinstance(td, Tag) or td.name != "td":
                td.extract()

    for row in all_pr_matrix_rows:
        for td in row:
            if not isinstance(td, Tag) and isinstance(td, NavigableString):
                td.extract()

    steps = len(teams_names)

    # print(len(all_pr_matrix_rows))

    grouping_pr_matrix_rows = []

    for i in range(0, len(all_pr_matrix_rows), steps):
        start = i
        end = i + steps
        sub_list = all_pr_matrix_rows[start: end]
        grouping_pr_matrix_rows.append(sub_list)

# for table in individual_map_tables[1:2]:
#     for team in table:
        # print(team)
        # for data in team[2:]:
        #     print(data)


# for index, table in enumerate(individual_map_tables):
#     map_name = table.find('th', style="padding: 0; padding-left: 15px; line-height: 0; vertical-align: middle;") \
#         .find_all(string=True, recursive=False)[-1].strip()
#     # print(map_name)
#     team_pick_rates[map_name] = {}
#     teams_that_played = table.select("tr:has(td.mod-picked)")
#     # print(f'Index {index}')
#     for team in teams_that_played:
#         for data in team:
#             if isinstance(data, Tag) and data.find('a').find('img'):
#                 print(data.text)
    teams_pick_rates[stage] = {}
    for map_index, list_of_tr in enumerate(grouping_pr_matrix_rows):
        map = maps_names[map_index]
        teams_pick_rates[stage][map] = {}
        for team_index, tr in enumerate(list_of_tr):
            team = teams_names[team_index]
            team_pick_rate = {}
            for agent_index, td in enumerate(tr):
                try:
                    classes = td.get("class", [])
                    agent = agents_names[agent_index]
                    if "mod-picked" in classes:
                        picked = 1
                    else:
                        picked = 0
                    team_pick_rate[agent] = picked
                except AttributeError:
                    continue
    # #             try:
    # #                 mod_picked = td['class']
    # #                 print(f'1')
    # #             except:
    # #                 print(f'0')
            teams_pick_rates[stage][map][team] = team_pick_rate
# print(teams_pick_rates["Lotus"])

# for key, value in teams_pick_rates.items():
#     for key1, value1 in value.items():
#         print(f'{key1}: {value1}')
# for index, tr in enumerate(all_pr_matrix_rows[0]):
# #     print(tr.prettify())
#     if not isinstance(tr, Tag):
#         if isinstance(tr, NavigableString):
#             tr.extract()

    # else:
    #     print(tr)
    # for td in tr:
    #     try:
    #         mod_picked = td['class']
    #         print(f'1')
    #     except:
    #         print(f'0')
    # print(f'\n')

# for row in all_pr_matrix_rows:
#     print(len(row))

# print(len(all_pr_matrix_rows))