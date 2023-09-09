import requests
from bs4 import BeautifulSoup, Tag
import re

def map_tables(tag):
    return tag.name == "table" and "class" in tag.attrs and tag["class"] == ["wf-table"]

url = f"https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16333.16334.16335.16336.16337"
page = requests.get(url)

soup = BeautifulSoup(page.content, "html.parser")

agent_pictures = soup.find("table", class_="wf-table mod-pr-global").find_all("th", style=" vertical-align: middle; padding-top: 0; padding-bottom: 0; width: 65px;")

# maps = soup.find("table", class_="wf-table mod-pr-global").find_all("td", style="white-space: nowrap; padding-top: 0; padding-bottom: 0;")

# pick_rates = soup.find("table", class_="wf-table mod-pr-global").find_all("div", class_="color-sq")

# max_rows = len(soup.find("table", class_="wf-table mod-pr-global").find_all("tr", class_="pr-global-row"))

individual_map_tables = soup.find_all(map_tables)

all_teams = individual_map_tables[0].find_all("span", class_="text-of")

maps_names = []

for table in individual_map_tables:
    map_name = table.find('th', style="padding: 0; padding-left: 15px; line-height: 0; vertical-align: middle;") \
                    .find_all(string=True, recursive=False)[-1].strip()
    maps_names.append(map_name)


# print(maps_names)
# all_maps = individual_map_tables.find_all('th', style="padding: 0; padding-left: 15px; line-height: 0; vertical-align: middle;") \
#         .find_all(string=True, recursive=False)[-1].strip()

# print(all_maps)

# print(all_teams)

teams_names = []
for team in all_teams:
    teams_names.append(team.text.strip())

agents_names = []
pattern = r'/(\w+)\.png'
for th in agent_pictures:
    file_name = th.find("img").get("src")
    match = re.search(pattern, file_name)
    agent_name = match.group(1)
    agents_names.append(agent_name)

# print(teams_names)
# print(len(individual_map_tables))

team_pick_rates = {}


for table in individual_map_tables[1:]:
    for team in table:
        for data in team[2:]:
            print(data)


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

