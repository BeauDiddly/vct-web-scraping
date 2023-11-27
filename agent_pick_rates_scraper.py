import requests
from bs4 import BeautifulSoup, Tag, NavigableString
import re
import time
import csv
import asyncio
from MaxReentriesReached.max_reentries_reached import MaxReentriesReached
from WebScraper.fetch import fetch, generate_urls_combination, scraping_agents_data
from WebScraper.retrieve_urls import retrieve_urls
import aiohttp
# url = f"https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16334.16335.16336.16337"

# urls = {"all_stages": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo",
#         "semi_finals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16339.16332.16333.16334.16335.16336.16337",
#         "grand_finals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16332.16333.16334.16335.16336.16337",
#         "playoffs": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16332.16333.16334.16335.16336.16337",
#         "alpha_round_of_sixteen": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16333.16334.16335.16336.16337",
#         "alpha_quarterfinals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16334.16335.16336.16337",
#         "alpha_semifinals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16335.16336.16337",
#         "alpha": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16335.16336.16337",
#         "omega_round_of_sixteen": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334.16336.16337",
#         "omega_quarterfinals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334.16335.16337",
#         "omega_semifinals": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334.16335.16336",
#         "omega": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339.16332.16333.16334",
#         "bracket_stage": "https://www.vlr.gg/event/agents/1188/champions-tour-2023-lock-in-s-o-paulo?exclude=16338.16339"}
async def main():
    url = "https://www.vlr.gg/vct-2023"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    urls = {}

    tournament_cards = soup.find_all("a", class_="wf-card mod-flex event-item")

    retrieve_urls(urls, tournament_cards, "/event/", "/event/agents")

    stages_filter = {}

    async with aiohttp.ClientSession() as session:
        tasks = [generate_urls_combination(tournament_name, url, stages_filter, session) for tournament_name, url in urls.items()]
        await asyncio.gather(*tasks)
        
    pick_rates = {}

    async with aiohttp.ClientSession() as session:
        tasks = [scraping_agents_data(tournament_name, stages, session) for tournament_name, stages in stages_filter.items()]
        await asyncio.gather(*tasks)



    with open("maps_stats.csv", "w", newline="") as maps_stats_file, open("agents_pick_rates.csv", "w", newline="") as agents_pick_rates_file, \
        open("teams_pick_rates.csv", "w", newline="") as teams_pick_rates_file:
        maps_stats_writer = csv.writer(maps_stats_file)
        agents_pick_rates_writer = csv.writer(agents_pick_rates_file)
        teams_pick_rates_writer = csv.writer(teams_pick_rates_file)
        maps_stats_writer.writerow(["Tournament", "Stage", "Match Type", "Map", "Total Played", "Attacker Side Win Percentage", "Defender Side Win Percentage"])
        agents_pick_rates_writer.writerow(["Tournament", "Stage", "Match Type", "Map", "Agent", "Pick Rate"])
        teams_pick_rates_writer.writerow(["Tournament", "Stage", "Match Type", "Map", "Team", "Agent"])
        

        for tournament_name, stages in pick_rates.items():
            for stage_name, match_types in stages.items():
                for match_type, title in match_types.items():
                    maps_stats = title["Maps Stats"]
                    agents_pick_rates = title["Agents Pick Rates"]
                    teams_pick_rates = title["Teams Pick Rates"]

                    for map_name, stats in maps_stats.items():
                        maps_stats_writer.writerow([tournament_name, stage_name, match_type, map] + list(stats.values()))
                    
                    for map_name, agents in agents_pick_rates.items():
                        for agent_name, pick_rate in agents.items():
                            agents_pick_rates_writer.writerow([tournament, stage_name, match_type, map, agent_name, pick_rate])
                    
                    for map_name, teams in teams_pick_rates.items():
                        for team_name, agents in teams.items():
                            for agent in agents:
                                teams_pick_rates_writer.writerow([tournament, stage_name, match_type, map, team_name, agent])


                        