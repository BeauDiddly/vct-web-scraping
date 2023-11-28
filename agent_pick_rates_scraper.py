import requests
from bs4 import BeautifulSoup, Tag, NavigableString
import re
import time
import csv
import asyncio
import pandas as pd
import time
from datetime import datetime
from WebScraper.fetch import generate_urls_combination, scraping_agents_data
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
    start_time = time.time()

    now = datetime.now()

    current_time = now.strftime("%H:%M:%S")
    print("Current Time =", current_time)
    url = "https://www.vlr.gg/vct-2023"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    urls = {}

    tournament_cards = soup.find_all("a", class_="wf-card mod-flex event-item")

    retrieve_urls(urls, tournament_cards, "/event/", "/event/agents/")

    stages_filter = {}

    async with aiohttp.ClientSession() as session:
        tasks = [generate_urls_combination(tournament_name, url, stages_filter, session) for tournament_name, url in urls.items()]
        await asyncio.gather(*tasks)

    async with aiohttp.ClientSession() as session:
        tasks = [scraping_agents_data(tournament_name, stages, session) for tournament_name, stages in stages_filter.items()]
        results = await asyncio.gather(*tasks)

    all_results = {"maps_stats": [],
                   "agents_pick_rates": [],
                   "teams_picked_agents": []}
    
    dataframes = {}

    for result in results:
        for tournament_name, stages in result.items():
            for stage_name, match_types in stages.items():
                for match_type_name, stats in match_types.items():
                    maps_stats = stats["Maps Stats"]
                    agents_pick_rates = stats["Agents Pick Rates"]
                    teams_pick_rates = stats["Teams Pick Rates"]

                    for map_name, stats in maps_stats.items():
                        combined_list = [tournament_name, stage_name, match_type_name, map_name] + list(stats.values())
                        all_results["maps_stats"].append(combined_list)
                    
                    for map_name, agents in agents_pick_rates.items():
                        for agent_name, pick_rate in agents.items():
                            all_results["agents_pick_rates"].append([tournament_name, stage_name, match_type_name, map_name, agent_name, pick_rate])
                    
                    for map_name, teams in teams_pick_rates.items():
                        for team_name, agents in teams.items():
                            for agent in agents:
                                all_results["teams_picked_agents"].append([tournament_name, stage_name, match_type_name, map_name, team_name, agent])
    
    print(all_results["maps_stats"])
    dataframes["maps_stats"] = pd.DataFrame(all_results["maps_stats"],
                                           columns=["Tournament", "Stage", "Match Type", "Map", "Total Map Played",
                                                    "Attack Side Win Percentage", "Defender Side Win Percentage"])
    dataframes["agents_pick_rates"] = pd.DataFrame(all_results["agents_pick_rates"],
                                                   columns=["Tournament", "Stage", "Match Type", "Map", "Agent", "Pick Rate"])
    dataframes["teams_picked_agents"] = pd.DataFrame(all_results["teams_picked_agents"],
                                                     columns=["Tournament", "Stage", "Match Type", "Map", "Team", "Agent Picked"])

    end_time = time.time()
    elasped_time = end_time - start_time

    hours, remainder = divmod(elasped_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    print(f"Datascraping time: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")

    for file_name, dataframe in dataframes.items():
        dataframe.to_csv(f"agents/{file_name}.csv", encoding="utf-8", index=False)

    current_time = now.strftime("%H:%M:%S")
    print("End Time =", current_time)



if __name__ == "__main__":
    asyncio.run(main())
