import requests
from bs4 import BeautifulSoup
import time
from WebScraper.retrieve_urls import retrieve_urls
from datetime import datetime
import pandas as pd
import asyncio
import aiohttp
from WebScraper.fetch import scraping_matches_data




async def main():
    semaphore_count = 25
    matches_semaphore = asyncio.Semaphore(semaphore_count)
    year = input(f"Input the VCT year: ")
    start_time = time.time()

    now = datetime.now()

    current_time = now.strftime("%H:%M:%S")
    print("Current Time =", current_time)

    url = f"https://www.vlr.gg/vct-{year}"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    urls = {}

    tournament_cards = soup.find_all("a", class_="wf-card mod-flex event-item")

    retrieve_urls(urls, tournament_cards, "/event/", "/event/matches/")

    matches_cards = {}

    for tournament, url in urls.items():
        page = requests.get(url)

        soup = BeautifulSoup(page.content, "html.parser")

        all_cards = soup.select('div.wf-card:not([class*=" "])')
        modules = []
        # print(tournament ,len(all_cards))
        for cards in all_cards:
            all_modules = cards.find_all("a")
            modules.extend(all_modules)
        matches_cards[tournament] = modules

    dataframes = {}
    all_results = {"scores": [],
                   "maps_played": [],
                   "maps_scores": [],
                   "draft_phase": [],
                   "overview": [],
                   "kills": [],
                   "kills_stats": [],
                   "rounds_kills": [],
                   "eco_stats": [],
                   "eco_rounds": [],
                   "team_mapping": {}}

    async with aiohttp.ClientSession() as session:
        tasks = [scraping_matches_data(tournament_name, cards, matches_semaphore, session) for tournament_name, cards in matches_cards.items()]
        results = await asyncio.gather(*tasks)

    # print(results)

    for result in results:
        for dictionary in result:
            for name, data in dictionary.items():
                if name == "team_mapping":
                    all_results[name].update(data)
                else:
                    all_results[name].extend(data)


    # team_mapping = all_results["team_mapping"]

    dataframes["scores"] = pd.DataFrame(all_results["scores"],
                                        columns=["Tournament", "Stage", "Match Type", "Match Name", "Team A", "Team B", "Team A Score", "Team B Score", "Match Result"])
    dataframes["maps_played"] = pd.DataFrame(all_results["maps_played"],
                                             columns=["Tournament", "Stage", "Match Type", "Match Name", "Map"])
    dataframes["maps_scores"] = pd.DataFrame(all_results["maps_scores"],
                                                columns=["Tournament", "Stage", "Match Type", "Match Name", "Map", "Team A", "Team A Score",
                                                        "Team A Attacker Score", "Team A Defender Score", "Team A Overtime Score",
                                                        "Team B", "Team B Score", "Team B Attacker Score", "Team B Defender Score",
                                                        "Team B Overtime Score", "Duration"])
    dataframes["draft_phase"] = pd.DataFrame(all_results["draft_phase"],
                                                columns=["Tournament", "Stage", "Match Type", "Match Name", "Team", "Action", "Map"])
    dataframes["overview"] = pd.DataFrame(all_results["overview"],
                                            columns=["Tournament", "Stage", "Match Type", "Match Name", "Map", "Player", "Team",
                                                    "Agents", "Rating", "Average Combat Score", "Kills", "Deaths",
                                                    "Assists", "Kill - Deaths (KD)", "Kill, Assist, Trade, Survive %",
                                                    "Average Damage per Round", "Headshot %", "First Kills", "First Deaths",
                                                    "Kills - Deaths (FKD)", "Side"])
    dataframes["kills"] = pd.DataFrame(all_results["kills"],
                                        columns=["Tournament", "Stage", "Match Type", "Match Name", "Map", "Player Team",
                                                "Player", "Enemy Team", "Enemy", "Player Kills", "Enemy Kills",
                                                "Difference", "Kill Type"])
    dataframes["kills_stats"] = pd.DataFrame(all_results["kills_stats"],
                                                columns=["Tournament", "Stage", "Match Type", "Match Name", "Map", "Team",
                                                        "Player", "Agent", "2k", "3k", "4k", "5k", "1v1",
                                                        "1v2", "1v3", "1v4", "1v5", "Econ", "Spike Plants",
                                                        "Spike Defuse"])
    dataframes["rounds_kills"] = pd.DataFrame(all_results["rounds_kills"],
                                                columns=["Tournament", "Stage", "Match Type", "Match Name", "Map", "Round Number",
                                                        "Eliminator Team", "Eliminator", "Eliminator Agent", 
                                                        "Eliminated Team", "Eliminated", "Eliminated Agent", "Kill Type"])
    dataframes["eco_stats"] = pd.DataFrame(all_results["eco_stats"],
                                            columns=["Tournament", "Stage", "Match Type", "Match Name", "Map", "Team", "Type", "Initiated", "Won"])
    dataframes["eco_rounds"] = pd.DataFrame(all_results["eco_rounds"],
                                            columns=["Tournament", "Stage", "Match Type", "Match Name", "Map", "Round Number", "Team", "Credits", "Type", "Outcome"])
    dataframes["team_mapping"] = pd.DataFrame(list(all_results["team_mapping"].items()),
                                              columns=["Abbreviated", "Full Name"])

        
    end_time = time.time()
    elasped_time = end_time - start_time

    hours, remainder = divmod(elasped_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    print(f"Datascraping time: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")


    start_time = time.time()

    for file_name, dataframe in dataframes.items():
        dataframe.to_csv(f"vct_{year}/matches/{file_name}.csv", encoding="utf-8", index=False)

    end_time = time.time()

    elasped_time = end_time - start_time

    hours, remainder = divmod(elasped_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    print(f"Data to CSV time: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")

    now = datetime.now()


    current_time = now.strftime("%H:%M:%S")
    print("End Time =", current_time)

if __name__ == "__main__":
    asyncio.run(main())


