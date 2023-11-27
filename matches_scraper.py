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
    start_time = time.time()

    now = datetime.now()

    current_time = now.strftime("%H:%M:%S")
    print("Current Time =", current_time)

    url = "https://www.vlr.gg/vct-2023"
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
                "maps_scores": [],
                "draft_phase": [],
                "overview": [],
                "kills": [],
                "kills_stats": [],
                "rounds_kills": [],
                "eco_stats": [],
                "eco_rounds": []}

    async with aiohttp.ClientSession() as session:
        tasks = [scraping_matches_data(tournament_name, cards, session) for tournament_name, cards in matches_cards.items()]
        results = await asyncio.gather(*tasks)

    for result in results:
        for name, data in result.items():
            all_results[name].extend(data)


    dataframes["scores"] = pd.DataFrame(all_results["scores"],
                                        columns=["Tournament", "Stage", "Match Type", "Winner", "Loser", "Winner's Score", "Loser's Score"])
    dataframes["maps_scores"] = pd.DataFrame(all_results["maps_scores"],
                                                columns=["Tournament", "Stage", "Match Type", "Map", "Team A", "Team A's Score",
                                                        "Team A's Attack Score", "Team A's Defender Score", "Team A's Overtime Score",
                                                        "Team B", "Team B's Score", "Team B's Attack Score", "Team B' Defender Score",
                                                        "Team B's Overtime Score", "Duration"])
    dataframes["draft_phase"] = pd.DataFrame(all_results["draft_phase"],
                                                columns=["Tournament", "Stage", "Match Type", "Team", "Action", "Map"])
    dataframes["overview"] = pd.DataFrame(all_results["overview"],
                                            columns=["Tournament", "Stage", "Match Type", "Map", "Player", "Team",
                                                    "Agents", "Rating", "Average Combat Score", "Kills", "Deaths",
                                                    "Assists", "Kill - Deaths (KD)", "Kill, Assist, Trade, Survive %",
                                                    "Average Damage per Round", "Headshot %", "First Kills", "First Deaths",
                                                    "Kills - Deaths (FKD)", "Side"])
    dataframes["kills"] = pd.DataFrame(all_results["kills"],
                                        columns=["Tournament", "Stage", "Match Type", "Map", "Player's Team",
                                                "Player", "Enemy's Team", "Enemy", "Player's Kills", "Enemy's Kills",
                                                "Difference", "Kill Type"])
    dataframes["kills_stats"] = pd.DataFrame(all_results["kills_stats"],
                                                columns=["Tournament", "Stage", "Match Type", "Map", "Team",
                                                        "Player", "Agent", "2K", "3k", "4k", "5k", "1v1",
                                                        "1v2", "1v3", "1v4", "1v5", "Econ", "Spike Plants",
                                                        "Spike Defuse"])
    dataframes["rounds_kills"] = pd.DataFrame(all_results["rounds_kills"],
                                                columns=["Tournament", "Stage", "Match Type", "Map", "Round Number",
                                                        "Eliminator's Team", "Eliminator", "Eliminator's Agent", 
                                                        "Eliminated Team", "Eliminated", "Eliminated's Agent", "Kill Type"])
    dataframes["eco_stats"] = pd.DataFrame(all_results["eco_stats"],
                                            columns=["Tournament", "Stage", "Match Type", "Map", "Team", "Type", "Initiated", "Won"])
    dataframes["eco_rounds"] = pd.DataFrame(all_results["eco_rounds"],
                                            columns=["Tournament", "Stage", "Match Type", "Map", "Round Number", "Team", "Credits", "Type", "Outcome"])

        
    end_time = time.time()
    elasped_time = end_time - start_time

    hours, remainder = divmod(elasped_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    print(f"Datascraping time: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")


    start_time = time.time()

    for file_name, dataframe in dataframes.items():
        dataframe.to_csv(f"matches/{file_name}.csv", encoding="utf-8", index=False)

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


