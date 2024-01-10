import requests
from bs4 import BeautifulSoup
import time
from WebScraper.retrieve_urls import retrieve_urls
from WebScraper.fetch import generate_urls_combination, scraping_players_stats
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime


async def main():
    start_time = time.time()

    now = datetime.now()

    current_time = now.strftime("%H:%M:%S")
    print("Current Time =", current_time)
    url = "https://www.vlr.gg/vct-2023"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    tournament_cards = soup.find_all("a", class_="wf-card mod-flex event-item")
    urls = {}

    retrieve_urls(urls, tournament_cards, "/event/", "/event/stats/")
    filtered_urls = {}

    async with aiohttp.ClientSession() as session:
        tasks = [generate_urls_combination(tournament_name, url, filtered_urls, session) for tournament_name, url in urls.items()]
        await asyncio.gather(*tasks)


    async with aiohttp.ClientSession() as session:
        tasks = [scraping_players_stats(tournament_name, stages, session) for tournament_name, stages in filtered_urls.items()]
        results = await asyncio.gather(*tasks)
    
    all_result = []

    for result in results:
        for tournament_name, stages in result.items():
            for stage_name, match_types in stages.items():
                for match_type, teams in match_types.items():
                    for team_name, players in teams.items():
                        for player_name, agents in players.items():
                            for agents_played, stats in agents.items():
                                all_result.append([tournament_name, stage_name, match_type, player_name, team_name, agents_played] + list(stats.values()))
    
    players_stats_df = pd.DataFrame(all_result,
                                    columns=["Tournament", "Stage", "Match Type", "Player", "Team", "Agents", "Rounds Played",
                                            "Rating", "Average Combat Score", "Kills:Deaths", "Kill, Assist, Trade, Survive %",
                                            "Average Damage per Round", "Kills Per Round", "Assists Per Round", "First Kills Per Round",
                                            "First Deaths Per Round", "Headshot %", "Clutch Success %", "Clutches (won/played)",
                                            "Maximum Kills in a Single Map", "Kills", "Deaths", "Assists", "First Kills", "First Deaths"])

    players_stats_df.to_csv("players_stats/players_stats.csv", encoding="utf-8", index=False)

    end_time = time.time()
    elasped_time = end_time - start_time

    hours, remainder = divmod(elasped_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    print(f"Datascraping time: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")

    current_time = now.strftime("%H:%M:%S")
    print("End Time =", current_time)

if __name__ == "__main__":
    asyncio.run(main())

