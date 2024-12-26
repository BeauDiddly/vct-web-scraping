from bs4 import BeautifulSoup
from datetime import datetime
from utilities.WebScraper.retrieve_urls import retrieve_urls
from utilities.WebScraper.fetch import generate_urls_combination, scraping_players_stats
import time
import asyncio
import aiohttp
import pandas as pd
import requests
import boto3
import io


async def main():
    url_semaphore = asyncio.Semaphore(10)
    player_stats_semaphore = asyncio.Semaphore(5) 
    # year = input(f"Input the VCT year: ")
    year = 2025
    start_time = time.time()

    now = datetime.now()

    current_time = now.strftime("%H:%M:%S")
    print("Current Time =", current_time)
    team_df = pd.read_csv(f"vct_{year}/matches/team_mapping.csv")
    df = pd.read_csv(f"vct_{year}/matches/overview.csv")
    tournament_ids = {}
    team_player_df = df[["Player", "Team", "Tournament", "Stage", "Match Type", "Match Name"]].drop_duplicates()
    team_player_df["Match Type"] = team_player_df["Match Type"].str.strip()

    s3_client = boto3.client(
        's3'
    )

    url = f"https://www.vlr.gg/vct-{year}"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    tournament_cards = soup.find("div", class_="events-container").find_all("div", class_="events-container-col")[-1].find_all("a", class_="wf-card mod-flex event-item")
    urls = {}

    retrieve_urls(urls, tournament_ids, tournament_cards, "/event/", "/event/stats/")

    # if os.path.exists("all_ids/all_tournaments_stages_match_types_ids.csv"):
    #     all_tournaments_df = pd.read_csv("all_ids/all_tournaments_stages_match_types_ids.csv")
    #     filtered_df = all_tournaments_df[all_tournaments_df['Year'] == int(year)]
    #     all_tournaments = set(filtered_df["Tournament"].unique())
    #     current_tournaments = set(urls.keys())
    #     new_tournaments = list(all_tournaments ^ current_tournaments)
    #     if new_tournaments:
    #         filtered_urls = {tournament: url for tournament, url in urls.items() if tournament not in all_tournaments}
    #         urls = filtered_urls
    #     else:
    #         print("No new data")
    #         sys.exit(0)


    filtered_urls = {}
    stages_ids = {}
    match_types_ids = {}

    async with aiohttp.ClientSession() as session:
        tasks = [generate_urls_combination(tournament_name, stages_ids, match_types_ids, url, filtered_urls, url_semaphore, session) for tournament_name, url in urls.items()]
        await asyncio.gather(*tasks)


    async with aiohttp.ClientSession() as session:
        tasks = [scraping_players_stats(tournament_name, stages, team_player_df, player_stats_semaphore, session) for tournament_name, stages in filtered_urls.items()]
        results = await asyncio.gather(*tasks)
    
    all_result = []

    for result in results:
        for inner_list in result:
            for dictionary in inner_list:
                for tournament_name, stages in dictionary.items():
                    for stage_name, match_types in stages.items():
                        for match_type, teams in match_types.items():
                            for team_name, players in teams.items():
                                for player_name, agents in players.items():
                                    for agents_played, stats in agents.items():
                                        all_result.append([tournament_name, stage_name, match_type, player_name, team_name, agents_played] + list(stats.values()))
    
    players_stats_df = pd.DataFrame(all_result,
                                    columns=["Tournament", "Stage", "Match Type", "Player", "Teams", "Agents", "Rounds Played",
                                            "Rating", "Average Combat Score", "Kills:Deaths", "Kill, Assist, Trade, Survive %",
                                            "Average Damage Per Round", "Kills Per Round", "Assists Per Round", "First Kills Per Round",
                                            "First Deaths Per Round", "Headshot %", "Clutch Success %", "Clutches (won/played)",
                                            "Maximum Kills in a Single Map", "Kills", "Deaths", "Assists", "First Kills", "First Deaths"])
    csv_buffer = io.StringIO()
    players_stats_df.to_csv(csv_buffer, index=False)
    directory =  f"vct_{year}/players_stats/players_stats.csv"
    s3_client.put_object(Bucket="raw-data-vct", Key=directory, Body=csv_buffer.getvalue())
    # players_stats_df.to_csv(f"vct_{year}/players_stats/players_stats.csv", encoding="utf-8", index=False)

    end_time = time.time()
    elasped_time = end_time - start_time

    hours, remainder = divmod(elasped_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    print(f"Datascraping time: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")

    current_time = now.strftime("%H:%M:%S")
    print("End Time =", current_time)

if __name__ == "__main__":
    asyncio.run(main())

