import os
from Connect.connect import connect
from initialization.create_tables import create_all_tables
import asyncio
from initialization.add_data import *


async def main():
    conn, curr = connect()
    unique_ids = set()
    create_all_tables(curr)
    add_data_reference_tables(curr, unique_ids)
    drafts = pd.read_csv("matches/draft_phase.csv")
    eco_rounds = pd.read_csv("matches/eco_rounds.csv")
    eco_stats = pd.read_csv("matches/eco_stats.csv")
    kills = pd.read_csv("matches/kills.csv")
    kills_stats = pd.read_csv("matches/kills_stats.csv")
    maps_played = pd.read_csv("matches/maps_played.csv")
    maps_scores = pd.read_csv("matches/maps_scores.csv")
    overview = pd.read_csv("matches/overview.csv")
    rounds_kills = pd.read_csv("matches/rounds_kills.csv")
    scores = pd.read_csv("matches/scores.csv")
    pick_rates = pd.read_csv("agents/agents_pick_rates.csv")
    maps_stats = pd.read_csv("agents/maps_stats.csv")
    teams_picked_agents = pd.read_csv("agents/teams_picked_agents.csv")
    players_stats = pd.read_csv("players_stats/players_stats.csv")
    tables_and_dataframes = [
        ("Drafts", drafts, add_drafts),
        ("Eco Rounds", eco_rounds, add_eco_rounds),
        ("Eco Stats", eco_stats, add_eco_stats),
        ("Kills", kills, add_kills),
        ("Kills Stats", kills_stats, add_kills_stats),
        ("Maps Played", maps_played, add_maps_played),
        ("Maps Scores", maps_scores, add_maps_scores),
        ("Overview", overview, add_overview),
        ("Rounds Kills", rounds_kills, add_rounds_kills),
        ("Scores", scores, add_scores),
        ("Pick Rates", pick_rates, add_agents_pick_rates),
        ("Maps Stats", maps_stats, add_maps_stats),
        ("Teams Picked Agents", teams_picked_agents, add_teams_picked_agents),
        ("Players Stats", players_stats, add_players_stats),
    ]
    tasks = []

    for table_name, dataframe, insertion_function in tables_and_dataframes:
        task =insert_data(curr, dataframe, insertion_function, table_name)
        tasks.append(task)
    await asyncio.gather(*tasks)
    # tasks = all_data_table_functions(curr)
    # await asyncio.gather(add_drafts(curr), add_eco_rounds(curr), add_eco_stats(curr),
    #        add_kills(curr), add_kills_stats(curr), add_maps_played(curr),
    #        add_maps_scores(curr), add_overview(curr), add_rounds_kills(curr),
    #        add_scores(curr), add_agents_pick_rates(curr), add_maps_stats(curr),
    #        add_teams_picked_agents(curr), add_players_stats(curr))
    # loop.close()

    conn.commit()
    curr.close()
    conn.close()

if __name__ == '__main__':
    asyncio.run(main())