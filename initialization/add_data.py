from Connect.execute_query import execute_query
import pandas as pd
import numpy as np
from retrieve.retrieve import retrieve_primary_key
from checking.check_values import check_na
import asyncio
import time
from datetime import datetime
from process_df.process_df import *



def add_tournaments(df, engine):
   tournaments_df = df[["Tournament", "Tournament ID", "Year"]]
   strip_white_space(tournaments_df, "Tournament")
   tournaments_df = tournaments_df.drop_duplicates()
   tournaments_df = reorder_columns(tournaments_df, ["Tournament ID", "Tournament", "Year"])
   tournaments_df = rename_columns(tournaments_df, {"Tournament ID": "tournament_id", "Tournament": "tournament", "Year": "year"})
   tournaments_df.to_sql("tournaments", engine, index=False, if_exists = "append")
    
def add_stages(df, engine):
   stages_df = df[["Tournament ID", "Stage", "Stage ID", "Year"]]
   strip_white_space(stages_df, "Stage")
   stages_df = stages_df.drop_duplicates()
   stages_df.loc[stages_df["Stage ID"].isnull(), "Stage ID"] = stages_df["Tournament ID"] * 1000
   stages_df["Stage ID"] = pd.to_numeric(stages_df["Stage ID"])
   stages_df = reorder_columns(stages_df, ["Stage ID", "Tournament ID", "Stage", "Year"])
   stages_df = rename_columns(stages_df, {"Stage ID": "stage_id", "Tournament ID": "tournament_id", "Stage": "stage", "Year": "year"})
   stages_df.to_sql("stages", engine, index=False, if_exists="append")

def add_match_types(df, engine):
   match_types_df = df[["Tournament ID", "Stage ID", "Match Type", "Match Type ID", "Year"]]
   strip_white_space(match_types_df, "Match Type")
   match_types_df = match_types_df.drop_duplicates()
   match_types_df.loc[match_types_df["Stage ID"].isnull(), "Stage ID"] = match_types_df["Tournament ID"] * 1000 
   match_types_df.loc[match_types_df["Match Type ID"].isnull(), "Match Type ID"] = match_types_df["Stage ID"] * 100 
   match_types_df["Match Type ID"] = match_types_df["Match Type ID"].fillna(0000)
   match_types_df["Stage ID"] = pd.to_numeric(match_types_df["Stage ID"])
   match_types_df["Match Type ID"] = pd.to_numeric(match_types_df["Match Type ID"])
   match_types_df = reorder_columns(match_types_df, ["Match Type ID", "Tournament ID", "Stage ID", "Match Type", "Year"])
   match_types_df = rename_columns(match_types_df, {"Match Type ID": "match_type_id", "Tournament ID": "tournament_id", "Stage ID": "stage_id", 
                                                   "Match Type": "match_type", "Year": "year"})
   match_types_df.to_sql("match_types", engine, index=False, if_exists="append")


def add_matches(df, engine):
   matches_df = df[["Tournament ID", "Stage ID", "Match Type ID", "Match Name", "Match ID", "Year"]]
   strip_white_space(matches_df, "Match Name")
   matches_df = matches_df.drop_duplicates()
   matches_df = reorder_columns(matches_df, ["Match ID", "Tournament ID", "Stage ID", "Match Type ID", "Match Name", "Year"])
   matches_df = rename_columns(matches_df, {"Match ID": "match_id", "Tournament ID": "tournament_id",
                                             "Stage ID": "stage_id", "Match Type ID": "match_type_id", 
                                             "Match Name": "match", "Year": "year"})
   matches_df.to_sql("matches", engine, index=False, if_exists="append")

# def add_games(df, engine):
#    games_df = df[["Tournament ID", "Stage ID", "Match Type ID", "Match ID", "Game ID", "Map", "Year"]]
#    games_df = games_df.drop_duplicates()
#    games_df = reorder_columns(games_df, ["Game ID", "Tournament ID", "Stage ID", "Match Type ID", "Match ID", "Map", "Year"])
#    games_df = rename_columns(games_df, {"Game ID": "game_id", "Tournament ID": "tournament_id", "Stage ID": "stage_id",
#                                  "Match Type ID": "match_type_id", "Match ID": "match_id", 
#                                  "Map": "map", "Year": "year"})
#    games_df.to_sql("games", engine, index=False, if_exists="append")



def add_teams(df, engine):
   teams_df = df[["Team", "Team ID"]]
   teams_df = teams_df.drop_duplicates()
   strip_white_space(teams_df, "Team")
   teams_df["Team ID"] = teams_df["Team ID"].fillna(0000)
   teams_df = convert_column_to_int(teams_df, "Team ID")
   teams_df = reorder_columns(teams_df, {"Team ID", "Team"})
   teams_df = rename_columns(teams_df, {"Team ID": "team_id", "Team": "team"})
   teams_df.to_sql("teams", engine, index=False, if_exists="append")

def add_players(df, engine):
   players_df = df[["Player", "Player ID"]]
   players_df = players_df.drop_duplicates()
   players_df = reorder_columns(players_df, {"Player ID", "Player"})
   min_id = int(players_df["Player ID"].min())
   max_id = int(players_df["Player ID"].max())
   all_numbers = set(range(min_id, max_id + 1))
   null_count = players_df["Player ID"].isnull().sum()
   missing_numbers = sorted(all_numbers - set(players_df["Player ID"]))
   np.random.shuffle(missing_numbers)
   players_df.loc[players_df["Player ID"].isnull(), "Player ID"] = missing_numbers[:null_count]
   players_df = rename_columns(players_df, {"Player ID": "player_id", "Player": "player"})
   players_df.to_sql("players", engine, index=False, if_exists="append")

# async def insert_data(curr, dataframe, insertion_function, table_name):
#    start_time = time.time()
#    now = datetime.now()

#    print(f"Adding data to {table_name}")
#    tasks = [insertion_function(curr, row) for _, row in dataframe.iterrows()]
#    await asyncio.gather(*tasks)
#    print(f"Done adding data to {table_name}")
#    end_time = time.time()
#    elasped_time = end_time - start_time
#    hours, remainder = divmod(elasped_time, 3600)
#    minutes, seconds = divmod(remainder, 60)

#    print(f"Inserting data to {table_name} time: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")
   

async def add_drafts(file, year, curr, engine):
   drafts_df = csv_to_df(file)
   strip_white_space(drafts_df, "Match Type")
   strip_white_space(drafts_df, "Match Name")
   drafts_df = await change_reference_name_to_id(drafts_df, year)
   drafts_df = convert_column_to_int(drafts_df, "Team ID")
   drafts_df["year"] = year
   drafts_df = create_ids(drafts_df)
   drafts_df = drop_columns(drafts_df, ["Tournament", "Stage", "Match Type", "Match Name", "Team"])
   drafts_df = rename_columns(drafts_df, {"index": "draft_id","Tournament ID": "tournament_id", "Stage ID": "stage_id", "Match Type ID": "match_type_id", "Match ID": "match_id",
                                          "Team ID": "team_id", "Action": "action", "Map": "map"})
   drafts_df = reorder_columns(drafts_df, ["draft_id", "tournament_id", "stage_id", "match_type_id", "match_id", "team_id", "action", "map", "year"])
   print(drafts_df.sample(n=20))
   # drafts_df.to_sql("drafts", engine, index=False, if_exists="append")

async def add_eco_rounds(file, year, engine):
   eco_rounds_df = csv_to_df(file)
   strip_white_space(eco_rounds_df, "Match Type")
   strip_white_space(eco_rounds_df, "Match Name")
   strip_white_space(eco_rounds_df, "Team")
   eco_rounds_df = await change_reference_name_to_id(eco_rounds_df, year)
   eco_rounds_df["year"] = year
   eco_rounds_df = convert_column_to_int(eco_rounds_df, "Team ID")
   eco_rounds_df = create_ids(eco_rounds_df)
   eco_rounds_df = drop_columns(eco_rounds_df, ["Tournament", "Stage", "Match Type", "Match Name", "Team"])
   eco_rounds_df = rename_columns(eco_rounds_df, {"index": "eco_round_id","Tournament ID": "tournament_id", "Stage ID": "stage_id", "Match Type ID": "match_type_id", "Match ID": "match_id",
                                          "Team ID": "team_id", "Map": "map", "Round Number": "round_number", "Loadout Value": "loadout_value",
                                          "Remaining Credits": "credits", "Type": "eco_type", "Outcome": "outcome"})
   eco_rounds_df = reorder_columns(eco_rounds_df, ["eco_round_id", "tournament_id", "stage_id", "match_type_id", "match_id", "team_id",
                                             "map", "round_number", "loadout_value", "credits", "eco_type", "outcome", "year"])
   print(eco_rounds_df.sample(n=20))
   # eco_rounds_df.to_sql("eco_rounds", engine, index=False, if_exists="append", chunksize = 10000)
      
async def add_eco_stats(file, year, engine):
   eco_stats_df = csv_to_df(file)
   strip_white_space(eco_stats_df, "Stage")
   strip_white_space(eco_stats_df, "Match Type")
   strip_white_space(eco_stats_df, "Match Name")
   strip_white_space(eco_stats_df, "Team")
   eco_stats_df = await change_reference_name_to_id(eco_stats_df, year)
   eco_stats_df["year"] = year
   eco_stats_df = convert_missing_number(eco_stats_df, "Initiated")
   eco_stats_df = convert_column_to_int(eco_stats_df, "Team ID")
   eco_stats_df = create_ids(eco_stats_df)
   eco_stats_df = drop_columns(eco_stats_df, ["Tournament", "Stage", "Match Type", "Match Name", "Team"])
   eco_stats_df = rename_columns(eco_stats_df, {"index": "eco_stat_id","Tournament ID": "tournament_id", "Stage ID": "stage_id", "Match Type ID": "match_type_id", "Match ID": "match_id",
                                          "Team ID": "team_id", "Map": "map", "Type": "type", "Initiated": "initiated", "Won": "won"})
   eco_stats_df = reorder_columns(eco_stats_df, ["eco_stat_id", "tournament_id", "stage_id", "match_type_id", "match_id", "team_id", "map", "type", "initiated", "won"])
   print(eco_stats_df.sample(n=20))
   # eco_stats_df.to_sql("eco_stats", engine, index=False, if_exists="append", chunksize = 10000)
   
      

async def add_kills(file, year, engine):
   kills_df = csv_to_df(file)
   
   strip_white_space(kills_df, "Stage")
   strip_white_space(kills_df, "Match Type")
   strip_white_space(kills_df, "Match Name")
   strip_white_space(kills_df, "Player Team")
   strip_white_space(kills_df, "Enemy Team")
   kills_df = await change_reference_name_to_id(kills_df, year)
   kills_df = convert_missing_number(kills_df, "Player Kills")
   kills_df = convert_missing_number(kills_df, "Enemy Kills")
   kills_df = convert_missing_number(kills_df, "Difference")
   kills_df = convert_column_to_int(kills_df, "Team ID")
   kills_df = create_ids(kills_df)
   kills_df = drop_columns(kills_df, ["Tournament", "Stage", "Match Type", "Match Name", "Player Team", "Player", "Enemy Team", "Enemy"])
   print(kills_df.sample(n=20))
   # kills_df = rename_columns(kills_df, {"index": "kills_id", "Tournament ID": "tournament_id", "Stage ID": "stage_id", "Match Type ID": "match_type_id",
   #                                      "Match ID": "match_id", "Player Team ID": "player_team_id", "Player ID": "player_id", "Enemy Team ID": "enemy_team_id",
   #                                      "Enemy ID": "enemy_id", "Map": "map",})
   # print(kills_df.sample(n=20))
   
   # print(f"Adding kills")


async def add_kills_stats(file, year, engine):
   kills_stats_df = csv_to_df(file)
   strip_white_space(kills_stats_df, "Stage")
   strip_white_space(kills_stats_df, "Match Type")
   strip_white_space(kills_stats_df, "Match Name")
   strip_white_space(kills_stats_df, "Team")
   strip_white_space(kills_stats_df, "Player")

   kills_stats_df = await change_reference_name_to_id(kills_stats_df, year)
   kills_stats_df = convert_missing_number(kills_stats_df, "2k")
   kills_stats_df = convert_missing_number(kills_stats_df, "3k")
   kills_stats_df = convert_missing_number(kills_stats_df, "4k")
   kills_stats_df = convert_missing_number(kills_stats_df, "5k")
   kills_stats_df = convert_missing_number(kills_stats_df, "1v1")
   kills_stats_df = convert_missing_number(kills_stats_df, "1v2")
   kills_stats_df = convert_missing_number(kills_stats_df, "1v3")
   kills_stats_df = convert_missing_number(kills_stats_df, "1v4")
   kills_stats_df = convert_missing_number(kills_stats_df, "1v5")
   kills_stats_df = create_ids(kills_stats_df)
   kills_stats_df = drop_columns(kills_stats_df, ["Tournament", "Stage", "Match Type", "Match Name", "Team", "Player"])
   print(kills_stats_df.sample(n=20))


async def add_maps_played(file, year, engine):
   maps_played_df = csv_to_df(file)
   strip_white_space(maps_played_df, "Stage")
   strip_white_space(maps_played_df, "Match Type")
   strip_white_space(maps_played_df, "Match Name")
   maps_played_df = await change_reference_name_to_id(maps_played_df, year)
   maps_played_df = create_ids(maps_played_df)
   maps_played_df = drop_columns(maps_played_df, ["Tournament", "Stage", "Match Type", "Match Name"])
   print(maps_played_df.sample(n=20))

async def add_maps_scores(file, year, engine):
   maps_scores_df = csv_to_df(file)
   strip_white_space(maps_scores_df, "Stage")
   strip_white_space(maps_scores_df, "Match Type")
   strip_white_space(maps_scores_df, "Match Name")
   strip_white_space(maps_scores_df, "Team A")
   strip_white_space(maps_scores_df, "Team B")
   maps_scores_df = await change_reference_name_to_id(maps_scores_df, year)
   maps_scores_df = create_ids(maps_scores_df)
   maps_scores_df = drop_columns(maps_scores_df, ["Tournament", "Stage", "Match Type", "Match Name", "Team A", "Team B"])
   maps_scores_df = convert_missing_number(maps_scores_df, "Team A Overtime Score")
   maps_scores_df = convert_missing_number(maps_scores_df, "Team B Overtime Score")
   maps_scores_df = create_ids(maps_scores_df)
   print(maps_scores_df.sample(n=20))
   # print(f"Adding maps scores")
   # maps_scores = pd.read_csv("matches/maps_scores.csv")
   # query = """
   #    INSERT INTO maps_scores (
   #       tournament_id, stage_id, match_type_id, match_id, map_id, 
   #       team_a_id, team_a_score, team_a_attack_score, team_a_defender_score, team_a_overtime_score,
   #       team_b_id, team_b_score, team_b_attack_score, team_b_defender_score, team_b_overtime_score,
   #       duration
   #    ) VALUES (
   #       %s, %s, %s, %s, %s,
   #       %s, %s, %s, %s, %s,
   #       %s, %s, %s, %s, %s,
   #       %s
   #    );
   # """
   # # for index, row in maps_scores.iterrows():
   # tournament = row["Tournament"]
   # stage = row["Stage"]
   # match_type = row["Match Type"]
   # match_name = row["Match Name"]
   # map = row["Map"]
   # team_a = row["Team A"]
   # team_a_score = row["Team A Score"]
   # team_a_attack_score = row["Team A Attacker Score"]
   # team_a_defender_score = row["Team A Defender Score"]
   # team_a_overtime_score = check_na(row["Team A Overtime Score"], "int")
   # team_b = row["Team B"]
   # team_b_score = row["Team B Score"]
   # team_b_attack_score = row["Team B Attacker Score"]
   # team_b_defender_score = row["Team B Defender Score"]
   # team_b_overtime_score = check_na(row["Team B Overtime Score"], "int")
   # duration = check_na(row["Duration"], "interval")
   # tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   # stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   # match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   # match_id = retrieve_foreign_key(curr, "match_id", "matches", "match_name", match_name)
   # map_id = retrieve_foreign_key(curr, "map_id", "maps", "map_name", map)
   # team_a_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", team_a)
   # team_b_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", team_b)
   # data = (tournament_id, stage_id, match_type_id, match_id, map_id,
   #          team_a_id, team_a_score, team_a_attack_score, team_a_defender_score, team_a_overtime_score,
   #          team_b_id, team_b_score, team_b_attack_score, team_b_defender_score, team_b_overtime_score,
   #          duration)
   # execute_query(curr, query, data)
   # print(f"Done adding maps scores")
   # await asyncio.sleep(0)


async def add_overview(file, year, engine):
   overview_df = csv_to_df(file)
   strip_white_space(overview_df, "Stage")
   strip_white_space(overview_df, "Match Type")
   strip_white_space(overview_df, "Match Name")
   strip_white_space(overview_df, "Team")
   overview_df = await change_reference_name_to_id(overview_df, year)
   overview_df = convert_missing_number(overview_df)
   overview_df = create_ids(overview_df)
   overview_df = drop_columns(overview_df, ["Tournament", "Stage", "Match Type", "Match Name", "Player", "Team"])
   print(overview_df.sample(n=20))
   # print("Adding overview")
   # overview = pd.read_csv("matches/overview.csv")
   # query = """
   #    INSERT INTO overview (
   #       tournament_id, stage_id, match_type_id, match_id, map_id, player_id, team_id, agent_id,
   #       rating, average_combat_score, kills, deaths, assists, kill_deaths, kast_percentage, adr, headshot_percentage, first_kills, first_deaths, fkd, side
   #    )
   #    VALUES (
   #       %s, %s, %s, %s, %s, %s, %s, %s,
   #       %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
   #    );
   # """
   # for index, row in overview.iterrows():
   # tournament = row["Tournament"]
   # stage = row["Stage"]
   # match_type = row["Match Type"]
   # match_name = row["Match Name"]
   # map = row["Map"]
   # team = row["Team"]
   # player = row["Player"]
   # agent = row["Agents"]
   # rating = check_na(row["Rating"], "int")
   # average_combat_score = check_na(row["Average Combat Score"], "int")
   # kills = check_na(row["Kills"], "int")
   # deaths = check_na(row["Deaths"], "int")
   # assists = check_na(row["Assists"], "int")
   # kill_deaths = check_na(row["Kill - Deaths (KD)"], "int")
   # kast = check_na(row["Kill, Assist, Trade, Survive %"], "percentage")
   # adr = check_na(row["Average Damage per Round"], "int")
   # headshot_percentage = check_na(row["Headshot %"], "percentage")
   # first_kills = check_na(row["First Kills"], "int")
   # first_deaths = check_na(row["First Deaths"], "int")
   # fkd = check_na(row["Kills - Deaths (FKD)"], "int")
   # side = row["Side"]
   # tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   # stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   # match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   # match_id = retrieve_foreign_key(curr, "match_id", "matches", "match_name", match_name)
   # map_id = retrieve_foreign_key(curr, "map_id", "maps", "map_name", map)
   # team_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", team)
   # player_id = retrieve_foreign_key(curr, "player_id", "players", "player_name", player)
   # agent_id = retrieve_foreign_key(curr, "agent_id", "agents", "agent_name", agent)
   # data = (tournament_id, stage_id, match_type_id, match_id, map_id, player_id, team_id, agent_id,
   #          rating, average_combat_score, kills, deaths, assists, kill_deaths, kast, adr, headshot_percentage, first_kills, first_deaths, fkd, side)
   # execute_query(curr, query, data)
   # print(f"Done adding overview")
   # await asyncio.sleep(0)


async def add_rounds_kills(df, year, engine):
   rounds_kills_df = csv_to_df(df)
   strip_white_space(rounds_kills_df, "Stage")
   strip_white_space(rounds_kills_df, "Match Type")
   strip_white_space(rounds_kills_df, "Match Name")
   strip_white_space(rounds_kills_df, "Eliminator Team")
   strip_white_space(rounds_kills_df, "Eliminated Team")
   rounds_kills_df = await change_reference_name_to_id(rounds_kills_df, year)
   rounds_kills_df = create_ids(rounds_kills_df)
   rounds_kills_df = drop_columns(rounds_kills_df, ["Tournament", "Stage", "Match Type", "Match Name", "Eliminator", "Eliminator Team", "Eliminated", "Eliminated Team"])
   print(rounds_kills_df.sample(n=20))

   # print(f"Adding rounds kills")
   # rounds_kills = pd.read_csv("matches/rounds_kills.csv")
   # print(list(rounds_kills.columns))
   # query = """
   #    INSERT INTO rounds_kills (
   #       tournament_id, stage_id, match_type_id, match_id, map_id, round_number,
   #       eliminator_team_id, eliminator_id, eliminator_agent_id,
   #       eliminated_team_id, eliminated_id, eliminated_agent_id,
   #       kill_type
   #    ) VALUES (
   #       %s, %s, %s, %s, %s, %s,
   #       %s, %s, %s,
   #       %s, %s, %s,
   #       %s
   #    );
   # """
   # for index, row in rounds_kills.iterrows():
   # tournament = row["Tournament"]
   # stage = row["Stage"]
   # match_type = row["Match Type"]
   # match_name = row["Match Name"]
   # map = row["Map"]
   # round_number = row["Round Number"]
   # eliminator_team = row["Eliminator Team"]
   # eliminator = row["Eliminator"]
   # eliminator_agent = row["Eliminator Agent"]
   # eliminated_team = row['Eliminated Team']
   # eliminated = row["Eliminated"]
   # eliminated_agent = row["Eliminated Agent"]
   # kill_type = row["Kill Type"]
   # tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   # stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   # match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   # match_id = retrieve_foreign_key(curr, "match_id", "matches", "match_name", match_name)
   # map_id = retrieve_foreign_key(curr, "map_id", "maps", "map_name", map)
   # eliminator_team_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", eliminator_team)
   # eliminator_id = retrieve_foreign_key(curr, "player_id", "players", "player_name", eliminator)
   # eliminator_agent_id = retrieve_foreign_key(curr, "agent_id", "agents", "agent_name", eliminator_agent)
   # eliminated_team_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", eliminated_team)
   # eliminated_id = retrieve_foreign_key(curr, "player_id", "players", "player_name", eliminated)
   # eliminated_agent_id = retrieve_foreign_key(curr, "agent_id", "agents", "agent_name", eliminated_agent)
   # data = (tournament_id, stage_id, match_type_id, match_id, map_id, round_number,
   #          eliminator_team_id, eliminator_id, eliminator_agent_id,
   #          eliminated_team_id, eliminated_id, eliminated_agent_id,
   #          kill_type)
   # execute_query(curr, query, data)
   # print(f"Done adding rounds kills")
   # await asyncio.sleep(0)

async def add_scores(file, year, engine):
   scores_df = csv_to_df(file)
   strip_white_space(scores_df, "Stage")
   strip_white_space(scores_df, "Match Type")
   strip_white_space(scores_df, "Match Name")
   strip_white_space(scores_df, "Team A")
   strip_white_space(scores_df, "Team B")
   scores_df = await change_reference_name_to_id(scores_df, year)
   scores_df = create_ids(scores_df)
   scores_df = drop_columns(scores_df, ["Tournament", "Stage", "Match Type", "Match Name", "Team A", "Team B"])
   print(scores_df.sample(n=20))
   # print(f"Adding scores")
   # scores = pd.read_csv("matches/scores.csv")
   # query = """
   #    INSERT INTO scores (
   #       tournament_id, stage_id, match_type_id, match_id,
   #       winner_id, loser_id,
   #       winner_score, loser_score
   #    ) VALUES (
   #       %s, %s, %s, %s,
   #       %s, %s,
   #       %s, %s
   #    );
   # """
   # # for index, row in scores.iterrows():
   # tournament = row["Tournament"]
   # stage = row["Stage"]
   # match_type = row["Match Type"]
   # match_name = row["Match Name"]
   # winner = row["Winner"]
   # loser = row["Loser"]
   # winner_score = row["Winner Score"]
   # loser_score = row["Loser Score"]
   # tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   # stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   # match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   # match_id = retrieve_foreign_key(curr, "match_id", "matches", "match_name", match_name)
   # winner_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", winner)
   # loser_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", loser)
   # data = (tournament_id, stage_id, match_type_id, match_id,
   #          winner_id, loser_id,
   #          winner_score, loser_score)
   # execute_query(curr, query, data)
   # print(f"Done adding scores")
   # await asyncio.sleep(0)

async def add_win_loss_methods_count(file, year, engine):
   win_loss_methods_count_df = csv_to_df(file)
   strip_white_space(win_loss_methods_count_df, "Stage")
   strip_white_space(win_loss_methods_count_df, "Match Type")
   strip_white_space(win_loss_methods_count_df, "Match Name")
   strip_white_space(win_loss_methods_count_df, "Team")
   win_loss_methods_count_df = await change_reference_name_to_id(win_loss_methods_count_df, year)
   win_loss_methods_count_df = create_ids(win_loss_methods_count_df)
   win_loss_methods_count_df = drop_columns(win_loss_methods_count_df, ["Tournament", "Stage", "Match Type", "Match Name", "Team"])
   print(win_loss_methods_count_df.sample(n=20))
   

async def add_win_loss_methods_round_number(file, year, engine):
   win_loss_methods_round_number_df = csv_to_df(file)
   strip_white_space(win_loss_methods_round_number_df, "Stage")
   strip_white_space(win_loss_methods_round_number_df, "Match Type")
   strip_white_space(win_loss_methods_round_number_df, "Match Name")
   strip_white_space(win_loss_methods_round_number_df, "Team")
   win_loss_methods_round_number_df = await change_reference_name_to_id(win_loss_methods_round_number_df, year)
   win_loss_methods_round_number_df = create_ids(win_loss_methods_round_number_df)
   win_loss_methods_round_number_df = drop_columns(win_loss_methods_round_number_df, ["Tournament", "Stage", "Match Type", "Match Name", "Team"])
   print(win_loss_methods_round_number_df.sample(n=20))

async def add_agents_pick_rates(file, year, engine):
   agents_pick_rates_df = csv_to_df(file)
   strip_white_space(agents_pick_rates_df, "Stage")
   strip_white_space(agents_pick_rates_df, "Match Type")
   agents_pick_rates_df = await change_reference_name_to_id(agents_pick_rates_df, year)
   agents_pick_rates_df = create_ids(agents_pick_rates_df)
   agents_pick_rates_df = drop_columns(agents_pick_rates_df, ["Tournament", "Stage", "Match Type"])
   print(agents_pick_rates_df.sample(n=20))

   # print(f"Adding agents pick rates")
   # pick_rates = pd.read_csv("agents/agents_pick_rates.csv")
   # query = """
   #    INSERT INTO agents_pick_rates (
   #       tournament_id, stage_id, match_type_id, map_id,
   #       agent_id, pick_rate
   #    ) VALUES (
   #       %s, %s, %s, %s,
   #       %s, %s
   #    );
   # """
   # for index, row in pick_rates.iterrows():
   # tournament = row["Tournament"]
   # stage = row["Stage"]
   # match_type = row["Match Type"]
   # map = row["Map"]
   # agent = row["Agent"]
   # pick_rate = check_na(row["Pick Rate"], "percentage")
   # tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   # stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   # match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   # map_id = retrieve_foreign_key(curr, "map_id", "maps", "map_name", map)
   # agent_id = retrieve_foreign_key(curr, "agent_id", "agents", "agent_name", agent)
   # data = (tournament_id, stage_id, match_type_id, map_id, agent_id, pick_rate)
   # execute_query(curr, query, data)
   # print(f"Done adding agents pick rates")
   # await asyncio.sleep(0)

async def add_maps_stats(file, year, engine):
   maps_stats_df = csv_to_df(file)
   strip_white_space(maps_stats_df, "Stage")
   strip_white_space(maps_stats_df, "Match Type")
   maps_stats_df = await change_reference_name_to_id(maps_stats_df, year)
   maps_stats_df = create_ids(maps_stats_df)
   maps_stats_df = drop_columns(maps_stats_df, ["Tournament", "Stage", "Match Type"])
   print(maps_stats_df.sample(n=20))

   # print(f"Adding maps stats")
   # maps_stats = pd.read_csv("agents/maps_stats.csv")
   # query = """
   #    INSERT INTO maps_stats (
   #       tournament_id, stage_id, match_type_id, map_id, 
   #       total_maps_played, attacker_win_percentage, defender_win_percentage
   #    ) VALUES (
   #       %s, %s, %s, %s,
   #       %s, %s, %s
   #    );
   # """
   # for index, row in maps_stats.iterrows():
   # tournament = row["Tournament"]
   # stage = row["Stage"]
   # match_type = row["Match Type"]
   # map = row["Map"]
   # total_maps_played = row["Total Maps Played"]
   # attacker_win_percentage = float(row["Attacker Side Win Percentage"].strip("%")) / 100.0
   # defender_win_percentage = float(row["Defender Side Win Percentage"].strip("%")) / 100.0
   # tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   # stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   # match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   # map_id = retrieve_foreign_key(curr, "map_id", "maps", "map_name", map)

   # data = (tournament_id, stage_id, match_type_id, map_id, total_maps_played, attacker_win_percentage, defender_win_percentage)
   # execute_query(curr, query, data)
   # print(f"Done adding maps stats")
   # await asyncio.sleep(0)

async def add_teams_picked_agents(file, year, engine):
   teams_picked_agents_df = csv_to_df(file)
   strip_white_space(teams_picked_agents_df, "Stage")
   strip_white_space(teams_picked_agents_df, "Match Type")
   strip_white_space(teams_picked_agents_df, "Team")
   teams_picked_agents_df = teams_picked_agents_df[teams_picked_agents_df["Team"] != "Typhone"]
   teams_picked_agents_df = await change_reference_name_to_id(teams_picked_agents_df, year)
   teams_picked_agents_df = create_ids(teams_picked_agents_df)
   teams_picked_agents_df = drop_columns(teams_picked_agents_df, ["Tournament", "Stage", "Match Type"])
   print(teams_picked_agents_df.sample(n=20))
   # print(f"Adding teams picked agents")
   # teams_picked_agents = pd.read_csv("agents/teams_picked_agents.csv")
   # query = """
   #    INSERT INTO teams_picked_agents (
   #       tournament_id, stage_id, match_type_id, map_id, team_id, 
   #       agent_id, total_wins_by_map, total_loss_by_map, total_maps_played
   #    ) VALUES (
   #       %s, %s, %s, %s, %s,
   #       %s, %s, %s, %s
   #    );
   # """
   # for index, row in teams_picked_agents.iterrows():
   # tournament = row["Tournament"]
   # stage = row["Stage"]
   # match_type = row["Match Type"]
   # map = row["Map"]
   # team = row["Team"]
   # agent = row["Agent Picked"]
   # total_wins_by_map = row["Total Wins By Map"]
   # total_loss_by_map = row["Total Loss By Map"]
   # total_maps_played = row["Total Maps Played"]
   # tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   # stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   # match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   # map_id = retrieve_foreign_key(curr, "map_id", "maps", "map_name", map)
   # team_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", team)
   # agent_id = retrieve_foreign_key(curr, "agent_id", "agents", "agent_name", agent)

   # data = (tournament_id, stage_id, match_type_id, map_id, team_id, agent_id, total_wins_by_map, total_loss_by_map, total_maps_played)
   # execute_query(curr, query, data)
   # print(f"Done adding teams picked agents")
   # await asyncio.sleep(0)


async def add_players_stats(file, year, engine):
   players_stats_df = csv_to_df(file)
   print(players_stats_df[players_stats_df["Player"].isnull()])
   # print(players_stats_df[players_stats_df["Player"] == "2"])
   # players_stats_df = convert_column_to_str(players_stats_df, "Team")
   players_stats_df = await change_reference_name_to_id(players_stats_df, year)
   players_stats_df = create_ids(players_stats_df)
   players_stats_df = drop_columns(players_stats_df, ["Tournament", "Stage", "Match Type", "Player", "Team"])
   print(players_stats_df.sample(n=20))
   # print("Adding players stats")
   # players_stats = pd.read_csv("players_stats/players_stats.csv")
   # query = """
   #    INSERT INTO players_stats (
   #       tournament_id, stage_id, match_type_id, player_id, team_id, agents_id,
   #       rounds_played, rating, average_combat_score, kills_deaths, kast, adr,
   #       kills_per_round, assists_per_round, first_kills_per_round, first_deaths_per_round,
   #       headshot_percentage, clutch_success, clutches_won, clutches_played, mksp,
   #       kills, deaths, assists, first_kills, first_deaths
   #    ) VALUES (
   #       %s, %s, %s, %s, %s, %s,
   #       %s, %s, %s, %s, %s, %s,
   #       %s, %s, %s, %s,
   #       %s, %s, %s, %s, %s,
   #       %s, %s, %s, %s, %s
   #    );
   # """
   # for index, row in players_stats.iterrows():
   # tournament = row["Tournament"]
   # stage = row["Stage"]
   # match_type = row["Match Type"]
   # player = row["Player"]
   # team = row["Team"]
   # agents = row["Agents"]
   # rounds_played = row["Rounds Played"]
   # rating = check_na(row["Rating"], "int")
   # average_combat_score = check_na(row["Average Combat Score"], "int")
   # kills_deaths = row["Kills:Deaths"]
   # kast = check_na(row["Kill, Assist, Trade, Survive %"], "percentage")
   # adr = check_na(row["Average Damage per Round"], "int")
   # kills_per_round = row["Kills Per Round"]
   # assists_per_round = row["Assists Per Round"]
   # first_kills_per_round = check_na(row["First Kills Per Round"], "int")
   # first_deaths_per_round = check_na(row["First Deaths Per Round"], "int")
   # headshot_percentage = check_na(row["Headshot %"], "percentage")
   
   # clutch_success = check_na(row["Clutch Success %"], "percentage")

   # clutches_won, clutches_played = check_na(row["Clutches (won/played)"], "fraction")

   # mksp = row["Maximum Kills in a Single Map"]
   # kills = row["Kills"]
   # deaths = row["Deaths"]
   # assists = row["Assists"]
   # first_kills = row["First Kills"]
   # first_deaths = row["First Deaths"]
   # tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   # stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   # match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   # player_id = retrieve_foreign_key(curr, "player_id", "players", "player_name", player)
   # team_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", team)
   # agent_id = retrieve_foreign_key(curr, "agent_id", "agents", "agent_name", agents)
   # data = (tournament_id, stage_id, match_type_id, player_id, team_id, agent_id,
   #          rounds_played, rating, average_combat_score, kills_deaths, kast, adr,
   #          kills_per_round, assists_per_round, first_kills_per_round, first_deaths_per_round,
   #          headshot_percentage, clutch_success, clutches_won, clutches_played,
   #          mksp, kills, deaths, assists, first_kills, first_deaths)      
   # execute_query(curr, query, data)
   # print("Done adding players stats")
   # await asyncio.sleep(0)

# def add_all_data(curr, unique_ids):
#    add_tournaments(curr, unique_ids)
#    add_stages(curr, unique_ids)
#    add_match_types(curr, unique_ids)
#    add_matches(curr, unique_ids)
#    add_maps(curr, unique_ids)
#    add_teams(curr, unique_ids)
#    add_players(curr, unique_ids)
#    add_agents(curr, unique_ids)
   # add_drafts(curr)
   # add_eco_rounds(curr)
   # add_eco_stats(curr)
   # add_kills(curr)
   # add_kills_stats(curr)
   # add_maps_played(curr)
   # add_maps_scores(curr)
   # add_overview(curr)
   # add_rounds_kills(curr)
   # add_scores(curr)
   # add_agents_pick_rates(curr)
   # add_maps_stats(curr)
   # add_teams_picked_agents(curr)
   # add_players_stats(curr)
      
# def add_ids(curr, unique_ids):
#    add_tournaments(curr, unique_ids)
#    add_stages(curr, unique_ids)
#    add_match_types(curr, unique_ids)
#    add_matches(curr, unique_ids)
#    add_maps(curr, unique_ids)
#    add_teams(curr, unique_ids)
#    add_players(curr, unique_ids)
#    add_agents(curr, unique_ids)

# async def all_data_table_functions(curr):
#    return [add_drafts(curr), add_eco_rounds(curr), add_eco_stats(curr),
#            add_kills(curr), add_kills_stats(curr), add_maps_played(curr),
#            add_maps_scores(curr), add_overview(curr), add_rounds_kills(curr),
#            add_scores(curr), add_agents_pick_rates(curr), add_maps_stats(curr),
#            add_teams_picked_agents(curr), add_players_stats(curr)]
   
