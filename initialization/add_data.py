from Connect.execute_query import execute_query
import pandas as pd
from retrieve.retrieve import retrieve_primary_key
from checking.check_values import check_na
import asyncio
import time
from datetime import datetime
from process_df.process_df import rename_columns, reorder_columns, change_reference_name_to_id, csv_to_df, create_ids, convert_column_to_int, strip_white_space



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

def add_games(df, engine):
   games_df = df[["Tournament ID", "Stage ID", "Match Type ID", "Match ID", "Game ID", "Map", "Year"]]
   games_df = games_df.drop_duplicates()
   games_df = reorder_columns(games_df, ["Game ID", "Tournament ID", "Stage ID", "Match Type ID", "Match ID", "Map", "Year"])
   games_df = rename_columns(games_df, {"Game ID": "game_id", "Tournament ID": "tournament_id", "Stage ID": "stage_id",
                                 "Match Type ID": "match_type_id", "Match ID": "match_id", 
                                 "Map": "map", "Year": "year"})
   games_df.to_sql("games", engine, index=False, if_exists="append")



def add_teams(df, engine):
   teams_df = df[["Team", "Team ID"]]
   teams_df = teams_df.drop_duplicates()
   teams_df = reorder_columns(teams_df, {"Team ID", "Team"})
   teams_df = rename_columns(teams_df, {"Team ID": "team_id", "Team": "team"})
   teams_df.to_sql("teams", engine, index=False, if_exists="append")

def add_players(df, engine):
   players_df = df[["Player", "Player ID"]]
   players_df = players_df.drop_duplicates()
   players_df = reorder_columns(players_df, {"Player ID", "Player"})
   players_df = rename_columns(players_df, {"Player ID": "player_id", "Player": "player"})
   players_df.to_sql("players", engine, index=False, if_exists="append")

async def insert_data(curr, dataframe, insertion_function, table_name):
   start_time = time.time()
   now = datetime.now()

   print(f"Adding data to {table_name}")
   tasks = [insertion_function(curr, row) for _, row in dataframe.iterrows()]
   await asyncio.gather(*tasks)
   print(f"Done adding data to {table_name}")
   end_time = time.time()
   elasped_time = end_time - start_time
   hours, remainder = divmod(elasped_time, 3600)
   minutes, seconds = divmod(remainder, 60)

   print(f"Inserting data to {table_name} time: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")
   

def add_drafts(file, year, curr, engine):
   drafts_df = csv_to_df(file)
   strip_white_space(drafts_df, "Match Type")
   strip_white_space(drafts_df, "Match Name")
   drafts_df = change_reference_name_to_id(drafts_df, year, curr)
   print(drafts_df.sample(n=20))
   drafts_df["year"] = int(year)
   # drafts_df = rename_columns(drafts_df, {"Tournament": "tournament_id", "Stage": "stage_id", "Match Type": "match_type_id", "Match Name": "match_id",
   #                                        "Team": "team_id", "Action": "action", "Map": "map"})
   # drafts_df = reorder_columns(drafts_df, ["tournament_id", "stage_id", "match_type_id", "match_id", "team_id", "action", "map", "year"])
   # print(drafts_df["match_type_id"].unique())
   # drafts_df = convert_column_to_int(drafts_df, "match_type_id")
   # drafts_df = convert_column_to_int(drafts_df, "match_id")
   # drafts_df = convert_column_to_int(drafts_df, "team_id")
   # drafts_df = create_ids(drafts_df, "drafts_id")
   # print(drafts_df.dtypes)
   # drafts_df.to_sql("drafts", engine, index=False, if_exists="append")

async def add_eco_rounds(curr, row):
   # print(f"Adding eco rounds")
   # eco_rounds = pd.read_csv("matches/eco_rounds.csv")
   query = """
      INSERT INTO eco_rounds (
         tournament_id, stage_id, match_type_id, match_id, map_id,
         round_number, team_id, credits, eco_type, outcome
      ) VALUES (
         %s, %s, %s, %s, %s,
         %s, %s, %s, %s, %s
      );
   """
   # for index, row in eco_rounds.iterrows():
   tournament = row["Tournament"]
   stage = row["Stage"]
   match_type = row["Match Type"]
   match_name = row["Match Name"]
   map = row["Map"]
   round_number = row["Round Number"]
   team = row["Team"]
   credits = row["Credits"]
   eco_type = row["Type"]
   outcome = row["Outcome"]
   tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   match_id = retrieve_foreign_key(curr, "match_id", "matches", "match_name", match_name)
   team_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", team)
   map_id = retrieve_foreign_key(curr, "map_id", "maps", "map_name", map)
   data = (tournament_id, stage_id, match_type_id, match_id, map_id, round_number, team_id, credits, eco_type, outcome)
   execute_query(curr, query, data)
   # print(f"Done adding eco rounds")
   # await asyncio.sleep(0)
      
async def add_eco_stats(curr, row):
   # print(f"Adding eco stats")
   # eco_stats = pd.read_csv("matches/eco_stats.csv")
   query = """
      INSERT INTO eco_stats (
         tournament_id, stage_id, match_type_id, match_id, map_id,
         team_id, eco_type, initiated, won
      ) VALUES (
         %s, %s, %s, %s, %s,
         %s, %s, %s, %s
      );
   """
   # for index, row in eco_stats.iterrows():
   tournament = row["Tournament"]
   stage = row["Stage"]
   match_type = row["Match Type"]
   match_name = row["Match Name"]
   map = row["Map"]
   team = row["Team"]
   eco_type = row["Type"]
   initiated = check_na(row["Initiated"], "int")
   won = row["Won"]
   tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   match_id = retrieve_foreign_key(curr, "match_id", "matches", "match_name", match_name)
   team_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", team)
   map_id = retrieve_foreign_key(curr, "map_id", "maps", "map_name", map)
   data = (tournament_id, stage_id, match_type_id, match_id, map_id, team_id, eco_type, initiated, won)
   execute_query(curr, query, data)
   # print(f"Done adding eco stats")
   # await asyncio.sleep(0)
      

async def add_kills(curr, row):
   # print(f"Adding kills")
   # kills = pd.read_csv("matches/kills.csv")
   query = """
      INSERT INTO kills (
         tournament_id, stage_id, match_type_id, match_id, map_id, 
         player_team_id, player_id,
         enemy_team_id, enemy_id,
         player_kills, enemy_kills, difference, kill_type
      ) VALUES (
         %s, %s, %s, %s, %s,
         %s, %s,
         %s, %s,
         %s, %s, %s, %s
      );
   """
   # for index, row in kills.iterrows():
   tournament = row["Tournament"]
   stage = row["Stage"]
   match_type = row["Match Type"]
   match_name = row["Match Name"]
   map = row["Map"]
   player_team = row["Player Team"]
   player = row["Player"]
   enemy_team = row["Enemy Team"]
   enemy = row["Enemy"]
   player_kills = check_na(row["Player Kills"], "int")

   enemy_kills = check_na(row["Enemy Kills"], "int")
   
   difference = check_na(row["Difference"], "int")

   kill_type = row["Kill Type"]
   tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   match_id = retrieve_foreign_key(curr, "match_id", "matches", "match_name", match_name)
   map_id = retrieve_foreign_key(curr, "map_id", "maps", "map_name", map)
   player_team_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", player_team)
   player_id = retrieve_foreign_key(curr, "player_id", "players", "player_name", player)
   enemy_team_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", enemy_team)
   enemy_id = retrieve_foreign_key(curr, "player_id", "players", "player_name", enemy)
   data = (tournament_id, stage_id, match_type_id, match_id, map_id, player_team_id, player_id, enemy_team_id,
         enemy_id, player_kills, enemy_kills, difference, kill_type)
   execute_query(curr, query, data)
   # except:
   #    print(row)
   #    print(tournament, stage, match_type, match_name, map, player_team, player, enemy_team, enemy)
   #    print(data)
   # print(f"Done adding kills")
   # await asyncio.sleep(0)


async def add_kills_stats(curr, row):
   # print(f"Adding kills stats")
   # kills_stats = pd.read_csv("matches/kills_stats.csv")
   query = """
      INSERT INTO kills_stats (
         tournament_id, stage_id, match_type_id, match_id, map_id, team_id, player_id, agent_id,
         two_kills, three_kills, four_kills, five_kills, one_vs_one, one_vs_two, one_vs_three, one_vs_four, one_vs_five,
         econ, spike_plants, spike_defuse
      )
      VALUES (
         %s, %s, %s, %s, %s, %s, %s, %s,
         %s, %s, %s, %s, %s, %s, %s, %s, %s,
         %s, %s, %s
      );
   """
   # for index, row in kills_stats.iterrows():
   tournament = row["Tournament"]
   stage = row["Stage"]
   match_type = row["Match Type"]
   match_name = row["Match Name"]
   map = row["Map"]
   team = row["Team"]
   player = row["Player"]
   agent = row["Agent"]
   two_kills = check_na(row["2k"], "int")
   three_kills = check_na(row["3k"], "int")
   four_kills = check_na(row["4k"], "int")
   five_kills = check_na(row["5k"], "int")
   one_vs_one = check_na(row["1v1"], "int")
   one_vs_two = check_na(row["1v2"], "int")
   one_vs_three = check_na(row["1v3"], "int")
   one_vs_four = check_na(row["1v4"], "int")
   one_vs_five = check_na(row["1v5"], "int")
   econ = row["Econ"]
   spike_plants = row["Spike Plants"]
   spike_defuse = row["Spike Defuse"]
   tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   match_id = retrieve_foreign_key(curr, "match_id", "matches", "match_name", match_name)
   map_id = retrieve_foreign_key(curr, "map_id", "maps", "map_name", map)
   team_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", team)
   player_id = retrieve_foreign_key(curr, "player_id", "players", "player_name", player)
   agent_id = retrieve_foreign_key(curr, "agent_id", "agents", "agent_name", agent)
   data = (tournament_id, stage_id, match_type_id, match_id, map_id, team_id, player_id, agent_id,
            two_kills, three_kills, four_kills, five_kills, one_vs_one, one_vs_two, one_vs_three,
            one_vs_four, one_vs_five, econ, spike_plants, spike_defuse)
   execute_query(curr, query, data)
   # print("Done adding kills stats")
   # await asyncio.sleep(0)

async def add_maps_played(curr, row):
   # print(f"Adding maps played")
   # maps_played = pd.read_csv("matches/maps_played.csv")
   query = """
      INSERT INTO maps_played (
         tournament_id, stage_id, match_type_id, match_id, map_id
      ) VALUES (
         %s, %s, %s, %s, %s
      )
   """
   # for index, row in maps_played.iterrows():
   tournament = row["Tournament"]
   stage = row["Stage"]
   match_type = row["Match Type"]
   match_name = row["Match Name"]
   map = row["Map"]
   tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   match_id = retrieve_foreign_key(curr, "match_id", "matches", "match_name", match_name)
   map_id = retrieve_foreign_key(curr, "map_id", "maps", "map_name", map)
   data = (tournament_id, stage_id, match_type_id, match_id, map_id)
   execute_query(curr, query, data)
   # print(f"Done adding maps played")
   # await asyncio.sleep(0)

async def add_maps_scores(curr, row):
   # print(f"Adding maps scores")
   # maps_scores = pd.read_csv("matches/maps_scores.csv")
   query = """
      INSERT INTO maps_scores (
         tournament_id, stage_id, match_type_id, match_id, map_id, 
         team_a_id, team_a_score, team_a_attack_score, team_a_defender_score, team_a_overtime_score,
         team_b_id, team_b_score, team_b_attack_score, team_b_defender_score, team_b_overtime_score,
         duration
      ) VALUES (
         %s, %s, %s, %s, %s,
         %s, %s, %s, %s, %s,
         %s, %s, %s, %s, %s,
         %s
      );
   """
   # for index, row in maps_scores.iterrows():
   tournament = row["Tournament"]
   stage = row["Stage"]
   match_type = row["Match Type"]
   match_name = row["Match Name"]
   map = row["Map"]
   team_a = row["Team A"]
   team_a_score = row["Team A Score"]
   team_a_attack_score = row["Team A Attacker Score"]
   team_a_defender_score = row["Team A Defender Score"]
   team_a_overtime_score = check_na(row["Team A Overtime Score"], "int")
   team_b = row["Team B"]
   team_b_score = row["Team B Score"]
   team_b_attack_score = row["Team B Attacker Score"]
   team_b_defender_score = row["Team B Defender Score"]
   team_b_overtime_score = check_na(row["Team B Overtime Score"], "int")
   duration = check_na(row["Duration"], "interval")
   tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   match_id = retrieve_foreign_key(curr, "match_id", "matches", "match_name", match_name)
   map_id = retrieve_foreign_key(curr, "map_id", "maps", "map_name", map)
   team_a_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", team_a)
   team_b_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", team_b)
   data = (tournament_id, stage_id, match_type_id, match_id, map_id,
            team_a_id, team_a_score, team_a_attack_score, team_a_defender_score, team_a_overtime_score,
            team_b_id, team_b_score, team_b_attack_score, team_b_defender_score, team_b_overtime_score,
            duration)
   execute_query(curr, query, data)
   # print(f"Done adding maps scores")
   # await asyncio.sleep(0)


async def add_overview(curr, row):
   # print("Adding overview")
   # overview = pd.read_csv("matches/overview.csv")
   query = """
      INSERT INTO overview (
         tournament_id, stage_id, match_type_id, match_id, map_id, player_id, team_id, agent_id,
         rating, average_combat_score, kills, deaths, assists, kill_deaths, kast_percentage, adr, headshot_percentage, first_kills, first_deaths, fkd, side
      )
      VALUES (
         %s, %s, %s, %s, %s, %s, %s, %s,
         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
      );
   """
   # for index, row in overview.iterrows():
   tournament = row["Tournament"]
   stage = row["Stage"]
   match_type = row["Match Type"]
   match_name = row["Match Name"]
   map = row["Map"]
   team = row["Team"]
   player = row["Player"]
   agent = row["Agents"]
   rating = check_na(row["Rating"], "int")
   average_combat_score = check_na(row["Average Combat Score"], "int")
   kills = check_na(row["Kills"], "int")
   deaths = check_na(row["Deaths"], "int")
   assists = check_na(row["Assists"], "int")
   kill_deaths = check_na(row["Kill - Deaths (KD)"], "int")
   kast = check_na(row["Kill, Assist, Trade, Survive %"], "percentage")
   adr = check_na(row["Average Damage per Round"], "int")
   headshot_percentage = check_na(row["Headshot %"], "percentage")
   first_kills = check_na(row["First Kills"], "int")
   first_deaths = check_na(row["First Deaths"], "int")
   fkd = check_na(row["Kills - Deaths (FKD)"], "int")
   side = row["Side"]
   tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   match_id = retrieve_foreign_key(curr, "match_id", "matches", "match_name", match_name)
   map_id = retrieve_foreign_key(curr, "map_id", "maps", "map_name", map)
   team_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", team)
   player_id = retrieve_foreign_key(curr, "player_id", "players", "player_name", player)
   agent_id = retrieve_foreign_key(curr, "agent_id", "agents", "agent_name", agent)
   data = (tournament_id, stage_id, match_type_id, match_id, map_id, player_id, team_id, agent_id,
            rating, average_combat_score, kills, deaths, assists, kill_deaths, kast, adr, headshot_percentage, first_kills, first_deaths, fkd, side)
   execute_query(curr, query, data)
   # print(f"Done adding overview")
   # await asyncio.sleep(0)


async def add_rounds_kills(curr, row):
   # print(f"Adding rounds kills")
   # rounds_kills = pd.read_csv("matches/rounds_kills.csv")
   # print(list(rounds_kills.columns))
   query = """
      INSERT INTO rounds_kills (
         tournament_id, stage_id, match_type_id, match_id, map_id, round_number,
         eliminator_team_id, eliminator_id, eliminator_agent_id,
         eliminated_team_id, eliminated_id, eliminated_agent_id,
         kill_type
      ) VALUES (
         %s, %s, %s, %s, %s, %s,
         %s, %s, %s,
         %s, %s, %s,
         %s
      );
   """
   # for index, row in rounds_kills.iterrows():
   tournament = row["Tournament"]
   stage = row["Stage"]
   match_type = row["Match Type"]
   match_name = row["Match Name"]
   map = row["Map"]
   round_number = row["Round Number"]
   eliminator_team = row["Eliminator Team"]
   eliminator = row["Eliminator"]
   eliminator_agent = row["Eliminator Agent"]
   eliminated_team = row['Eliminated Team']
   eliminated = row["Eliminated"]
   eliminated_agent = row["Eliminated Agent"]
   kill_type = row["Kill Type"]
   tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   match_id = retrieve_foreign_key(curr, "match_id", "matches", "match_name", match_name)
   map_id = retrieve_foreign_key(curr, "map_id", "maps", "map_name", map)
   eliminator_team_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", eliminator_team)
   eliminator_id = retrieve_foreign_key(curr, "player_id", "players", "player_name", eliminator)
   eliminator_agent_id = retrieve_foreign_key(curr, "agent_id", "agents", "agent_name", eliminator_agent)
   eliminated_team_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", eliminated_team)
   eliminated_id = retrieve_foreign_key(curr, "player_id", "players", "player_name", eliminated)
   eliminated_agent_id = retrieve_foreign_key(curr, "agent_id", "agents", "agent_name", eliminated_agent)
   data = (tournament_id, stage_id, match_type_id, match_id, map_id, round_number,
            eliminator_team_id, eliminator_id, eliminator_agent_id,
            eliminated_team_id, eliminated_id, eliminated_agent_id,
            kill_type)
   execute_query(curr, query, data)
   # print(f"Done adding rounds kills")
   # await asyncio.sleep(0)

async def add_scores(curr, row):
   # print(f"Adding scores")
   # scores = pd.read_csv("matches/scores.csv")
   query = """
      INSERT INTO scores (
         tournament_id, stage_id, match_type_id, match_id,
         winner_id, loser_id,
         winner_score, loser_score
      ) VALUES (
         %s, %s, %s, %s,
         %s, %s,
         %s, %s
      );
   """
   # for index, row in scores.iterrows():
   tournament = row["Tournament"]
   stage = row["Stage"]
   match_type = row["Match Type"]
   match_name = row["Match Name"]
   winner = row["Winner"]
   loser = row["Loser"]
   winner_score = row["Winner Score"]
   loser_score = row["Loser Score"]
   tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   match_id = retrieve_foreign_key(curr, "match_id", "matches", "match_name", match_name)
   winner_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", winner)
   loser_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", loser)
   data = (tournament_id, stage_id, match_type_id, match_id,
            winner_id, loser_id,
            winner_score, loser_score)
   execute_query(curr, query, data)
   # print(f"Done adding scores")
   # await asyncio.sleep(0)
   

async def add_agents_pick_rates(curr, row):
   # print(f"Adding agents pick rates")
   # pick_rates = pd.read_csv("agents/agents_pick_rates.csv")
   query = """
      INSERT INTO agents_pick_rates (
         tournament_id, stage_id, match_type_id, map_id,
         agent_id, pick_rate
      ) VALUES (
         %s, %s, %s, %s,
         %s, %s
      );
   """
   # for index, row in pick_rates.iterrows():
   tournament = row["Tournament"]
   stage = row["Stage"]
   match_type = row["Match Type"]
   map = row["Map"]
   agent = row["Agent"]
   pick_rate = check_na(row["Pick Rate"], "percentage")
   tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   map_id = retrieve_foreign_key(curr, "map_id", "maps", "map_name", map)
   agent_id = retrieve_foreign_key(curr, "agent_id", "agents", "agent_name", agent)
   data = (tournament_id, stage_id, match_type_id, map_id, agent_id, pick_rate)
   execute_query(curr, query, data)
   # print(f"Done adding agents pick rates")
   # await asyncio.sleep(0)

async def add_maps_stats(curr, row):
   # print(f"Adding maps stats")
   # maps_stats = pd.read_csv("agents/maps_stats.csv")
   query = """
      INSERT INTO maps_stats (
         tournament_id, stage_id, match_type_id, map_id, 
         total_maps_played, attacker_win_percentage, defender_win_percentage
      ) VALUES (
         %s, %s, %s, %s,
         %s, %s, %s
      );
   """
   # for index, row in maps_stats.iterrows():
   tournament = row["Tournament"]
   stage = row["Stage"]
   match_type = row["Match Type"]
   map = row["Map"]
   total_maps_played = row["Total Maps Played"]
   attacker_win_percentage = float(row["Attacker Side Win Percentage"].strip("%")) / 100.0
   defender_win_percentage = float(row["Defender Side Win Percentage"].strip("%")) / 100.0
   tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   map_id = retrieve_foreign_key(curr, "map_id", "maps", "map_name", map)

   data = (tournament_id, stage_id, match_type_id, map_id, total_maps_played, attacker_win_percentage, defender_win_percentage)
   execute_query(curr, query, data)
   # print(f"Done adding maps stats")
   # await asyncio.sleep(0)

async def add_teams_picked_agents(curr, row):
   # print(f"Adding teams picked agents")
   # teams_picked_agents = pd.read_csv("agents/teams_picked_agents.csv")
   query = """
      INSERT INTO teams_picked_agents (
         tournament_id, stage_id, match_type_id, map_id, team_id, 
         agent_id, total_wins_by_map, total_loss_by_map, total_maps_played
      ) VALUES (
         %s, %s, %s, %s, %s,
         %s, %s, %s, %s
      );
   """
   # for index, row in teams_picked_agents.iterrows():
   tournament = row["Tournament"]
   stage = row["Stage"]
   match_type = row["Match Type"]
   map = row["Map"]
   team = row["Team"]
   agent = row["Agent Picked"]
   total_wins_by_map = row["Total Wins By Map"]
   total_loss_by_map = row["Total Loss By Map"]
   total_maps_played = row["Total Maps Played"]
   tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   map_id = retrieve_foreign_key(curr, "map_id", "maps", "map_name", map)
   team_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", team)
   agent_id = retrieve_foreign_key(curr, "agent_id", "agents", "agent_name", agent)

   data = (tournament_id, stage_id, match_type_id, map_id, team_id, agent_id, total_wins_by_map, total_loss_by_map, total_maps_played)
   execute_query(curr, query, data)
   # print(f"Done adding teams picked agents")
   # await asyncio.sleep(0)


async def add_players_stats(curr, row):
   # print("Adding players stats")
   # players_stats = pd.read_csv("players_stats/players_stats.csv")
   query = """
      INSERT INTO players_stats (
         tournament_id, stage_id, match_type_id, player_id, team_id, agents_id,
         rounds_played, rating, average_combat_score, kills_deaths, kast, adr,
         kills_per_round, assists_per_round, first_kills_per_round, first_deaths_per_round,
         headshot_percentage, clutch_success, clutches_won, clutches_played, mksp,
         kills, deaths, assists, first_kills, first_deaths
      ) VALUES (
         %s, %s, %s, %s, %s, %s,
         %s, %s, %s, %s, %s, %s,
         %s, %s, %s, %s,
         %s, %s, %s, %s, %s,
         %s, %s, %s, %s, %s
      );
   """
   # for index, row in players_stats.iterrows():
   tournament = row["Tournament"]
   stage = row["Stage"]
   match_type = row["Match Type"]
   player = row["Player"]
   team = row["Team"]
   agents = row["Agents"]
   rounds_played = row["Rounds Played"]
   rating = check_na(row["Rating"], "int")
   average_combat_score = check_na(row["Average Combat Score"], "int")
   kills_deaths = row["Kills:Deaths"]
   kast = check_na(row["Kill, Assist, Trade, Survive %"], "percentage")
   adr = check_na(row["Average Damage per Round"], "int")
   kills_per_round = row["Kills Per Round"]
   assists_per_round = row["Assists Per Round"]
   first_kills_per_round = check_na(row["First Kills Per Round"], "int")
   first_deaths_per_round = check_na(row["First Deaths Per Round"], "int")
   headshot_percentage = check_na(row["Headshot %"], "percentage")
   
   clutch_success = check_na(row["Clutch Success %"], "percentage")

   clutches_won, clutches_played = check_na(row["Clutches (won/played)"], "fraction")

   mksp = row["Maximum Kills in a Single Map"]
   kills = row["Kills"]
   deaths = row["Deaths"]
   assists = row["Assists"]
   first_kills = row["First Kills"]
   first_deaths = row["First Deaths"]
   tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament)
   stage_id = retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage)
   match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type)
   player_id = retrieve_foreign_key(curr, "player_id", "players", "player_name", player)
   team_id = retrieve_foreign_key(curr, "team_id", "teams", "team_name", team)
   agent_id = retrieve_foreign_key(curr, "agent_id", "agents", "agent_name", agents)
   data = (tournament_id, stage_id, match_type_id, player_id, team_id, agent_id,
            rounds_played, rating, average_combat_score, kills_deaths, kast, adr,
            kills_per_round, assists_per_round, first_kills_per_round, first_deaths_per_round,
            headshot_percentage, clutch_success, clutches_won, clutches_played,
            mksp, kills, deaths, assists, first_kills, first_deaths)      
   execute_query(curr, query, data)
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
      
def add_ids(curr, unique_ids):
   add_tournaments(curr, unique_ids)
   add_stages(curr, unique_ids)
   add_match_types(curr, unique_ids)
   add_matches(curr, unique_ids)
   add_maps(curr, unique_ids)
   add_teams(curr, unique_ids)
   add_players(curr, unique_ids)
   add_agents(curr, unique_ids)

# async def all_data_table_functions(curr):
#    return [add_drafts(curr), add_eco_rounds(curr), add_eco_stats(curr),
#            add_kills(curr), add_kills_stats(curr), add_maps_played(curr),
#            add_maps_scores(curr), add_overview(curr), add_rounds_kills(curr),
#            add_scores(curr), add_agents_pick_rates(curr), add_maps_stats(curr),
#            add_teams_picked_agents(curr), add_players_stats(curr)]