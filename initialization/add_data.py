from Connect.execute_query import execute_query
import pandas as pd
import numpy as np
from retrieve.retrieve import retrieve_primary_key
from checking.check_values import check_na
import asyncio
import time
from datetime import datetime
from process_df.process_df import *

def add_agents(engine):
   all_agents = ["astra", "breach", "brimstone", "chamber", "cypher", "deadlock", "fade", "gekko", "harbor", "iso", "jett", "kayo",
              "killjoy", "neon", "omen", "phoenix", "raze", "reyna", "sage", "skye", "sova", "viper", "yoru"]
   agent_ids = {agent: sum(ord(char) for char in agent) for agent in all_agents}
   df = pd.DataFrame(list(agent_ids.items()), columns=["agent", "agent_id"])
   df = reorder_columns(df, {"agent_id", "agent"})
   df.to_sql("agents", engine, index=False, if_exists="append")
   
def add_maps(engine):
   all_maps = ["Bind", "Haven", "Split", "Ascent", "Icebox", "Breeze", "Fracture", "Pearl", "Lotus", "Sunset", "All Maps"]
   map_ids = {map: sum(ord(char) for char in map) for map in all_maps}
   df = pd.DataFrame(list(map_ids.items()), columns=["map", "map_id"])
   df = reorder_columns(df, {"map_id", "map"})
   df.to_sql("maps", engine, index=False, if_exists="append")

def add_tournaments_stages_match_types(df, engine):
   df = df[["Tournament", "Tournament ID", "Stage", "Stage ID", "Match Type", "Match Type ID", "Year"]]
   df = df.drop_duplicates()
   null_stage_count, missing_stage_ids = get_missing_numbers(df, "Stage ID")
   null_match_type_count, missing_match_type_ids = get_missing_numbers(df, "Match Type ID")
   add_missing_ids(df, "Stage ID", missing_stage_ids, null_stage_count)
   add_missing_ids(df, "Match Type ID", missing_match_type_ids, null_match_type_count)
   add_tournaments(df, engine)
   add_stages(df, engine)
   add_match_types(df, engine)
   upper_round_df = df[(df["Tournament ID"] == 560) &
                       (df["Stage ID"] == 1096) &
                       (df["Match Type"] == "Upper Round 1")]
   upper_round_id = upper_round_df["Match Type ID"].values[0]
   return upper_round_id

def add_tournaments(df, engine):
   df = df[["Tournament", "Tournament ID", "Year"]]
   df = df.drop_duplicates()
   df = rename_columns(df)
   df = reorder_columns(df, ["tournament_id", "tournament", "year"])
   df.to_sql("tournaments", engine, index=False, if_exists = "append")
    
def add_stages(df, engine):
   df = df[["Tournament ID", "Stage", "Stage ID", "Year"]]
   df = df.drop_duplicates()
   df = rename_columns(df) 
   df = reorder_columns(df, ["stage_id", "tournament_id", "stage", "year"])
   df.to_sql("stages", engine, index=False, if_exists="append")

def add_match_types(df, engine):
   df = df[["Tournament ID", "Stage ID", "Match Type", "Match Type ID", "Year"]]
   df = df.drop_duplicates()
   df = rename_columns(df)
   df = reorder_columns(df, ["match_type_id", "tournament_id", "stage_id", "match_type", "year"])
   df.to_sql("match_types", engine, index=False, if_exists="append")


def add_matches(df, upper_round_id, engine):
   filtered = df[(df["Tournament ID"] == 560) &
                  (df["Stage ID"] == 1096) &
                  (df["Match Type"] == "Upper Round 1")]
   df = df[["Tournament ID", "Stage ID", "Match Type ID", "Match Name", "Match ID", "Year"]]
   df.loc[filtered.index, "Match Type ID"] = upper_round_id
   df = df.drop_duplicates()
   df.rename(columns={"Match Name": "Match"}, inplace=True)
   df = rename_columns(df)
   df = reorder_columns(df, ["match_id", "tournament_id", "stage_id", "match_type_id", "match", "year"])
   df.to_sql("matches", engine, index=False, if_exists="append")



def add_teams(df, engine):
   df = df[["Team", "Team ID"]]
   df = df.drop_duplicates()
   null_team_count, missing_team_id = get_missing_numbers(df, "Team ID")
   add_missing_ids(df, "Team ID", missing_team_id, null_team_count)
   df = rename_columns(df)
   df = reorder_columns(df, {"team_id", "team"})
   df.to_sql("teams", engine, index=False, if_exists="append")

def add_players(df, engine):
   df = df[["Player", "Player ID"]]
   df = df.drop_duplicates()
   null_player_count, missing_player_id = get_missing_numbers(df, "Player ID")
   add_missing_ids(df, "Player ID", missing_player_id, null_player_count)
   df = add_missing_player(df, 2021)
   df = remove_leading_zeroes_from_players(df)
   df = rename_columns(df)
   df = reorder_columns(df, {"player_id", "player"})
   df.to_sql("players", engine, index=False, if_exists="append")

   

async def add_drafts(file, year, engine):
   drafts_df = csv_to_df(file)
   drafts_df = convert_to_category(drafts_df)
   drafts_df = await change_reference_name_to_id(drafts_df, year)
   drafts_df["year"] = year
   drafts_df = create_ids(drafts_df)
   drafts_df = drop_columns(drafts_df)
   drafts_df = rename_columns(drafts_df)
   drafts_df = reorder_columns(drafts_df, ["index", "tournament_id", "stage_id", "match_type_id", "match_id", "team_id", "map_id", "action", "year"])
   print(drafts_df.sample(n=20))
   # drafts_df.to_sql("drafts", engine, index=False, if_exists="append")

async def add_eco_rounds(file, year, engine):
   eco_rounds_df = csv_to_df(file)
   eco_rounds_df = convert_to_category(eco_rounds_df)
   eco_rounds_df = await change_reference_name_to_id(eco_rounds_df, year)
   eco_rounds_df["year"] = year
   eco_rounds_df = k_to_numeric(eco_rounds_df, "Loadout Value")
   eco_rounds_df = k_to_numeric(eco_rounds_df, "Remaining Credits")
   eco_rounds_df = get_eco_type(eco_rounds_df)
   eco_rounds_df = create_ids(eco_rounds_df)
   eco_rounds_df = drop_columns(eco_rounds_df)
   eco_rounds_df = rename_columns(eco_rounds_df)
   eco_rounds_df = reorder_columns(eco_rounds_df, ["index", "tournament_id", "stage_id", "match_type_id", "match_id", "team_id",
                                             "map_id", "round_number", "loadout_value", "remaining_credits", "type", "outcome", "year"])
   print(eco_rounds_df.sample(n=20))
   # eco_rounds_df.to_sql("eco_rounds", engine, index=False, if_exists="append", chunksize = 10000)
      
async def add_eco_stats(file, year, engine):
   eco_stats_df = csv_to_df(file)
   eco_stats_df = convert_to_category(eco_stats_df)
   eco_stats_df = await change_reference_name_to_id(eco_stats_df, year)
   eco_stats_df["year"] = year
   eco_stats_df = convert_missing_numbers(eco_stats_df)
   eco_stats_df = create_ids(eco_stats_df)
   eco_stats_df = drop_columns(eco_stats_df)
   eco_stats_df = rename_columns(eco_stats_df)
   eco_stats_df = reorder_columns(eco_stats_df, ["index", "tournament_id", "stage_id", "match_type_id", "match_id", "team_id", "map_id", "type", "initiated", "won",
                                                 "year"])
   print(eco_stats_df.sample(n=20))
   # eco_stats_df.to_sql("eco_stats", engine, index=False, if_exists="append", chunksize = 10000)
   
      

async def add_kills(file, year, engine):
   kills_df = csv_to_df(file)
   kills_df = remove_leading_zeroes_from_players(kills_df)
   kills_df = convert_to_category(kills_df)
   kills_df = await change_reference_name_to_id(kills_df, year)
   kills_df = convert_missing_numbers(kills_df)
   kills_df = create_ids(kills_df)
   kills_df["year"] = year
   kills_df = drop_columns(kills_df)
   kills_df = rename_columns(kills_df)
   kills_df = reorder_columns(kills_df, ["index", "tournament_id", "stage_id", "match_type_id", "match_id", "player_team_id", "player_id", "enemy_team_id", "enemy_id",
                                         "map_id", "player_kills", "enemy_kills", "difference", "kill_type", "year"])
   print(kills_df.sample(n=20))
   
   # print(f"Adding kills")

async def add_kills_stats(file, year, engine):
   kills_stats_df = csv_to_df(file)
   kills_stats_df = remove_leading_zeroes_from_players(kills_stats_df)
   kills_stats_df = convert_to_category(kills_stats_df)
   kills_stats_df["year"] = year
   kills_stats_df = await change_reference_name_to_id(kills_stats_df, year)
   kills_stats_df = convert_missing_numbers(kills_stats_df)
   kills_stats_df = create_ids(kills_stats_df)
   kills_stats_df = drop_columns(kills_stats_df)
   kills_stats_df = rename_columns(kills_stats_df)
   kills_stats_df = reorder_columns(kills_stats_df, ["index", "tournament_id", "stage_id", "match_type_id", "match_id", "team_id", "player_id", "map_id", "agents",
                                                     "2k", "3k", "4k", "5k", "1v1", "1v2", "1v3", "1v4", "1v5", "econ", "spike_plants", "spike_defuses"])
   agents_df = kills_stats_df[["index", "agents"]]
   agents_df = splitting_agents(agents_df)
   agents_df.rename(columns={"agents": "agent"}, inplace=True)
   kills_stats_df.drop(columns="agents", inplace=True)
   agents_df = await change_reference_name_to_id(agents_df, year)
   agents_df["year"] = year
   print(kills_stats_df.sample(n=20))
   print(agents_df.sample(n=20))


async def add_maps_played(file, year, engine):
   maps_played_df = csv_to_df(file)
   maps_played_df = convert_to_category(maps_played_df)
   maps_played_df = await change_reference_name_to_id(maps_played_df, year)
   maps_played_df = create_ids(maps_played_df)
   maps_played_df = drop_columns(maps_played_df)
   maps_played_df = rename_columns(maps_played_df)
   maps_played_df = reorder_columns(maps_played_df, ["index", "tournament_id", "stage_id", "match_type_id", "match_id", "map_id"])
   print(maps_played_df.sample(n=20))

async def add_maps_scores(file, year, engine):
   maps_scores_df = csv_to_df(file)
   maps_scores_df = convert_to_category(maps_scores_df)
   maps_scores_df = await change_reference_name_to_id(maps_scores_df, year)
   maps_scores_df = create_ids(maps_scores_df)
   maps_scores_df = drop_columns(maps_scores_df)
   maps_scores_df = standardized_duration(maps_scores_df)
   maps_scores_df = convert_missing_numbers(maps_scores_df)
   maps_scores_df = rename_columns(maps_scores_df)
   maps_scores_df = reorder_columns(maps_scores_df, ["index", "tournament_id", "stage_id", "match_type_id", "match_id", "map_id", "team_a_id",
                                                     "team_b_id", "team_a_score", "team_a_attacker_score", "team_a_defender_score",
                                                     "team_a_overtime_score", "team_b_score", "team_b_attacker_score",
                                                     "team_b_defender_score", "team_b_overtime_score", "duration"])
   maps_scores_df["year"]= year
   print(maps_scores_df.sample(n=20))


async def add_overview(file, year, engine):
   overview_df = csv_to_df(file)
   overview_df = remove_leading_zeroes_from_players(overview_df)
   overview_df = convert_to_category(overview_df)
   overview_df = await change_reference_name_to_id(overview_df, year)
   overview_df = drop_columns(overview_df)
   overview_df = convert_percentages(overview_df)
   overview_df = convert_missing_numbers(overview_df)
   overview_df = create_ids(overview_df)
   overview_df = rename_columns(overview_df)
   print(overview_df.columns)
   overview_df = reorder_columns(overview_df, ["index", "tournament_id", "stage_id", "match_type_id", "match_id", "map_id", "player_id",  "team_id",
                                               "agents", "rating", "acs", "kills", "deaths", "assists", "kd", "kast", "adpr", "headshot", "fk",
                                               "fd", "fkd", "side"])
   overview_df["year"] = year
   agents_df = overview_df[["index", "agents"]]
   agents_df = splitting_agents(agents_df)
   agents_df.rename(columns={"agents": "agent"}, inplace=True)
   overview_df.drop(columns="agents", inplace=True)
   agents_df = await change_reference_name_to_id(agents_df, year)
   agents_df.drop(columns="agent", inplace=True)
   agents_df["year"] = year
   print(overview_df.sample(n=20))
   print(agents_df.sample(n=20))



async def add_rounds_kills(df, year, engine):
   rounds_kills_df = csv_to_df(df)
   rounds_kills_df = remove_leading_zeroes_from_players(rounds_kills_df)
   rounds_kills_df = convert_to_category(rounds_kills_df)
   rounds_kills_df = await change_reference_name_to_id(rounds_kills_df, year)
   rounds_kills_df = drop_columns(rounds_kills_df)
   rounds_kills_df = create_ids(rounds_kills_df)
   rounds_kills_df = rename_columns(rounds_kills_df)
   rounds_kills_df = reorder_columns(rounds_kills_df, ["index", "tournament_id", "stage_id", "match_type_id", "match_id",
                                                       "map_id", "eliminator_team_id", "eliminated_team_id",
                                                       "eliminator_id", "eliminated_id", "eliminator_agent_id", "eliminated_agent_id",
                                                       "round_number", "kill_type"])
   rounds_kills_df["year"] = year
   print(rounds_kills_df.sample(n=20))


async def add_scores(file, year, engine):
   scores_df = csv_to_df(file)
   scores_df = convert_to_category(scores_df)
   scores_df = await change_reference_name_to_id(scores_df, year)
   scores_df = create_ids(scores_df)
   scores_df = drop_columns(scores_df)
   scores_df = rename_columns(scores_df)
   scores_df["year"] = year
   scores_df = reorder_columns(scores_df, ["index", "tournament_id", "stage_id", "match_type_id", "match_id", "team_a_id", "team_b_id",
                                            "team_a_score", "team_b_score", "match_result", "year"])
   print(scores_df.sample(n=20))


async def add_win_loss_methods_count(file, year, engine):
   win_loss_methods_count_df = csv_to_df(file)
   win_loss_methods_count_df = convert_to_category(win_loss_methods_count_df)
   win_loss_methods_count_df = await change_reference_name_to_id(win_loss_methods_count_df, year)
   win_loss_methods_count_df = create_ids(win_loss_methods_count_df)
   win_loss_methods_count_df = drop_columns(win_loss_methods_count_df)
   win_loss_methods_count_df = rename_columns(win_loss_methods_count_df)
   win_loss_methods_count_df["year"] = year
   win_loss_methods_count_df = reorder_columns(win_loss_methods_count_df, ["index", "tournament_id", "stage_id", "match_type_id", "match_id", "team_id",
                                                                           "map_id", 'elimination', 'detonated', 'defused', 'time_expiry_no_plant', "eliminated",
                                                                           'defused_failed', 'detonation_denied', 'time_expiry_failed_to_plant', "year"])
   print(win_loss_methods_count_df.sample(n=20))
   

async def add_win_loss_methods_round_number(file, year, engine):
   win_loss_methods_round_number_df = csv_to_df(file)
   win_loss_methods_round_number_df = convert_to_category(win_loss_methods_round_number_df)
   win_loss_methods_round_number_df = await change_reference_name_to_id(win_loss_methods_round_number_df, year)
   win_loss_methods_round_number_df = create_ids(win_loss_methods_round_number_df)
   win_loss_methods_round_number_df = drop_columns(win_loss_methods_round_number_df)
   win_loss_methods_round_number_df = rename_columns(win_loss_methods_round_number_df)
   win_loss_methods_round_number_df["year"] = year
   win_loss_methods_round_number_df = reorder_columns(win_loss_methods_round_number_df, ["index", "tournament_id", "stage_id", "match_type_id", "match_id", "team_id",
                                                                                         "map_id", "round_number", "method", "outcome", "year"])
   print(win_loss_methods_round_number_df.sample(n=20))

async def add_agents_pick_rates(file, year, engine):
   agents_pick_rates_df = csv_to_df(file)
   agents_pick_rates_df = await change_reference_name_to_id(agents_pick_rates_df, year)
   agents_pick_rates_df = create_ids(agents_pick_rates_df)
   agents_pick_rates_df = drop_columns(agents_pick_rates_df)
   agents_pick_rates_df = convert_percentages(agents_pick_rates_df)
   agents_pick_rates_df = rename_columns(agents_pick_rates_df)
   agents_pick_rates_df["year"] = year
   agents_pick_rates_df = reorder_columns(agents_pick_rates_df, ["index", "tournament_id", "stage_id", "match_type_id", "map_id", "agent_id",
                                                                 "pick_rate", "year"])
   print(agents_pick_rates_df.sample(n=20))


async def add_maps_stats(file, year, engine):
   maps_stats_df = csv_to_df(file)
   maps_stats_df = await change_reference_name_to_id(maps_stats_df, year)
   maps_stats_df = create_ids(maps_stats_df)
   maps_stats_df = drop_columns(maps_stats_df)
   maps_stats_df = convert_percentages(maps_stats_df)
   maps_stats_df = rename_columns(maps_stats_df)
   maps_stats_df['year'] = year
   maps_stats_df = reorder_columns(maps_stats_df, ["index", "tournament_id", "stage_id", "match_type_id", "map_id", "total_maps_played",
                                                   "attacker_side_win_percentage", "defender_side_win_percentage", "year"])
   print(maps_stats_df.sample(n=20))

async def add_teams_picked_agents(file, year, engine):
   teams_picked_agents_df = csv_to_df(file)
   teams_picked_agents_df = await change_reference_name_to_id(teams_picked_agents_df, year)
   teams_picked_agents_df = create_ids(teams_picked_agents_df)
   teams_picked_agents_df = drop_columns(teams_picked_agents_df)
   teams_picked_agents_df = rename_columns(teams_picked_agents_df)
   teams_picked_agents_df["year"] = year
   teams_picked_agents_df = reorder_columns(teams_picked_agents_df, ["index", "tournament_id", "stage_id", "match_type_id", "map_id",
                                                                     "agent_id", "total_wins_by_map", "total_loss_by_map", "total_maps_played",
                                                                     "year"])
   print(teams_picked_agents_df.sample(n=20))



async def add_players_stats(file, year, engine):
   players_stats_df = csv_to_df(file)
   players_stats_df = convert_to_category(players_stats_df)
   print(players_stats_df[players_stats_df["Player"] == "1000010"])
   players_stats_df = await change_reference_name_to_id(players_stats_df, year)
   players_stats_df = create_ids(players_stats_df)
   players_stats_df = convert_clutches(players_stats_df)
   players_stats_df = drop_columns(players_stats_df)
   players_stats_df = convert_percentages(players_stats_df)
   players_stats_df = rename_columns(players_stats_df)
   players_stats_df["year"] = year
   players_stats_df = reorder_columns(players_stats_df, ["index", "tournament_id", "stage_id", "match_type_id", "player_id", "teams", "agents", "rounds_played",
                                                         "rating", "acs", "kd", "kast", "adr", "kpr", "apr", "fkpr", "fdpr", "headshot",
                                                         "clutch_success", "clutches_won", "clutches_played", "mksp", "kills", "deaths", "assists",
                                                         "fk", "fd", "year"])
   agents_df = players_stats_df[["index", "agents"]]
   agents_df = splitting_agents(agents_df)
   agents_df.rename(columns={"agents": "agent"}, inplace=True)
   players_stats_df.drop(columns="agents", inplace=True)
   teams_df = players_stats_df[["index", "teams"]]
   teams_df = splitting_teams(teams_df)
   teams_df.rename(columns={"teams": "team"}, inplace=True)
   players_stats_df.drop(columns="teams", inplace=True)
   teams_df = await change_reference_name_to_id(teams_df, year)
   teams_df["year"] = year
   print(players_stats_df.sample(n=20))
   print(agents_df.sample(n=20))
   print(teams_df.sample(n=20))


async def process_csv_files(csv_files, year, engine):
   for csv_file in csv_files:
      file_name = csv_file.split("/")[2]
      print(file_name, year)
      if year == 0:
         print("Yes")
      # if file_name == "draft_phase.csv":
      #    await add_drafts(csv_file, year, engine)
      # elif file_name == "eco_rounds.csv":
      #    await add_eco_rounds(csv_file, year, engine)
      # elif file_name == "eco_stats.csv":
      #    await add_eco_stats(csv_file, year, engine)
      # elif file_name == "kills.csv":
      #    await add_kills(csv_file, year, engine)
      # elif file_name == "kills_stats.csv":
      #    await add_kills_stats(csv_file, year, engine)
      # elif file_name == "maps_played.csv":
      #    await add_maps_played(csv_file, year, engine)
      # elif file_name == "maps_scores.csv":
      #    await add_maps_scores(csv_file, year, engine)
      elif file_name == "overview.csv":
         await add_overview(csv_file, year, engine)
      # elif file_name == "rounds_kills.csv":
      #    await add_rounds_kills(csv_file, year, engine)
      # elif file_name == "scores.csv":
      #    await add_scores(csv_file, year, engine)
      # elif file_name == "win_loss_methods_count.csv":
      #    await add_win_loss_methods_count(csv_file, year, engine)
      # elif file_name == "win_loss_methods_round_number.csv":
      #    await add_win_loss_methods_round_number(csv_file, year, engine)

async def process_year(year, csv_files, engine):
   await process_csv_files(csv_files, year, engine)



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
   
