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
   df = reorder_columns(df, ["Tournament ID", "Tournament", "Year"])
   df = rename_columns(df, {"Tournament ID": "tournament_id", "Tournament": "tournament", "Year": "year"})
   df.to_sql("tournaments", engine, index=False, if_exists = "append")
    
def add_stages(df, engine):
   df = df[["Tournament ID", "Stage", "Stage ID", "Year"]]
   df = df.drop_duplicates()
   df = reorder_columns(df, ["Stage ID", "Tournament ID", "Stage", "Year"])
   df = rename_columns(df, {"Stage ID": "stage_id", "Tournament ID": "tournament_id", "Stage": "stage", "Year": "year"})
   df.to_sql("stages", engine, index=False, if_exists="append")

def add_match_types(df, engine):
   df = df[["Tournament ID", "Stage ID", "Match Type", "Match Type ID", "Year"]]
   df = df.drop_duplicates()
   df = reorder_columns(df, ["Match Type ID", "Tournament ID", "Stage ID", "Match Type", "Year"])
   df = rename_columns(df, {"Match Type ID": "match_type_id", "Tournament ID": "tournament_id", "Stage ID": "stage_id", 
                                                   "Match Type": "match_type", "Year": "year"})
   df.to_sql("match_types", engine, index=False, if_exists="append")


def add_matches(df, upper_round_id, engine):
   filtered = df[(df["Tournament ID"] == 560) &
                  (df["Stage ID"] == 1096) &
                  (df["Match Type"] == "Upper Round 1")]
   df = df[["Tournament ID", "Stage ID", "Match Type ID", "Match Name", "Match ID", "Year"]]
   df.loc[filtered.index, "Match Type ID"] = upper_round_id
   df = df.drop_duplicates()
   df = reorder_columns(df, ["Match ID", "Tournament ID", "Stage ID", "Match Type ID", "Match Name", "Year"])
   df = rename_columns(df, {"Match ID": "match_id", "Tournament ID": "tournament_id",
                                             "Stage ID": "stage_id", "Match Type ID": "match_type_id", 
                                             "Match Name": "match", "Year": "year"})
   df.to_sql("matches", engine, index=False, if_exists="append")



def add_teams(df, engine):
   df = df[["Team", "Team ID"]]
   df = df.drop_duplicates()
   null_team_count, missing_team_id = get_missing_numbers(df, "Team ID")
   add_missing_ids(df, "Team ID", missing_team_id, null_team_count)
   df = reorder_columns(df, {"Team ID", "Team"})
   df = rename_columns(df, {"Team ID": "team_id", "Team": "team"})
   df.to_sql("teams", engine, index=False, if_exists="append")

def add_players(df, engine):
   df = df[["Player", "Player ID"]]
   df = df.drop_duplicates()
   null_player_count, missing_player_id = get_missing_numbers(df, "Player ID")
   add_missing_ids(df, "Player ID", missing_player_id, null_player_count)
   df = add_missing_player(df, 2021)
   df = reorder_columns(df, {"Player ID", "Player"})
   df = rename_columns(df, {"Player ID": "player_id", "Player": "player"})
   df.to_sql("players", engine, index=False, if_exists="append")

   

async def add_drafts(file, year, engine):
   drafts_df = csv_to_df(file)
   drafts_df = await change_reference_name_to_id(drafts_df, year)
   drafts_df = convert_column_to_int(drafts_df, "Team ID")
   drafts_df["year"] = year
   drafts_df = create_ids(drafts_df)
   drafts_df = drop_columns(drafts_df, ["Tournament", "Stage", "Match Type", "Match Name", "Team", "Map"])
   drafts_df = rename_columns(drafts_df, {"index": "draft_id","Tournament ID": "tournament_id", "Stage ID": "stage_id", "Match Type ID": "match_type_id", "Match ID": "match_id",
                                          "Team ID": "team_id", "Action": "action", "Map ID": "map_id"})
   drafts_df = reorder_columns(drafts_df, ["draft_id", "tournament_id", "stage_id", "match_type_id", "match_id", "team_id", "map_id", "action", "year"])
   print(drafts_df.sample(n=20))
   # drafts_df.to_sql("drafts", engine, index=False, if_exists="append")

async def add_eco_rounds(file, year, engine):
   eco_rounds_df = csv_to_df(file)
   eco_rounds_df = await change_reference_name_to_id(eco_rounds_df, year)
   eco_rounds_df["year"] = year
   eco_rounds_df = convert_column_to_int(eco_rounds_df, "Team ID")
   eco_rounds_df = k_to_numeric(eco_rounds_df, "Loadout Value")
   eco_rounds_df = k_to_numeric(eco_rounds_df, "Remaining Credits")
   eco_rounds_df = get_eco_type(eco_rounds_df, "Type")
   eco_rounds_df = create_ids(eco_rounds_df)
   eco_rounds_df = drop_columns(eco_rounds_df, ["Tournament", "Stage", "Match Type", "Match Name", "Team", "Map"])
   eco_rounds_df = rename_columns(eco_rounds_df, {"index": "eco_round_id","Tournament ID": "tournament_id", "Stage ID": "stage_id", "Match Type ID": "match_type_id", "Match ID": "match_id",
                                          "Team ID": "team_id", "Map ID": "map_id", "Round Number": "round_number", "Loadout Value": "loadout_value",
                                          "Remaining Credits": "credits", "Type": "eco_type", "Outcome": "outcome"})
   eco_rounds_df = reorder_columns(eco_rounds_df, ["eco_round_id", "tournament_id", "stage_id", "match_type_id", "match_id", "team_id",
                                             "map_id", "round_number", "loadout_value", "credits", "eco_type", "outcome", "year"])
   print(eco_rounds_df.sample(n=20))
   # eco_rounds_df.to_sql("eco_rounds", engine, index=False, if_exists="append", chunksize = 10000)
      
async def add_eco_stats(file, year, engine):
   eco_stats_df = csv_to_df(file)
   eco_stats_df = await change_reference_name_to_id(eco_stats_df, year)
   eco_stats_df["year"] = year
   eco_stats_df = convert_missing_numbers(eco_stats_df)
   eco_stats_df = convert_column_to_int(eco_stats_df, "Team ID")
   eco_stats_df = create_ids(eco_stats_df)
   eco_stats_df = drop_columns(eco_stats_df, ["Tournament", "Stage", "Match Type", "Match Name", "Team", "Map"])
   eco_stats_df = rename_columns(eco_stats_df, {"index": "eco_stat_id","Tournament ID": "tournament_id", "Stage ID": "stage_id", "Match Type ID": "match_type_id", "Match ID": "match_id",
                                          "Team ID": "team_id", "Map ID": "map_id", "Type": "type", "Initiated": "initiated", "Won": "won"})
   eco_stats_df = reorder_columns(eco_stats_df, ["eco_stat_id", "tournament_id", "stage_id", "match_type_id", "match_id", "team_id", "map_id", "type", "initiated", "won"])
   print(eco_stats_df.sample(n=20))
   # eco_stats_df.to_sql("eco_stats", engine, index=False, if_exists="append", chunksize = 10000)
   
      

async def add_kills(file, year, engine):
   kills_df = csv_to_df(file)
   
   kills_df = await change_reference_name_to_id(kills_df, year)
   kills_df = convert_missing_numbers(kills_df)
   kills_df = convert_column_to_int(kills_df, "Player Team ID")
   kills_df = convert_column_to_int(kills_df, "Enemy Team ID")
   kills_df = convert_column_to_int(kills_df, "Player ID")
   kills_df = convert_column_to_int(kills_df, "Enemy ID")
   kills_df = create_ids(kills_df)
   kills_df["year"] = year
   kills_df = drop_columns(kills_df, ["Tournament", "Stage", "Match Type", "Match Name", "Player Team", "Player", "Enemy Team", "Enemy", "Map"])
   kills_df = rename_columns(kills_df, {"index": "kills_id", "Tournament ID": "tournament_id", "Stage ID": "stage_id", "Match Type ID": "match_type_id",
                                        "Match ID": "match_id", "Player Team ID": "player_team_id", "Player ID": "player_id", "Enemy Team ID": "enemy_team_id",
                                        "Enemy ID": "enemy_id", "Map ID": "map_id", "Player Kills": "player_kills", "Enemy Kills": "enemy_kills", "Difference": "difference",
                                        "Kill Type": "kill_type"})
   kills_df = reorder_columns(kills_df, ["kills_id", "tournament_id", "stage_id", "match_type_id", "match_id", "player_team_id", "player_id", "enemy_team_id", "enemy_id",
                                         "map_id", "player_kills", "enemy_kills", "difference", "kill_type", "year"])
   print(kills_df.sample(n=20))
   
   # print(f"Adding kills")

async def add_kills_stats(file, year, engine):
   kills_stats_df = csv_to_df(file)
   kills_stats_df["year"] = year
   # multiple_agents_df = kills_stats_df[kills_stats_df["Agent"].str.contains(",")]
   # single_agents_df = kills_stats_df[-kills_stats_df["Agent"].str.contains(",")]
   kills_stats_df = await change_reference_name_to_id(kills_stats_df, year)
   kills_stats_df = convert_missing_numbers(kills_stats_df)
   kills_stats_df = create_ids(kills_stats_df)
   kills_stats_df = drop_columns(kills_stats_df, ["Tournament", "Stage", "Match Type", "Match Name", "Team", "Player", "Map"])
   kills_stats_df = rename_columns(kills_stats_df, {"index": "kills_stats_id", "Tournament ID": "tournament_id", "Stage ID": "stage_id", "Match Type ID": "match_type_id",
                                                    "Match ID": "match_id", "Map ID": "map_id", "Team ID": "team_id", "Player ID": "player_id", "Agents": "agents",
                                                    "Econ": "econ", "Spike Plants": "spike_plants", "Spike Defuses": "spike_defuses"})
   kills_stats_df = reorder_columns(kills_stats_df, ["kills_stats_id", "tournament_id", "stage_id", "match_type_id", "match_id", "team_id", "player_id", "map_id", "agents",
                                                     "2k", "3k", "4k", "5k", "1v1", "1v2", "1v3", "1v4", "1v5", "econ", "spike_plants", "spike_defuses"])
   agents_df = kills_stats_df[["kills_stats_id", "agents"]]
   agents_df = splitting_agents(agents_df)
   agents_df = rename_columns(agents_df, {"agents": "agent"})
   kills_stats_df = drop_columns(kills_stats_df, ["agents"])
   agents_df = await change_reference_name_to_id(agents_df, year)
   agents_df["year"] = year
   print(kills_stats_df.sample(n=20))
   print(agents_df)


async def add_maps_played(file, year, engine):
   maps_played_df = csv_to_df(file)
   maps_played_df = await change_reference_name_to_id(maps_played_df, year)
   maps_played_df = create_ids(maps_played_df)
   maps_played_df = drop_columns(maps_played_df, ["Tournament", "Stage", "Match Type", "Match Name", "Map"])
   maps_played_df = rename_columns(maps_played_df, {"index": "maps_played_id", "Tournament ID": "tournament_id", "Stage ID": "stage_id", "Match Type ID": "match_type_id",
                                                    "Match ID": "match_id", "Map ID": "map_id"})
   maps_played_df = reorder_columns(maps_played_df, ["maps_played_id", "tournament_id", "stage_id", "match_type_id", "match_id", "map_id"])
   print(maps_played_df.sample(n=20))

async def add_maps_scores(file, year, engine):
   maps_scores_df = csv_to_df(file)
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
   strip_white_space(overview_df, "Stage")
   strip_white_space(overview_df, "Match Type")
   strip_white_space(overview_df, "Match Name")
   strip_white_space(overview_df, "Team")
   overview_df = await change_reference_name_to_id(overview_df, year)
   overview_df = convert_missing_numbers(overview_df)
   overview_df = create_ids(overview_df)
   overview_df = drop_columns(overview_df, ["Tournament", "Stage", "Match Type", "Match Name", "Player", "Team"])
   print(overview_df.sample(n=20))



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


async def add_maps_stats(file, year, engine):
   maps_stats_df = csv_to_df(file)
   strip_white_space(maps_stats_df, "Stage")
   strip_white_space(maps_stats_df, "Match Type")
   maps_stats_df = await change_reference_name_to_id(maps_stats_df, year)
   maps_stats_df = create_ids(maps_stats_df)
   maps_stats_df = drop_columns(maps_stats_df, ["Tournament", "Stage", "Match Type"])
   print(maps_stats_df.sample(n=20))

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



async def add_players_stats(file, year, engine):
   players_stats_df = csv_to_df(file)
   print(players_stats_df[players_stats_df["Player"].isnull()])
   # print(players_stats_df[players_stats_df["Player"] == "2"])
   # players_stats_df = convert_column_to_str(players_stats_df, "Team")
   players_stats_df = await change_reference_name_to_id(players_stats_df, year)
   players_stats_df = create_ids(players_stats_df)
   players_stats_df = drop_columns(players_stats_df, ["Tournament", "Stage", "Match Type", "Player", "Team"])
   print(players_stats_df.sample(n=20))


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
   
