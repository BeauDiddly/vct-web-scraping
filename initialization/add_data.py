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