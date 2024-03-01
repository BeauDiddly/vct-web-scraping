import pandas as pd
from checking.check_values import check_na
from retrieve.retrieve import retrieve_primary_key
from Connect.connect import create_pool, create_db_url
import asyncpg
import asyncio
import numpy as np

def strip_white_space(df, column_name):
    df.loc[:, column_name] = df[column_name].str.strip()

def create_ids(df):
    df.reset_index(inplace=True)
    return df

def convert_column_to_int(df, column_name):
    df[column_name] = pd.to_numeric(df[column_name]).astype(int)
    return df

def drop_columns(df, column_names):
    df = df.drop(columns=column_names)
    return df

def reorder_columns(df, column_names):
    return df.reindex(columns=column_names)

def rename_columns(df, columns_names):
    return df.rename(columns=columns_names)

def csv_to_df(file):
    return pd.read_csv(file)

def create_tuples(df):
    tuples = [tuple(x) for x in df.values]
    return tuples

def create_conditions_values_1d(df, ids, column):
    conditions, values = [], []
    for id in ids:
        name, id = next(iter(id.items()), (None, None))
        conditions.append(df[column] == name)
        values.append(id)
    return conditions, values


def create_stages_conditions_values(df, ids):
    conditions = []
    values = []
    for id in ids:
        for key, value in id.items():
            conditions.append((df["Tournament ID"] == key[0]) & (df["Stage"] == key[1]))
            values.append(value)
    return conditions, values

def create_match_types_conditions_values(df, ids):
    conditions = []
    values = []
    for id in ids:
        for key, value in id.items():
            conditions.append((df["Tournament ID"] == key[0]) & (df["Stage ID"] == key[1]) & (df["Match Type"] == key[2]))
            values.append(value)
    return conditions, values

def create_matches_conditions_values(df, ids):
    conditions = []
    values = []
    for id in ids:
        for key, value in id.items():
            conditions.append((df["Tournament ID"] == key[0]) & (df["Stage ID"] == key[1]) & (df["Match Type ID"] == key[2]) & (df["Match Name"] == key[3]))
            values.append(value)
    return conditions, values

async def change_reference_name_to_id(df, year, db_conn_info):
    db_url = create_db_url()
    # pool = await create_pool(**db_conn_info)
    async with asyncpg.create_pool(db_url) as pool:
        async with pool.acquire() as conn:
            if "Tournament" in df:
                tournaments = df["Tournament"].unique().tolist()
                tournament_ids = [await retrieve_primary_key(pool, "tournament_id", "tournaments", "tournament", tournament, year) for tournament in tournaments]
                conditions, values = create_conditions_values_1d(df, tournament_ids, "Tournament")
                df["Tournament ID"] = np.select(conditions, values, default=None)
 
            if "Stage" in df:
                stages = df[["Tournament ID", "Stage"]].drop_duplicates()
                tuples = create_tuples(stages)
                stage_ids = [await retrieve_primary_key(conn, "stage_id", "stages", "stage", (tournament_id, stage), year) 
                                  for tournament_id, stage in tuples]
                conditions, values = create_stages_conditions_values(df, stage_ids)
                df["Stage ID"] = np.select(conditions, values, default=None)
            if "Match Type" in df:
                match_types = df[["Tournament ID", "Stage ID", "Match Type"]].drop_duplicates()
                tuples = create_tuples(match_types)
                match_types_ids = [await retrieve_primary_key(conn, "match_type_id", "match_types", "match_type", (tournament_id, stage_id, match_type), year) 
                                    for tournament_id, stage_id, match_type in tuples]
                conditions, values = create_match_types_conditions_values(df, match_types_ids)
                df["Match Type ID"] = np.select(conditions, values, default=None)
            if "Match Name" in df:
                matches = df[["Tournament ID", "Stage ID", "Match Type ID", "Match Name"]].drop_duplicates()
                tuples = create_tuples(matches)
                matches_id = [await retrieve_primary_key(conn, "match_id", "matches", "match", (tournament_id, stage_id, match_type_id, match_name), year)
                                 for tournament_id, stage_id, match_type_id, match_name in tuples]
                conditions, values = create_matches_conditions_values(df, matches_id)
                df["Match ID"] = np.select(conditions, values, default=None) 
            if "Team" in df or "Enemy Team" in df or "Eliminator Team" in df or "Eliminated Team" in df:
                for column in ["Team", "Enemy Team", "Eliminator Team", "Eliminated Team"]:
                    if column in df:
                        teams = df[column].unique().tolist()
                        team_ids = [await retrieve_primary_key(pool, "team_id", "teams", "team", team)
                                    for team in teams]
                        conditions, values = create_conditions_values_1d(df, team_ids, column)
                        df[f"{column} ID"] = np.select(conditions, values, default=None)
            if "Player" in df or "Enemy" in df or "Eliminator" in df or "Eliminated" in df:
                for column in ["Player", "Enemy", "Eliminator", "Eliminated"]:
                    if column in df:
                        players = df[column].unique().tolist()
                        player_ids = [await retrieve_primary_key(pool, "player_id", "players", "player", player)
                                      for player in players]
                        conditions, values = create_conditions_values_1d(df, player_ids, column)
                        df[f"{column} ID"] = np.select(conditions, values, default=None)
    return df


# def process_eco_stats_df(df):
#     change_reference_name_to_id(df)
#     df["Initiated"] = df["Initiated"].apply(lambda col: check_na(col["Initiated"], "int"), axis = 1)


# def process_kills_df(df):
#     change_reference_name_to_id(df)
#     df["Player Kills"] = df["Player Kills"].apply(lambda col: check_na(col["Player Kills"], "int"), axis = 1)
#     df["Enemy Kills"] = df["Enemy Kills"].apply(lambda col: check_na(col["Enemy Kills"], "int"), axis = 1)
#     df["Difference"] = df["Difference"].apply(lambda col: check_na(col["Difference"], "int"), axis = 1)

# def process_kills_stats_df(df):
#     change_reference_name_to_id(df)
#     df["2k"] = df["2k"].apply(lambda col: check_na(col["2k"], "int"), axis = 1)
#     df["3k"] = df["3k"].apply(lambda col: check_na(col["3k"], "int"), axis = 1)
#     df["4k"] = df["4k"].apply(lambda col: check_na(col["4k"], "int"), axis = 1)
#     df["5k"] = df["5k"].apply(lambda col: check_na(col["5k"], "int"), axis = 1)
#     df["1v1"] = df["1v1"].apply(lambda col: check_na(col["1v1"], "int"), axis = 1)
#     df["1v2"] = df["1v2"].apply(lambda col: check_na(col["1v2"], "int"), axis = 1)
#     df["1v3"] = df["1v3"].apply(lambda col: check_na(col["1v3"], "int"), axis = 1)
#     df["1v4"] = df["1v4"].apply(lambda col: check_na(col["1v4"], "int"), axis = 1)
#     df["1v5"] =  df["1v5"].apply(lambda col: check_na(col["1v5"], "int"), axis = 1)

# def process_maps_scores_df(df):
#     change_reference_name_to_id(df)
#     df["Team A Overtime Score"] = df["Team A Overtime Score"].apply(lambda col: check_na(col["Team A Overtime Score"], "int"), axis = 1)
#     df["Team B Overtime Score"] = df["Team B Overtime Score"].apply(lambda col: check_na(col["Team B Overtime Score"], "int"), axis = 1)
#     df["Duration"] = df["Duration"].apply(lambda col: check_na(col["Duration"], "interval"), axis = 1)

# def process_overview_df(df, curr):
#     change_reference_name_to_id(df)
#     df["Rating"] = df["Rating"].apply(lambda col: check_na(col["Rating"], "float"), axis = 1)
#     df["Average Combat Score"] = df["Average Combat Score"].apply(lambda col: check_na(col["Average Combat Score"], "int"), axis = 1)
#     df["Kills"]  = df["Kills"].apply(lambda col: check_na(col["Kills"], "int"), axis = 1)
#     df["Deaths"] = df["Deaths"].apply(lambda col: check_na(col["Deaths"], "int"), axis = 1)
#     df["Assists"] = df["Assists"].apply(lambda col: check_na(col["Assists"], "int"), axis = 1)
#     df["Kill - Deaths (KD)"] = df["Kill - Deaths (KD)"].apply(lambda col: check_na(col["Kill - Deaths (KD)"], "int"), axis = 1)
#     df["Kill, Assist, Trade, Survive %"] = df["Kill, Assist, Trade, Survive %"].apply(lambda col: check_na(col["Kill, Assist, Trade, Survive %"], "percentage"), axis = 1)
#     df["Average Damage per Round"] = df["Average Damage per Round"].apply(lambda col: check_na(col["Average Damage per Round"], "int"), axis = 1)
#     df["Headshot %"] = df["Headshot %"].apply(lambda col: check_na(col["Headshot %"], "percentage"), axis = 1)
#     df["First Kills"] = df["First Kills"].apply(lambda col: check_na(col["First Kills"], "int"), axis = 1)
#     df["First Deaths"] = df["First Deaths"].apply(lambda col: check_na(col["First Deaths"], "int"), axis = 1)
#     df["Kills - Deaths (FKD)"] = df["Kills - Deaths (FKD)"].apply(lambda col: check_na(col["Kills - Deaths (FKD)"], "int"), axis = 1)

# def process_agents_pick_rates_df(df):
#     change_reference_name_to_id(df)
#     df["Pick Rate"] = df["Pick Rate"].apply(lambda col: check_na(col["Pick Rate"], "percentage"), axis = 1)

# def process_maps_stats_df(df):
#     change_reference_name_to_id(df)
#     df["Attacker Side Win Percentage"] = df["Attacker Side Win Percentage"].apply(lambda col: float(col["Attacker Side Win Percentage"].strip("%")) / 100.0)
#     df["Defender Side Win Percentage"] = df["Defender Side Win Percentage"].apply(lambda col: float(col["Defender Side Win Percentage"].strip("%")) / 100.0)

# def process_players_stats_df(df):
#     change_reference_name_to_id(df)
#     df["Rating"] = df["Rating"].apply(lambda col: check_na(col["Rating"], "float"), axis = 1)
#     df["Kill, Assist, Trade, Survive %"] = df["Kill, Assist, Trade, Survive %"].apply(lambda col: check_na(col["Kill, Assist, Trade, Survive %"], "percentage"), axis = 1)
#     df["Average Damage per Round"] = df["Average Damage per Round"].apply(lambda col: check_na(col["Average Damage per Round"], "int"), axis = 1)
#     df["First Kills Per Round"] = df["First Kills Per Round"].apply()
#     df["First Deaths Per Round"] = df["First Deaths Per Round"].apply()
#     df["Headshot %"] = df["Headshot %"].apply(lambda col: check_na(col["Headshot %"], "percentage"), axis = 1)
#     df["Clutch Success %"] = df["Clutch Success %"].apply(lambda col: check_na(col["Clutch Success %"], "percentage"), axis = 1)
#     df["Clutches (won/played)"] = df["Clutches (won/played)"].apply(lambda col: check_na(col["Clutches (won/played)"]), axis = 1)


# def process_df(file_names, year):
#     files_df = {}
#     for file_name in file_names:
#         files_df[file_names] = pd.read_csv(file_names)

#     for file_name, df in files_df.items():
#         if file_name in ["draft_phrase.csv", "eco_rounds.csv", "eco_stats.csv", "maps_played.csv", "rounds_kills.csv", "scores.csv", "teams_picked_agents.csv"]:
#             change_reference_name_to_id(df)
#         elif file_name == "kills.csv":
#             process_kills_df(df)
#         elif file_name == "kills_stats.csv":
#             process_kills_stats_df(df)

#         elif file_name == "maps_scores.csv":
#             process_maps_scores_df(df)

#         elif file_name == "overview.csv":
#             process_overview_df(df)
        
#         elif file_name == "agents_pick_rates.csv":
#             process_agents_pick_rates_df(df)

#         elif file_name == "maps_stats.csv":
#             process_maps_stats_df(df)

#         elif file_name == "players_stats.csv":
#             process_players_stats_df(df)
#     return files_df
