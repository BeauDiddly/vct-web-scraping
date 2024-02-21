import pandas as pd
from checking.check_values import check_na
from retrieve.retrieve import retrieve_foreign_key

def reorder_columns(df, column_names):
    return df.reindex(columns=column_names)

def rename_columns(df, columns_names):
    return df.rename(columns=columns_names)

# def change_reference_name_to_id(df):
#     if "Tournament" in df:
#         df["Tournament"] = df["Tournament"].apply(lambda tournament: retrieve_foreign_key(curr, "tournament_id", "tournaments", "tournament_name", tournament))
#     if "Stage" in df:
#         df["Stage"] = df["Stage"].apply(lambda stage: retrieve_foreign_key(curr, "stage_id", "stages", "stage_name", stage))
#     if "Match Type" in df:
#         df["Match Type"] = df["Match_type"].apply(lambda match_type: retrieve_foreign_key(curr, "match_type_id", "match_types", "match_type_name", match_type))
#     if "Match Name" in df:
#         df["Match Name"] = df["Match name"].apply(lambda match_name: retrieve_foreign_key(curr, "match name_id", "match names", "match name_name", match_name))
#     if "Map" in df:
#         df["Map"] = df["Map"].apply(lambda map: retrieve_foreign_key(curr, "map_id", "maps", "map_name", map))
#     if "Team" in df:
#         df["Team"] = df["Team"].apply(lambda col: check_na(col["Team"], "string"), axis = 1)
#         df["Team"] = df["Team"].apply(lambda team: retrieve_foreign_key(curr, "team_id", "teams", "team_name", team))
#     if "Player" in df:
#         df["Player"] = df["Player"].apply(lambda col: check_na(col["Player"], "string"), axis = 1)
#         #use retreive foreign key
#     if "Enemy" in df:
#         df["Enemy"] = df["Enemy"].apply(lambda col: check_na(col["Enemy"], "string"), axis = 1)
#         #use retrieve foreign key
#     if "Enemy Team" in df:
#         df["Enemy Team"] = df["Enemy Team"].apply(lambda col: check_na(col["Enemy Team"], "string"), axis = 1)
#     if "Eliminator Team" in df:
#         df["Eliminator Team"] = df["Eliminator Team"].apply(lambda col: check_na(col["Eliminator Team"], "string"), axis = 1)
#     if "Eliminator" in df:
#         df["Eliminator"] = df["Eliminator"].apply(lambda col: check_na(col["Eliminator"], "string"), axis = 1)
#     if "Eliminated Team" in df:
#         df["Eliminated Team"] = df["Eliminated Team"].apply(lambda col: check_na(col["Eliminated Team"], "string"), axis = 1)
#     if "Eliminated" in df:
#         df["Eliminated"] = df["Eliminated"].apply(lambda col: check_na(col["Eliminated"], "string"), axis = 1)
#     if "Agents" in df:
#         df["Agents"].apply(lambda agents:)
#     if "Agent" in df:
#         df["Agent"].apply(lambda agents:)


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
