import pandas as pd
from checking.check_values import check_na
from retrieve.retrieve import retrieve_primary_key
from Connect.connect import create_pool, create_db_url
import asyncpg
import asyncio
import numpy as np
import sys

# na_values = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND',
#             '-1.#QNAN', '-NaN', '-nan', '1.#IND',
#             '1.#QNAN', 'N/A', 'NULL', 'NaN',
#             'n/a', 'null']

def strip_white_space(df, column_name):
    df.loc[:, column_name] = df[column_name].str.strip()

def create_ids(df):
    df.reset_index(inplace=True)
    return df

def standardized_duration(df):
    mask = df["Duration"].str.count(":") == 2
    df.loc[mask, "Duration"] = "0" + df.loc[mask, "Duration"]

    mask = df["Duration"].str.count(":") == 1
    df.loc[mask, "Duration"] = "00:" + df.loc[mask, "Duration"]

    df["Duration"].fillna("00:00:00", inplace=True)

    hours = df['Duration'].str.split(':').str[0].astype("int32")
    minutes = df['Duration'].str.split(':').str[1].astype("int32")
    seconds = df['Duration'].str.split(':').str[2].astype("int32")

    df['Duration'] = hours * 3600 + minutes * 60 + seconds

    return df

def convert_clutches(df):
    df["Clutches (won/played)"] = df["Clutches (won/played)"].fillna("0/0")
    clutches_split = df['Clutches (won/played)'].str.split('/', expand=True)
    df["Clutches Won"] = clutches_split[0]
    df["Clutches Played"] = clutches_split[1]
    mask = (df["Clutches (won/played)"] == '0/0')
    df.loc[mask, ["Clutches Won", "Clutches Played"]] = pd.NA
    return df

def convert_percentages(df):
    columns = [ "Kill, Assist, Trade, Survive %", "Headshot %", "Pick Rate", "Attacker Side Win Percentage", "Defender Side Win Percentage",
               "Clutch Success %"]
    for column in columns:
        if column in df:
            mask = df[column].str.contains("%", na=False)
            df.loc[mask, column] = df.loc[mask, column].str.rstrip("%").astype("float32") / 100
    return df


def convert_missing_numbers(df):
    for column in ["Rating", "Average Combat Score", "Kills", "Deaths", "Assists", "Kills - Deaths (KD)", "Average Damage per Round",
                   "First Kills", "First Deaths", "Kills - Deaths (FKD)", "Team A Overtime Score",
                   "Team B Overtime Score", "2k", "3k", "4k", "5k", "1v1", "1v2", "1v3", "1v4", "1v5", "Player Kills", "Enemy Kills", "Difference",
                   "Initiated"]:
        if column in df:
            df[column] = pd.to_numeric(df[column], errors="coerce", downcast="float")
    return df

def add_missing_ids(df, column, missing_numbers, null_count):
    df.loc[df[column].isnull(), column] = missing_numbers[:null_count]

def get_missing_numbers(df, column):
   min_id = int(df[column].min())
   max_id = int(df[column].max())
   all_numbers = set(range(min_id, max_id + 1))
   null_count = df[column].isnull().sum()
   missing_numbers = sorted(all_numbers - set(df[column]))
   np.random.shuffle(missing_numbers)
   return null_count, missing_numbers

def k_to_numeric(df, column):
    df[column] = df[column].str.replace("k", "")
    df[column] = df[column].astype("float32")
    df[column] *= 1000
    df[column] = df[column].astype("float32")
    return df

def get_eco_type(df):
    df["Type"] = df["Type"].str.split(":").str[0]
    return df

def convert_to_category(df):
    columns = ["Tournament", "Stage", "Match Type", "Match Name", "Agents", "Eliminator", "Eliminated", "Eliminator Team", "Eliminated Team",
               "Eliminator Agent", "Eliminated Agent", "Team A", "Team B", "Player Team", "Enemy Team", "Team", "Player", "Player Team",
               "Map", "Agent"]
    for column in columns:
        if column in df:
            df[column] = df[column].astype("category")
    return df

def drop_columns(df):
    columns = ["Tournament", "Stage", "Match Type", "Match Name", "Team", "Map", "Player", "Player Team", "Enemy Team", "Enemy",
               "Team A", "Team B", "Eliminator", "Eliminator Team", "Eliminated", "Eliminated Team", "Agent", "Clutches (won/played)"]
    for column in columns:
        if column in df and "Time Expiry (Failed to Plant)" not in df:
            df.drop(columns=column, inplace=True)
    return df

def reorder_columns(df, column_names):
    return df.reindex(columns=column_names)

def rename_columns(df):
    stats_columns = {"Average Combat Score": "acs", "Kills - Deaths (KD)": "kd", "Kill, Assist, Trade, Survive %": "kast",
                     "Average Damage Per Round": "adpr", "Headshot %": "headshot", "First Kills": "fk", "First Deaths": "fd",
                     "Kills - Deaths (FKD)": "fkd", "Kills:Deaths": "kd", "Kills Per Round": "kpr", "Assists Per Round": "apr",
                     "First Kills Per Round": "fkpr", "First Deaths Per Round": "fdpr", "Clutch Success %": "clutch_success",
                    "Maximum Kills in a Single Map": "mksp"}
    for column in df.columns:
        if column in stats_columns:
            new_column_name = stats_columns[column]
        else:    
            new_column_name = column.lower().replace(" ", "_").replace("(", "").replace(")", "")
        df.rename(columns={column: new_column_name}, inplace=True)
    return df

def csv_to_df(file):
    return pd.read_csv(file)

def create_tuples(df):
    tuples = [tuple(x) for x in df.values]
    return tuples

def splitting_teams(df):
    df.loc[:, "teams"] = df["teams"].str.split(", ")
    df = df.explode("teams")
    return df

def splitting_agents(df):
    df.loc[:, "agents"] = df["agents"].str.split(", ")
    df = df.explode("agents")
    return df

def add_missing_player(df, year):
    if "Player" in df and "Player ID" in df:
        if year == 2021:
            nan_player = df[df["Player ID"] == 10207].index
            df.loc[nan_player, "Player"] = "nan"
            df.loc[len(df.index)] = ["pATE", 9505]
            df.loc[len(df.index)] = ["Wendigo", 26880]
        elif year == 2022:
            df.loc[len(df.index)] = ["Wendigo", 26880]
        df.drop_duplicates(inplace=True, subset=["Player", "Player ID"])
        df.reset_index(drop=True, inplace=True)
    return df

def remove_leading_zeroes_from_players(df):
    columns = ["Player", "Eliminated", "Eliminator", "Enemy"]
    for column in columns:
        if column in df and "Time Expiry (Failed to Plant)" not in df:
            mask = df[df[column] == "002"].index
            df.loc[mask, column] = "2"
            mask = df[df[column] == "01000010"].index
            df.loc[mask, column] = "1000010"
    return df

def flatten_list_of_dicts(list_of_dicts):
    conditional_mapping = {}
    for dictionary in list_of_dicts:
        conditional_mapping.update(dictionary)
    return conditional_mapping


def create_conditions_values_1d(df, ids, column):
    conditions, values = [], []
    for key, value in ids.items():
        conditions.append(df[column] == key)
        values.append(value)
    return conditions, values


def create_stages_conditions_values(df, ids):
    conditions = []
    values = []
    for key, value in ids.items():
        conditions.append((df["Tournament ID"] == key[0]) & (df["Stage"] == key[1]))
        values.append(value)
    return conditions, values

def create_match_types_conditions_values(df, ids):
    conditions = []
    values = []
    for key, value in ids.items():
        conditions.append((df["Tournament ID"] == key[0]) & (df["Stage ID"] == key[1]) & (df["Match Type"] == key[2]))
        values.append(value)
    return conditions, values

def create_matches_conditions_values(df, ids):
    conditions = []
    values = []
    for key, value in ids.items():
        conditions.append((df["Tournament ID"] == key[0]) & (df["Stage ID"] == key[1]) & (df["Match Type ID"] == key[2]) & (df["Match Name"] == key[3]))
        values.append(value)
    return conditions, values

async def process_column(conn, df, column, id_name, table_name, value_name):
    values = df[column].unique().tolist()
    ids = [await retrieve_primary_key(conn, id_name, table_name, value_name, value) for value in values if pd.notna(value)]
    ids = flatten_list_of_dicts(ids)
    conditions, result_values = create_conditions_values_1d(df, ids, column)
    df[f"{column} ID"] = np.select(conditions, result_values)
    if table_name == "players":
        df[f"{column} ID"] = df[f"{column} ID"].astype("UInt32")
    else:
        df[f"{column} ID"] = df[f"{column} ID"].astype("UInt16")

async def process_tournaments_stages_match_types_matches(pool, df, year):
    async with pool.acquire() as conn:
        if "Tournament" in df:
            tournaments = df["Tournament"].unique().tolist()
            tournament_ids = [await retrieve_primary_key(pool, "tournament_id", "tournaments", "tournament", tournament, year) for tournament in tournaments]
            tournament_ids = flatten_list_of_dicts(tournament_ids)
            conditions, values = create_conditions_values_1d(df, tournament_ids, "Tournament")
            df["Tournament ID"] = np.select(conditions, values)
            if "Stage" in df:
                stages = df[["Tournament ID", "Stage"]].drop_duplicates()
                tuples = create_tuples(stages)
                stage_ids = [await retrieve_primary_key(conn, "stage_id", "stages", "stage", (tournament_id, stage), year) 
                            for tournament_id, stage in tuples]
                stage_ids = flatten_list_of_dicts(stage_ids)
                conditions, values = create_stages_conditions_values(df, stage_ids)
                df["Stage ID"] = np.select(conditions, values)
                if "Match Type" in df:
                    match_types = df[["Tournament ID", "Stage ID", "Match Type"]].drop_duplicates()
                    tuples = create_tuples(match_types)
                    match_types_ids = [await retrieve_primary_key(conn, "match_type_id", "match_types", "match_type", (tournament_id, stage_id, match_type), year) 
                                        for tournament_id, stage_id, match_type in tuples]
                    match_types_ids = flatten_list_of_dicts(match_types_ids)
                    conditions, values = create_match_types_conditions_values(df, match_types_ids)
                    df["Match Type ID"] = np.select(conditions, values)
                if "Match Name" in df:
                    matches = df[["Tournament ID", "Stage ID", "Match Type ID", "Match Name"]].drop_duplicates()
                    tuples = create_tuples(matches)
                    matches_id = [await retrieve_primary_key(conn, "match_id", "matches", "match", (tournament_id, stage_id, match_type_id, match_name), year)
                                    for tournament_id, stage_id, match_type_id, match_name in tuples]
                    matches_id = flatten_list_of_dicts(matches_id)
                    conditions, values = create_matches_conditions_values(df, matches_id)
                    df["Match ID"] = np.select(conditions, values)


async def process_teams(pool, df):
    async with pool.acquire() as conn:
        team_columns = ["Team", "Player Team", "Enemy Team", "Eliminator Team", "Eliminated Team", "Team A", "Team B"]
        tasks = [await process_column(conn, df, column, "team_id", "teams", "team") for column in team_columns if column in df]
        # await asyncio.gather(*tasks)

async def process_players(pool, df):
    async with pool.acquire() as conn:
        player_columns = ["Player", "Enemy", "Eliminator", "Eliminated"]
        tasks = [await process_column(conn, df, column, "player_id", "players", "player") for column in player_columns if column in df and "Time Expiry (Failed to Plant)" not in df]
        # await asyncio.gather(*tasks)

async def process_agents(pool, df):
    async with pool.acquire() as conn:
        agent_columns = ["agent", "Eliminator Agent", "Eliminated Agent", "Agent"]
        tasks = [await process_column(conn, df, column, "agent_id", "agents", "agent") for column in agent_columns if column in df]

async def process_maps(pool, df):
    async with pool.acquire() as conn:
        map_columns = ["Map"]
        tasks = [await process_column(conn, df, column, "map_id", "maps", "map") for column in map_columns if column in df]

async def change_reference_name_to_id(df, year):
    db_url = create_db_url()
    async with asyncpg.create_pool(db_url) as pool:
        await asyncio.gather(
            process_tournaments_stages_match_types_matches(pool, df, year),
            process_players(pool, df),
            process_teams(pool, df),
            process_maps(pool, df),
            process_agents(pool, df)
        )

    return df


async def combine_drafts(file, file_name, year, dfs):
   drafts_df = csv_to_df(file)
   drafts_df = convert_to_category(drafts_df)
   drafts_df = await change_reference_name_to_id(drafts_df, year)
   drafts_df["year"] = year
   drafts_df = create_ids(drafts_df)
   drafts_df = drop_columns(drafts_df)
   drafts_df = rename_columns(drafts_df)
   drafts_df = reorder_columns(drafts_df, ["index", "tournament_id", "stage_id", "match_type_id", "match_id", "team_id", "map_id", "action", "year"])
   dfs[file_name] = pd.concat([drafts_df, dfs[file_name]], ignore_index=True)
   # print(drafts_df.sample(n=20))
   # drafts_df.to_sql("drafts", dfs, index=False, if_exists="append")

async def combine_eco_rounds(file, file_name, year, dfs):
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
   dfs[file_name] = pd.concat([eco_rounds_df, dfs[file_name]], ignore_index=True)
#    print(eco_rounds_df.sample(n=20))
   # eco_rounds_df.to_sql("eco_rounds", dfs, index=False, if_exists="append", chunksize = 10000)
      
async def combine_eco_stats(file, file_name, year, dfs):
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
   dfs[file_name] = pd.concat([eco_stats_df, dfs[file_name]], ignore_index=True)
#    print(eco_stats_df.sample(n=20))
   # eco_stats_df.to_sql("eco_stats", dfs, index=False, if_exists="append", chunksize = 10000)
   
      

async def combine_kills(file, file_name, year, dfs):
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
   dfs[file_name] = pd.concat([kills_df, dfs[file_name]], ignore_index=True)
#    print(kills_df.sample(n=20))
   
   # print(f"combineing kills")

async def combine_kills_stats(file, file_name, year, dfs):
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
#    agents_df = kills_stats_df[["index", "agents"]]
#    agents_df = splitting_agents(agents_df)
#    agents_df.rename(columns={"agents": "agent"}, inplace=True)
#    kills_stats_df.drop(columns="agents", inplace=True)
#    agents_df = await change_reference_name_to_id(agents_df, year)
#    agents_df["year"] = year
   dfs[file_name] = pd.concat([kills_stats_df, dfs[file_name]], ignore_index=True)
#    print(kills_stats_df.sample(n=20))
#    print(agents_df.sample(n=20))


async def combine_maps_played(file, file_name, year, dfs):
   maps_played_df = csv_to_df(file)
   maps_played_df = convert_to_category(maps_played_df)
   maps_played_df = await change_reference_name_to_id(maps_played_df, year)
   maps_played_df = create_ids(maps_played_df)
   maps_played_df = drop_columns(maps_played_df)
   maps_played_df = rename_columns(maps_played_df)
   maps_played_df = reorder_columns(maps_played_df, ["index", "tournament_id", "stage_id", "match_type_id", "match_id", "map_id"])
   dfs[file_name] = pd.concat([maps_played_df, dfs[file_name]], ignore_index=True)
#    print(maps_played_df.sample(n=20))

async def combine_maps_scores(file, file_name, year, dfs):
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
   dfs[file_name] = pd.concat([maps_scores_df, dfs[file_name]], ignore_index=True)
#    print(maps_scores_df.sample(n=20))


async def combine_overview(file, file_name, year, dfs):
   overview_df = csv_to_df(file)
   overview_df = remove_leading_zeroes_from_players(overview_df)
   overview_df = convert_to_category(overview_df)
   overview_df = await change_reference_name_to_id(overview_df, year)
   overview_df = drop_columns(overview_df)
   overview_df = convert_percentages(overview_df)
   overview_df = convert_missing_numbers(overview_df)
   overview_df = create_ids(overview_df)
   overview_df = rename_columns(overview_df)
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
   dfs[file_name] = pd.concat([overview_df, dfs[file_name]], ignore_index=True)
#    print(overview_df.sample(n=20))
#    print(agents_df.sample(n=20))



async def combine_rounds_kills(file, file_name, year, dfs):
   rounds_kills_df = csv_to_df(file)
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
   dfs[file_name] = pd.concat([rounds_kills_df, dfs[file_name]], ignore_index=True)
#    print(rounds_kills_df.sample(n=20))


async def combine_scores(file, file_name, year, dfs):
   scores_df = csv_to_df(file)
   scores_df = convert_to_category(scores_df)
   scores_df = await change_reference_name_to_id(scores_df, year)
   scores_df = create_ids(scores_df)
   scores_df = drop_columns(scores_df)
   scores_df = rename_columns(scores_df)
   scores_df["year"] = year
   scores_df = reorder_columns(scores_df, ["index", "tournament_id", "stage_id", "match_type_id", "match_id", "team_a_id", "team_b_id",
                                            "team_a_score", "team_b_score", "match_result", "year"])
   dfs[file_name] = pd.concat([scores_df, dfs[file_name]], ignore_index=True)
#    print(scores_df.sample(n=20))


async def combine_win_loss_methods_count(file, file_name, year, dfs):
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
   dfs[file_name] = pd.concat([win_loss_methods_count_df, dfs[file_name]], ignore_index=True)
#    print(win_loss_methods_count_df.sample(n=20))
   

async def combine_win_loss_methods_round_number(file, file_name, year, dfs):
   win_loss_methods_round_number_df = csv_to_df(file)
   win_loss_methods_round_number_df = convert_to_category(win_loss_methods_round_number_df)
   win_loss_methods_round_number_df = await change_reference_name_to_id(win_loss_methods_round_number_df, year)
   win_loss_methods_round_number_df = create_ids(win_loss_methods_round_number_df)
   win_loss_methods_round_number_df = drop_columns(win_loss_methods_round_number_df)
   win_loss_methods_round_number_df = rename_columns(win_loss_methods_round_number_df)
   win_loss_methods_round_number_df["year"] = year
   win_loss_methods_round_number_df = reorder_columns(win_loss_methods_round_number_df, ["index", "tournament_id", "stage_id", "match_type_id", "match_id", "team_id",
                                                                                         "map_id", "round_number", "method", "outcome", "year"])
   dfs[file_name] = pd.concat([win_loss_methods_round_number_df, dfs[file_name]], ignore_index=True)
#    print(win_loss_methods_round_number_df.sample(n=20))

async def combine_agents_pick_rates(file, file_name, year, dfs):
   agents_pick_rates_df = csv_to_df(file)
   agents_pick_rates_df = convert_to_category(agents_pick_rates_df)
   agents_pick_rates_df = await change_reference_name_to_id(agents_pick_rates_df, year)
   agents_pick_rates_df = create_ids(agents_pick_rates_df)
   agents_pick_rates_df = drop_columns(agents_pick_rates_df)
   agents_pick_rates_df = convert_percentages(agents_pick_rates_df)
   agents_pick_rates_df = rename_columns(agents_pick_rates_df)
   agents_pick_rates_df["year"] = year
   agents_pick_rates_df = reorder_columns(agents_pick_rates_df, ["index", "tournament_id", "stage_id", "match_type_id", "map_id", "agent_id",
                                                                 "pick_rate", "year"])
   dfs[file_name] = pd.concat([agents_pick_rates_df, dfs[file_name]], ignore_index=True)
#    print(agents_pick_rates_df.sample(n=20))


async def combine_maps_stats(file, file_name, year, dfs):
   maps_stats_df = csv_to_df(file)
   maps_stats_df = convert_to_category(maps_stats_df)
   maps_stats_df = await change_reference_name_to_id(maps_stats_df, year)
   maps_stats_df = create_ids(maps_stats_df)
   maps_stats_df = drop_columns(maps_stats_df)
   maps_stats_df = convert_percentages(maps_stats_df)
   maps_stats_df = rename_columns(maps_stats_df)
   maps_stats_df['year'] = year
   maps_stats_df = reorder_columns(maps_stats_df, ["index", "tournament_id", "stage_id", "match_type_id", "map_id", "total_maps_played",
                                                   "attacker_side_win_percentage", "defender_side_win_percentage", "year"])
   dfs[file_name] = pd.concat([maps_stats_df, dfs[file_name]], ignore_index=True)
#    print(maps_stats_df.sample(n=20))

async def combine_teams_picked_agents(file, file_name, year, dfs):
   teams_picked_agents_df = csv_to_df(file)
   teams_picked_agents_df = convert_to_category(teams_picked_agents_df)
   teams_picked_agents_df = await change_reference_name_to_id(teams_picked_agents_df, year)
   teams_picked_agents_df = create_ids(teams_picked_agents_df)
   teams_picked_agents_df = drop_columns(teams_picked_agents_df)
   teams_picked_agents_df = rename_columns(teams_picked_agents_df)
   teams_picked_agents_df["year"] = year
   teams_picked_agents_df = reorder_columns(teams_picked_agents_df, ["index", "tournament_id", "stage_id", "match_type_id", "map_id",
                                                                     "agent_id", "total_wins_by_map", "total_loss_by_map", "total_maps_played",
                                                                     "year"])
   dfs[file_name] = pd.concat([teams_picked_agents_df, dfs[file_name]], ignore_index=True)
#    print(teams_picked_agents_df.sample(n=20))



async def combine_players_stats(file, file_name, year, dfs):
   players_stats_df = csv_to_df(file)
   players_stats_df = remove_leading_zeroes_from_players(players_stats_df)
   players_stats_df = convert_to_category(players_stats_df)
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
#    agents_df = players_stats_df[["index", "agents"]]
#    agents_df = splitting_agents(agents_df)
#    agents_df.rename(columns={"agents": "agent"}, inplace=True)
#    players_stats_df.drop(columns="agents", inplace=True)
#    teams_df = players_stats_df[["index", "teams"]]
#    teams_df = splitting_teams(teams_df)
#    teams_df.rename(columns={"teams": "team"}, inplace=True)
#    players_stats_df.drop(columns="teams", inplace=True)
#    teams_df = await change_reference_name_to_id(teams_df, year)
#    teams_df["year"] = year
   dfs[file_name] = pd.concat([players_stats_df, dfs[file_name]], ignore_index=True)
#    print(players_stats_df.sample(n=20))
#    print(agents_df.sample(n=20))
#    print(teams_df.sample(n=20))


async def process_csv_files(csv_files, year, dfs):
    for csv_file in csv_files:
        file_name = csv_file.split("/")[2]
        print(file_name, year)
        if year == 0:
            print("Yes")
        elif file_name == "draft_phase.csv":
            await combine_drafts(csv_file, file_name, year, dfs)
        # elif file_name == "eco_rounds.csv":
        #    await combine_eco_rounds(csv_file, file_name, year, dfs)
        # elif file_name == "eco_stats.csv": 
        #    await combine_eco_stats(csv_file, file_name, year, dfs)
        # elif file_name == "kills.csv":
        #    await combine_kills(csv_file, file_name, year, dfs)
        # elif file_name == "kills_stats.csv":
        #    await combine_kills_stats(csv_file, file_name, year, dfs)
        # elif file_name == "maps_played.csv":
        #    await add_maps_played(csv_file, file_name, year, dfs)
        # elif file_name == "maps_scores.csv":
        #    await add_maps_scores(csv_file, file_name, year, dfs)
        # elif file_name == "overview.csv":
        #    await add_overview(csv_file, file_name, year, dfs)
        # elif file_name == "rounds_kills.csv":
        #    await add_rounds_kills(csv_file, file_name, year, dfs)
        # elif file_name == "scores.csv":
        #    await add_scores(csv_file, file_name, year, dfs)
        # elif file_name == "win_loss_methods_count.csv":
        #    await add_win_loss_methods_count(csv_file, file_name, year, dfs)
        # elif file_name == "win_loss_methods_round_number.csv":
        #    await add_win_loss_methods_round_number(csv_file, file_name, year, dfs)
        # elif file_name == "agents_pick_rates.csv":
        #    await add_agents_pick_rates(csv_file, file_name, year, dfs)
        # elif file_name == "maps_stats.csv":
        #    await add_maps_stats(csv_file, file_name, year, dfs)
        # elif file_name == "teams_picked_agents.csv":
        #    await add_teams_picked_agents(csv_file, file_name, year, dfs)
        # elif file_name == "players_stats.csv":
        #    await add_players_stats(csv_file, file_name, year, dfs)

async def process_year(year, csv_files, dfs):
   await process_csv_files(csv_files, year, dfs)

   
