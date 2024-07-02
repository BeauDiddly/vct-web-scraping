from retrieve.retrieve import retrieve_primary_key
from Connect.connect import create_db_url
import numpy as np
import pandas as pd
import asyncpg
import asyncio


# na_values = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND',
#             '-1.#QNAN', '-NaN', '-nan', '1.#IND',
#             '1.#QNAN', 'N/A', 'NULL', 'NaN',
#             'n/a', 'null']

def combine_dfs(combined_dfs, dfs):
    for file_name, dfs_dict in dfs.items():
        for category, df_list in dfs_dict.items():
            for df in df_list:
                combined_dfs[file_name][category] = pd.concat([combined_dfs[file_name][category], df], ignore_index=True)
            combined_dfs[file_name][category] = create_index_column(combined_dfs[file_name][category])



def strip_white_space(df, column_name):
    df.loc[:, column_name] = df[column_name].str.strip()

def create_index_column(df):
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

def convert_to_ints(df):
    int_with_na_columns = ["initiated", "player_kills", "enemy_kills", "difference", "two_kills", "three_kills", "four_kills", "five_kills", "one_vs_one", "one_vs_two", "one_vs_three",
               "one_vs_four", "one_vs_five", "econ", "spike_plants", "spike_defuses", "year", "team_a_defender_score", "team_b_defender_score", "team_a_overtime_score",
                "team_b_overtime_score", "duration", "acs", "kd", "fk", "fd", "fkd"]
    int_no_decimal_columns = ["deaths"]
    for column in int_with_na_columns:
        if column in df:
            df[column] = df[column].astype("Int64")
    # for column in int_no_decimal_columns:
    #     if column in df:
    #         print("hi")
    #         df[column] = df[column].astype(int)
    return df

def convert_clutches(df):
    df["Clutches (won/played)"] = df["Clutches (won/played)"].fillna("0/0")
    clutches_split = df['Clutches (won/played)'].str.split('/', expand=True)
    df["Clutches Won"] = clutches_split[0]
    df["Clutches Played"] = clutches_split[1]
    mask = (df["Clutches (won/played)"] == '0/0')
    df.loc[mask, ["Clutches Won", "Clutches Played"]] = pd.NA
    return df

def convert_percentages_to_decimal(df):
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
    if column == "Player ID":
        df.loc[len(df.index)] = [pd.NA, 0]


def add_player_nan(df):
    condition = (
        (df['Tournament'] == "Champions Tour Philippines Stage 1: Challengers 2") &
        (df['Stage'].isin(["Qualifier 1", "All Stages"])) &
        (df['Match Type'].isin(["Round of 16", "All Match Types"])) &
        (df['Player'].isnull()) &
        (df['Agents'] == "reyna")
    )
    if "Match Name" in df:
        player_nan_overview_condition = (df['Tournament'] == 'Champions Tour Philippines Stage 1: Challengers 2') & \
                                        (df['Stage'] == 'Qualifier 1') & \
                                        (df['Match Type'] == 'Round of 16') & \
                                        (df['Player'].isnull()) & \
                                        (df["Match Name"] == "KADILIMAN vs MGS Spades")
        filtered_indices = df.index[player_nan_overview_condition]
        df.loc[filtered_indices, "Player"] = "nan"
    filtered_indices = df.index[condition]
    df.loc[filtered_indices, "Player"] = "nan"
    return df

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

def get_upper_round_id(df):
    upper_round_df = df[(df["Tournament ID"] == 560) &
                       (df["Stage ID"] == 1096) &
                       (df["Match Type"] == "Upper Round 1")]
    upper_round_id = upper_round_df["Match Type ID"].values[0]
    return upper_round_id

def convert_reference_columns_to_category(df):
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
                     "Maximum Kills in a Single Map": "mksp", "2k": "two_kills", "3k": "three_kills", "4k": "four_kills", "5k": "five_kills",
                     "1v1": "one_vs_one", "1v2": "one_vs_two", "1v3": "one_vs_three", "1v4": "one_vs_four", "1v5": "one_vs_five"}
    for column in df.columns:
        if column in stats_columns:
            new_column_name = stats_columns[column]
        else:    
            new_column_name = column.lower().replace(" ", "_").replace("(", "").replace(")", "")
        df.rename(columns={column: new_column_name}, inplace=True)
    return df

def remove_nan_rows(df, cols):
    df = df.dropna(subset=cols, how='all')
    return df

def csv_to_df(file):
    return pd.read_csv(file)

def create_tuples(df):
    tuples = [tuple(x) for x in df.values]
    return tuples

def splitting_teams(df):
    df['teams'] = df['teams'].replace('Stay Small, Stay Second', 'Stay Small; Stay Second', regex=True)
    df.loc[:, "teams"] = df["teams"].str.split(", ")
    df = df.explode("teams")
    df['teams'] = df['teams'].replace('Stay Small; Stay Second', 'Stay Small, Stay Second', regex=True)

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

def create_boolean_indexing(df, ids, columns):
    conditions = []
    values = []
    for key, value in ids.items():
        if len(columns) > 1:
            compound_condition = [df[columns[i]] == key[i] for i in range(len(key))]
            conditions.append(np.logical_and.reduce(compound_condition))
        else:
            conditions.append(df[columns[0]] == key)
        values.append(value)
    return conditions, values


async def process_column(pool, df, df_column, id_name, table_name, table_column_name):
    values = df[df_column].unique().tolist()
    ids = await asyncio.gather(
        *(retrieve_primary_key(pool, id_name, table_name, table_column_name, [value]) for value in values if pd.notna(value))
    )
    ids = flatten_list_of_dicts(ids)
    conditions, result_values = create_boolean_indexing(df, ids, [df_column])
    df[f"{df_column} ID"] = np.select(conditions, result_values)
    if table_name == "players":
        df[f"{df_column} ID"] = df[f"{df_column} ID"].astype("UInt32")
    else:
        df[f"{df_column} ID"] = df[f"{df_column} ID"].astype("UInt16")

async def process_tournaments_stages_match_types_matches(pool, df, year):
    if "Tournament" in df:
        tournaments = df["Tournament"].unique().tolist()
        tournament_ids = await asyncio.gather(
            *(retrieve_primary_key(pool, "tournament_id", "tournaments", "tournament", [tournament], year)
                for tournament in tournaments)
            )
        tournament_ids = flatten_list_of_dicts(tournament_ids)
        columns = ["Tournament"]
        conditions, values = create_boolean_indexing(df, tournament_ids, columns)
        df["Tournament ID"] = np.select(conditions, values)
        if "Stage" in df:
            stages = df[["Tournament ID", "Stage"]].drop_duplicates()
            tuples = create_tuples(stages)
            stage_ids = await asyncio.gather(
                *(retrieve_primary_key(pool, "stage_id", "stages", "stage", [stage, tournament_id], year) 
                    for tournament_id, stage in tuples)
                    )
            stage_ids = flatten_list_of_dicts(stage_ids)
            columns = ["Stage", "Tournament ID"]
            conditions, values = create_boolean_indexing(df, stage_ids, columns)
            df["Stage ID"] = np.select(conditions, values)
            if "Match Type" in df:
                match_types = df[["Tournament ID", "Stage ID", "Match Type"]].drop_duplicates()
                tuples = create_tuples(match_types)
                match_types_ids = await asyncio.gather(
                    *(retrieve_primary_key(pool, "match_type_id", "match_types", "match_type", [match_type, tournament_id, stage_id], year) 
                        for tournament_id, stage_id, match_type in tuples)
                        )
                columns = ["Match Type", "Tournament ID", "Stage ID"]
                match_types_ids = flatten_list_of_dicts(match_types_ids)
                conditions, values = create_boolean_indexing(df, match_types_ids, columns)
                df["Match Type ID"] = np.select(conditions, values)
            if "Match Name" in df:
                matches = df[["Tournament ID", "Stage ID", "Match Type ID", "Match Name"]].drop_duplicates()
                tuples = create_tuples(matches)
                matches_id = await asyncio.gather(
                    *(retrieve_primary_key(pool, "match_id", "matches", "match", [match_name, tournament_id, stage_id, match_type_id], year)
                        for tournament_id, stage_id, match_type_id, match_name in tuples)
                        )
                columns = ["Match Name", "Tournament ID", "Stage ID", "Match Type ID"]
                matches_ids = flatten_list_of_dicts(matches_id)
                conditions, values = create_boolean_indexing(df, matches_ids, columns)
                df["Match ID"] = np.select(conditions, values)

async def process_teams(pool, df):
    team_columns = ["Team", "Player Team", "Enemy Team", "Eliminator Team", "Eliminated Team", "Team A", "Team B"]
    await asyncio.gather(
        *(process_column(pool, df, column, "team_id", "teams", "team") for column in team_columns if column in df)
    )

async def process_players(pool, df):
    player_columns = ["Player", "Enemy", "Eliminator", "Eliminated"]
    await asyncio.gather(
        *(process_column(pool, df, column, "player_id", "players", "player") for column in player_columns if column in df and "Time Expiry (Failed to Plant)" not in df)
        )

async def process_agents(pool, df):
    agent_columns = ["Eliminator Agent", "Eliminated Agent", "Agent"]
    await asyncio.gather(
        *(process_column(pool, df, column, "agent_id", "agents", "agent") for column in agent_columns if column in df)
    )

async def process_maps(pool, df):
    if "Map" in df:
        await process_column(pool, df, "Map", "map_id", "maps", "map")

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

def process_tournaments_stages_match_types(df):
    df = df[["Tournament", "Tournament ID", "Stage", "Stage ID", "Match Type", "Match Type ID", "Year"]]
    df = df.drop_duplicates()
    null_stage_count, missing_stage_ids = get_missing_numbers(df, "Stage ID")
    null_match_type_count, missing_match_type_ids = get_missing_numbers(df, "Match Type ID")
    add_missing_ids(df, "Stage ID", missing_stage_ids, null_stage_count)
    add_missing_ids(df, "Match Type ID", missing_match_type_ids, null_match_type_count)
    return df

def process_tournaments(df):
   df = df[["Tournament", "Tournament ID", "Year"]]
   df = df.drop_duplicates()
   df = rename_columns(df)
   df = reorder_columns(df, ["tournament_id", "tournament", "year"])
   return df

def process_stages(df):
   df = df[["Tournament ID", "Stage", "Stage ID", "Year"]]
   df = df.drop_duplicates()
   df = rename_columns(df) 
   df = reorder_columns(df, ["stage_id", "tournament_id", "stage", "year"])
   return df

def process_match_types(df):
   df = df[["Tournament ID", "Stage ID", "Match Type", "Match Type ID", "Year"]]
   df = df.drop_duplicates()
   df = rename_columns(df)
   df = reorder_columns(df, ["match_type_id", "tournament_id", "stage_id", "match_type", "year"])
   return df

def process_matches(df, upper_round_id):
    filtered = df[(df["Tournament ID"] == 560) &
                  (df["Stage ID"] == 1096) &
                  (df["Match Type"] == "Upper Round 1")]
    df = df[["Tournament ID", "Stage ID", "Match Type ID", "Match Name", "Match ID", "Year"]]
    df.loc[filtered.index, "Match Type ID"] = upper_round_id
    df = df.drop_duplicates()
    df.rename(columns={"Match Name": "Match"}, inplace=True)
    df = rename_columns(df)
    df = reorder_columns(df, ["match_id", "tournament_id", "stage_id", "match_type_id", "match", "year"])
    return df

def process_teams_ids(df):
   df = df[["Team", "Team ID"]]
   df = df.drop_duplicates()
   null_team_count, missing_team_id = get_missing_numbers(df, "Team ID")
   add_missing_ids(df, "Team ID", missing_team_id, null_team_count)
   df = rename_columns(df)
   df = reorder_columns(df, {"team_id", "team"})
   return df

def process_players_ids(df):
   df = df[["Player", "Player ID"]]
   df = df.drop_duplicates()
   null_player_count, missing_player_id = get_missing_numbers(df, "Player ID")
   add_missing_ids(df, "Player ID", missing_player_id, null_player_count)
   df = add_missing_player(df, 2021)
   df = remove_leading_zeroes_from_players(df)
   df = rename_columns(df)
   df = reorder_columns(df, {"player_id", "player"})
   return df

async def process_drafts(file, file_name, year, dfs):
   drafts_df = csv_to_df(file)
   drafts_df = convert_reference_columns_to_category(drafts_df)
   drafts_df = await change_reference_name_to_id(drafts_df, year)
   drafts_df = drop_columns(drafts_df)
   drafts_df = rename_columns(drafts_df)
   drafts_df = reorder_columns(drafts_df, ["tournament_id", "stage_id", "match_type_id", "match_id", "team_id", "map_id", "action"])
   drafts_df["year"] = year
   dfs[file_name]["main"].append(drafts_df) 

async def process_eco_rounds(file, file_name, year, dfs):
   eco_rounds_df = csv_to_df(file)
   eco_rounds_df = convert_reference_columns_to_category(eco_rounds_df)
   eco_rounds_df = await change_reference_name_to_id(eco_rounds_df, year)
   eco_rounds_df = k_to_numeric(eco_rounds_df, "Loadout Value")
   eco_rounds_df = k_to_numeric(eco_rounds_df, "Remaining Credits")
   eco_rounds_df = get_eco_type(eco_rounds_df)
   eco_rounds_df = drop_columns(eco_rounds_df)
   eco_rounds_df = rename_columns(eco_rounds_df)
   eco_rounds_df = reorder_columns(eco_rounds_df, ["tournament_id", "stage_id", "match_type_id", "match_id", "team_id",
                                             "map_id", "round_number", "loadout_value", "remaining_credits", "type", "outcome"])
   eco_rounds_df["year"] = year
   dfs[file_name]["main"].append(eco_rounds_df)
      
async def process_eco_stats(file, file_name, year, dfs):
   eco_stats_df = csv_to_df(file)
   eco_stats_df = convert_reference_columns_to_category(eco_stats_df)
   eco_stats_df = await change_reference_name_to_id(eco_stats_df, year)
   eco_stats_df = convert_missing_numbers(eco_stats_df)
   eco_stats_df = drop_columns(eco_stats_df)
   eco_stats_df = rename_columns(eco_stats_df)
   eco_stats_df = reorder_columns(eco_stats_df, ["tournament_id", "stage_id", "match_type_id", "match_id", "team_id", "map_id", "type", "initiated", "won"])
   eco_stats_df = convert_to_ints(eco_stats_df)
   eco_stats_df["year"] = year
   dfs[file_name]["main"].append(eco_stats_df)
   
      

async def process_kills(file, file_name, year, dfs):
   kills_df = csv_to_df(file)
   kills_df = remove_nan_rows(kills_df, ['Player Kills', 'Enemy Kills', 'Difference'])
   kills_df = remove_leading_zeroes_from_players(kills_df)
   kills_df = convert_reference_columns_to_category(kills_df)
   kills_df = await change_reference_name_to_id(kills_df, year)
   kills_df = convert_missing_numbers(kills_df)
   kills_df = drop_columns(kills_df)
   kills_df = rename_columns(kills_df)
   kills_df = reorder_columns(kills_df, ["tournament_id", "stage_id", "match_type_id", "match_id", "player_team_id", "player_id", "enemy_team_id", "enemy_id",
                                         "map_id", "player_kills", "enemy_kills", "difference", "kill_type"])
   kills_df = convert_to_ints(kills_df)
   kills_df["year"] = year
   dfs[file_name]["main"].append(kills_df)

async def process_kills_stats(file, file_name, year, dfs):
   kills_stats_df = csv_to_df(file)
   kills_stats_df = remove_leading_zeroes_from_players(kills_stats_df)
   kills_stats_df = add_player_nan(kills_stats_df)
   kills_stats_df = convert_reference_columns_to_category(kills_stats_df)
   kills_stats_df = await change_reference_name_to_id(kills_stats_df, year)
   kills_stats_df = convert_missing_numbers(kills_stats_df)
   kills_stats_df = drop_columns(kills_stats_df)
   kills_stats_df = rename_columns(kills_stats_df)
   kills_stats_df = reorder_columns(kills_stats_df, ["tournament_id", "stage_id", "match_type_id", "match_id", "team_id", "player_id", "map_id", "agents",
                                                     "two_kills", "three_kills", "four_kills", "five_kills", "one_vs_one", "one_vs_two", "one_vs_three",
                                                     "one_vs_four", "one_vs_five", "econ", "spike_plants", "spike_defuses"])
   kills_stats_df = convert_to_ints(kills_stats_df)
   kills_stats_df["year"] = year
   dfs[file_name]["main"].append(kills_stats_df)

async def process_kills_stats_agents(combined_dfs, combined_df):
   agents_df = combined_df[["index", "agents", "year"]]
   agents_df = splitting_agents(agents_df)
   agents_df.rename(columns={"agents": "Agent"}, inplace=True)
   agents_df = await change_reference_name_to_id(agents_df, 0)
   combined_df.drop(columns="agents", inplace=True)
   agents_df.drop(columns="Agent", inplace=True)
   agents_df = convert_to_ints(agents_df)
   agents_df = rename_columns(agents_df)
   agents_df = reorder_columns(agents_df, ["index", "agent_id", "year"])
   combined_dfs["kills_stats.csv"]["agents"] = pd.concat([combined_dfs["kills_stats.csv"]["agents"], agents_df], ignore_index=True)


async def process_maps_played(file, file_name, year, dfs):
   maps_played_df = csv_to_df(file)
   maps_played_df = convert_reference_columns_to_category(maps_played_df)
   maps_played_df = await change_reference_name_to_id(maps_played_df, year)
   maps_played_df = drop_columns(maps_played_df)
   maps_played_df = rename_columns(maps_played_df)
   maps_played_df = reorder_columns(maps_played_df, ["tournament_id", "stage_id", "match_type_id", "match_id", "map_id"])
   maps_played_df["year"] = year
   dfs[file_name]["main"].append(maps_played_df)

async def process_maps_scores(file, file_name, year, dfs):
   maps_scores_df = csv_to_df(file)
   maps_scores_df = convert_reference_columns_to_category(maps_scores_df)
   maps_scores_df = await change_reference_name_to_id(maps_scores_df, year)
   maps_scores_df = drop_columns(maps_scores_df)
   maps_scores_df = standardized_duration(maps_scores_df)
   maps_scores_df = convert_missing_numbers(maps_scores_df)
   maps_scores_df = rename_columns(maps_scores_df)
   maps_scores_df = reorder_columns(maps_scores_df, ["tournament_id", "stage_id", "match_type_id", "match_id", "map_id", "team_a_id",
                                                     "team_b_id", "team_a_score", "team_a_attacker_score", "team_a_defender_score",
                                                     "team_a_overtime_score", "team_b_score", "team_b_attacker_score",
                                                     "team_b_defender_score", "team_b_overtime_score", "duration"])
   maps_scores_df = convert_to_ints(maps_scores_df)
   maps_scores_df["year"]= year
   dfs[file_name]["main"].append(maps_scores_df)


async def process_overview(file, file_name, year, dfs):
   overview_df = csv_to_df(file)
   overview_df = remove_nan_rows(overview_df, ["Rating", "Average Combat Score", "Kills", "Deaths", "Assists", "Kills - Deaths (KD)", "Kill, Assist, Trade, Survive %",
                                               	"Average Damage Per Round", "Headshot %", "First Kills"	, "First Deaths", "Kills - Deaths (FKD)"])
   overview_df = remove_leading_zeroes_from_players(overview_df)
   overview_df = add_player_nan(overview_df)
   overview_df = convert_reference_columns_to_category(overview_df)
   overview_df = await change_reference_name_to_id(overview_df, year)
   overview_df = drop_columns(overview_df)
   overview_df = convert_percentages_to_decimal(overview_df)
   overview_df = convert_missing_numbers(overview_df)
   overview_df = rename_columns(overview_df)
   overview_df = reorder_columns(overview_df, ["tournament_id", "stage_id", "match_type_id", "match_id", "map_id", "player_id",  "team_id",
                                               "agents", "rating", "acs", "kills", "deaths", "assists", "kd", "kast", "adpr", "headshot", "fk",
                                               "fd", "fkd", "side"])
   overview_df = convert_to_ints(overview_df)
   overview_df["year"] = year
   dfs[file_name]["main"].append(overview_df)

async def process_overview_agents(combined_dfs, combined_df):
   agents_df = combined_df[["index", "agents", "year"]]
   agents_df = splitting_agents(agents_df)
   agents_df.rename(columns={"agents": "Agent"}, inplace=True)
   combined_df.drop(columns="agents", inplace=True)
   agents_df = await change_reference_name_to_id(agents_df, 0)
   agents_df.drop(columns="Agent", inplace=True)
   agents_df = rename_columns(agents_df)
   agents_df = reorder_columns(agents_df, ["index", "agent_id", "year"])
   agents_df = convert_to_ints(agents_df)
   combined_dfs["overview.csv"]["agents"] = pd.concat([combined_dfs["overview.csv"]["agents"], agents_df], ignore_index=True)

async def process_rounds_kills(file, file_name, year, dfs):
   rounds_kills_df = csv_to_df(file)
   if year == 2021:
       rounds_kills_df[rounds_kills_df["Eliminator Team"].isna()].to_csv("test.csv")
   rounds_kills_df = remove_leading_zeroes_from_players(rounds_kills_df)
   rounds_kills_df = convert_reference_columns_to_category(rounds_kills_df)
   rounds_kills_df = await change_reference_name_to_id(rounds_kills_df, year)
   rounds_kills_df = drop_columns(rounds_kills_df)
   rounds_kills_df = rename_columns(rounds_kills_df)
   rounds_kills_df = reorder_columns(rounds_kills_df, ["tournament_id", "stage_id", "match_type_id", "match_id",
                                                       "map_id", "eliminator_team_id", "eliminated_team_id",
                                                       "eliminator_id", "eliminated_id", "eliminator_agent_id", "eliminated_agent_id",
                                                       "round_number", "kill_type"])
   rounds_kills_df["year"] = year
   dfs[file_name]["main"].append(rounds_kills_df)


async def process_scores(file, file_name, year, dfs):
   scores_df = csv_to_df(file)
   scores_df = convert_reference_columns_to_category(scores_df)
   scores_df = await change_reference_name_to_id(scores_df, year)
   scores_df = drop_columns(scores_df)
   scores_df = rename_columns(scores_df)
   scores_df = reorder_columns(scores_df, ["tournament_id", "stage_id", "match_type_id", "match_id", "team_a_id", "team_b_id",
                                            "team_a_score", "team_b_score", "match_result"])
   scores_df["year"] = year
   dfs[file_name]["main"].append(scores_df)


async def process_win_loss_methods_count(file, file_name, year, dfs):
   win_loss_methods_count_df = csv_to_df(file)
   win_loss_methods_count_df = convert_reference_columns_to_category(win_loss_methods_count_df)
   win_loss_methods_count_df = await change_reference_name_to_id(win_loss_methods_count_df, year)
   win_loss_methods_count_df = drop_columns(win_loss_methods_count_df)
   win_loss_methods_count_df = rename_columns(win_loss_methods_count_df)
   win_loss_methods_count_df = reorder_columns(win_loss_methods_count_df, ["tournament_id", "stage_id", "match_type_id", "match_id", "team_id",
                                                                           "map_id", 'elimination', 'detonated', 'defused', 'time_expiry_no_plant', "eliminated",
                                                                           'defused_failed', 'detonation_denied', 'time_expiry_failed_to_plant'])
   win_loss_methods_count_df["year"] = year
   dfs[file_name]["main"].append(win_loss_methods_count_df)
   

async def process_win_loss_methods_round_number(file, file_name, year, dfs):
   win_loss_methods_round_number_df = csv_to_df(file)
   win_loss_methods_round_number_df = convert_reference_columns_to_category(win_loss_methods_round_number_df)
   win_loss_methods_round_number_df = await change_reference_name_to_id(win_loss_methods_round_number_df, year)
   win_loss_methods_round_number_df = drop_columns(win_loss_methods_round_number_df)
   win_loss_methods_round_number_df = rename_columns(win_loss_methods_round_number_df)
   win_loss_methods_round_number_df = reorder_columns(win_loss_methods_round_number_df, ["tournament_id", "stage_id", "match_type_id", "match_id", "team_id",
                                                                                         "map_id", "round_number", "method", "outcome"])
   win_loss_methods_round_number_df["year"] = year
   dfs[file_name]["main"].append(win_loss_methods_round_number_df)

async def process_agents_pick_rates(file, file_name, year, dfs):
   agents_pick_rates_df = csv_to_df(file)
   agents_pick_rates_df = convert_reference_columns_to_category(agents_pick_rates_df)
   agents_pick_rates_df = await change_reference_name_to_id(agents_pick_rates_df, year)
   agents_pick_rates_df = drop_columns(agents_pick_rates_df)
   agents_pick_rates_df = convert_percentages_to_decimal(agents_pick_rates_df)
   agents_pick_rates_df = rename_columns(agents_pick_rates_df)
   agents_pick_rates_df = reorder_columns(agents_pick_rates_df, ["tournament_id", "stage_id", "match_type_id", "map_id", "agent_id",
                                                                 "pick_rate"])
   agents_pick_rates_df["year"] = year
   dfs[file_name]["main"].append(agents_pick_rates_df)


async def process_maps_stats(file, file_name, year, dfs):
   maps_stats_df = csv_to_df(file)
   maps_stats_df = convert_reference_columns_to_category(maps_stats_df)
   maps_stats_df = await change_reference_name_to_id(maps_stats_df, year)
   maps_stats_df = drop_columns(maps_stats_df)
   maps_stats_df = convert_percentages_to_decimal(maps_stats_df)
   maps_stats_df = rename_columns(maps_stats_df)
   maps_stats_df = reorder_columns(maps_stats_df, ["tournament_id", "stage_id", "match_type_id", "map_id", "total_maps_played",
                                                   "attacker_side_win_percentage", "defender_side_win_percentage"])
   maps_stats_df['year'] = year
   dfs[file_name]["main"].append(maps_stats_df)

async def process_teams_picked_agents(file, file_name, year, dfs):
   teams_picked_agents_df = csv_to_df(file)
   teams_picked_agents_df = convert_reference_columns_to_category(teams_picked_agents_df)
   teams_picked_agents_df = await change_reference_name_to_id(teams_picked_agents_df, year)
   teams_picked_agents_df = drop_columns(teams_picked_agents_df)
   teams_picked_agents_df = rename_columns(teams_picked_agents_df)
   teams_picked_agents_df = reorder_columns(teams_picked_agents_df, ["tournament_id", "stage_id", "match_type_id", "map_id",
                                                                     "agent_id", "total_wins_by_map", "total_loss_by_map", "total_maps_played"])
   teams_picked_agents_df["year"] = year
   dfs[file_name]["main"].append(teams_picked_agents_df)



async def process_players_stats(file, file_name, year, dfs):
   players_stats_df = csv_to_df(file)
   players_stats_df = remove_leading_zeroes_from_players(players_stats_df)
   players_stats_df = add_player_nan(players_stats_df)
   players_stats_df = convert_reference_columns_to_category(players_stats_df)
   players_stats_df = await change_reference_name_to_id(players_stats_df, year)
   players_stats_df = convert_clutches(players_stats_df)
   players_stats_df = drop_columns(players_stats_df)
   players_stats_df = convert_percentages_to_decimal(players_stats_df)
   players_stats_df = rename_columns(players_stats_df)
   players_stats_df = reorder_columns(players_stats_df, ["tournament_id", "stage_id", "match_type_id", "player_id", "teams", "agents", "rounds_played",
                                                         "rating", "acs", "kd", "kast", "adr", "kpr", "apr", "fkpr", "fdpr", "headshot",
                                                         "clutch_success", "clutches_won", "clutches_played", "mksp", "kills", "deaths", "assists",
                                                         "fk", "fd"])
   players_stats_df["year"] = year

   dfs[file_name]["main"].append(players_stats_df)


async def process_players_stats_agents(combined_dfs, combined_df):
   agents_df = combined_df[["index", "agents", "year"]]
   agents_df = splitting_agents(agents_df)
   agents_df.rename(columns={"agents": "Agent"}, inplace=True)
   agents_df = await change_reference_name_to_id(agents_df, 0)
   combined_df.drop(columns="agents", inplace=True)
   agents_df.drop(columns="Agent", inplace=True)
   agents_df = rename_columns(agents_df)
   agents_df = reorder_columns(agents_df, ["index", "agent_id", "year"])
   combined_dfs["players_stats.csv"]["agents"] = pd.concat([combined_dfs["players_stats.csv"]["agents"], agents_df], ignore_index=True)


async def process_players_stats_teams(combined_dfs, combined_df):
   teams_df = combined_df[["index", "teams", "year"]]
   teams_df = splitting_teams(teams_df)
   teams_df.rename(columns={"teams": "Team"}, inplace=True)
   combined_df.drop(columns="teams", inplace=True)
   teams_df = await change_reference_name_to_id(teams_df, 0)
   teams_df.drop(columns="Team", inplace=True)
   teams_df = rename_columns(teams_df)
   teams_df = reorder_columns(teams_df, ["index", "team_id", "year"])
   combined_dfs["players_stats.csv"]["teams"] = pd.concat([combined_dfs["players_stats.csv"]["teams"], teams_df], ignore_index=True)
 


async def process_csv_file(csv_file, year, dfs):
    file_name = csv_file.split("/")[-1]
    print(file_name, year)
    match file_name:
        # case "draft_phase.csv":
        #     await process_drafts(csv_file, file_name, year, dfs)
        # case "eco_rounds.csv":
        #     await process_eco_rounds(csv_file, file_name, year, dfs)
        # case "eco_stats.csv": 
        #     await process_eco_stats(csv_file, file_name, year, dfs)
        # case "kills.csv":
        #     await process_kills(csv_file, file_name, year, dfs)
        # case "kills_stats.csv":
        #     await process_kills_stats(csv_file, file_name, year, dfs)
        # case "maps_played.csv":
        #     await process_maps_played(csv_file, file_name, year, dfs)
        # case "maps_scores.csv":
        #     await process_maps_scores(csv_file, file_name, year, dfs)
        # case "overview.csv":
        #     await process_overview(csv_file, file_name, year, dfs)
        case "rounds_kills.csv":
            await process_rounds_kills(csv_file, file_name, year, dfs)
        # case "scores.csv":
        #     await process_scores(csv_file, file_name, year, dfs)
        # case "win_loss_methods_count.csv":
        #     await process_win_loss_methods_count(csv_file, file_name, year, dfs)
        # case "win_loss_methods_round_number.csv":
        #     await process_win_loss_methods_round_number(csv_file, file_name, year, dfs)
        # case "agents_pick_rates.csv":
        #     await process_agents_pick_rates(csv_file, file_name, year, dfs)
        # case "maps_stats.csv":
        #     await process_maps_stats(csv_file, file_name, year, dfs)
        # case "teams_picked_agents.csv":
        #     await process_teams_picked_agents(csv_file, file_name, year, dfs)
        # case "players_stats.csv":
        #     await process_players_stats(csv_file, file_name, year, dfs)
    

async def process_csv_files(csv_files, year, dfs):
    for csv_file in csv_files:
        await process_csv_file(csv_file, year, dfs)

async def process_years(csv_files_w_years, dfs):
    await asyncio.gather(
        *(process_csv_files(csv_files, year, dfs) for year, csv_files in csv_files_w_years.items())
    )
