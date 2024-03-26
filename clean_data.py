from find_csv_files.find_csv_files import find_csv_files
import os
import pandas as pd
from data_clean.data_clean import *

def main():
    years = [2021, 2022, 2023]

    matches_csv_files = [find_csv_files(f"{os.getcwd()}/vct_{year}/matches", "matches", year) for year in years]

    matches_result = {2021: {}, 2022: {}, 2023: {}}
    for i, file_list in enumerate(matches_csv_files):
        year = years[i]
        for file in file_list:
            file_name = file.split("/")[2]
            print(file_name)
            df = csv_to_df(file)
            df = remove_white_spaces(df)
            df = remove_white_spaces_in_between(df)
            df = remove_tabs_and_newlines(df)
            df = convert_to_str(df)
            df = fixed_team_names(df)
            df = fixed_player_names(df)
            df = convert_nan_players_teams(df)
            if file_name == "kills_stats.csv":
                df = get_all_agents_played_for_kills_stats(df)
            matches_result[year][file_name] = df
    for year, dataframes in matches_result.items():
        for file_name, dataframe in dataframes.items():
            dataframe.to_csv(f"cleaned_data/vct_{year}/matches/{file_name}", encoding="utf-8", index=False)

    agents_stats_csv_files = [find_csv_files(f"{os.getcwd()}/vct_{year}/agents", "agents", year) for year in years]

    agents_result = {2021: {}, 2022: {}, 2023: {}}
    for i, file_list in enumerate(agents_stats_csv_files):
        year = years[i]
        for file in file_list:
            file_name = file.split("/")[2]
            df = csv_to_df(file)
            df = remove_white_spaces(df)
            df = remove_white_spaces_in_between(df)
            df = remove_tabs_and_newlines(df)
            df = convert_to_str(df)
            df = fixed_team_names(df)
            df = fixed_player_names(df)
            agents_result[year][file_name] = df
    for year, dataframes in agents_result.items():
        for file_name, dataframe in dataframes.items():
            dataframe.to_csv(f"cleaned_data/vct_{year}/agents/{file_name}", encoding="utf-8", index=False)

    players_stats_csv_files = [find_csv_files(f"{os.getcwd()}/vct_{year}/players_stats", "players_stats", year) for year in years]
    players_result = {2021: {}, 2022: {}, 2023: {}}
    for i, file_list in enumerate(players_stats_csv_files):
        year = years[i]
        for file in file_list:
            file_name = file.split("/")[2]
            df = csv_to_df(file)
            df = remove_white_spaces(df)
            df = remove_white_spaces_in_between(df)
            df = remove_tabs_and_newlines(df)
            df = convert_to_str(df)
            df = convert_nan_players_teams(df)
            df = fixed_team_names(df)
            df = fixed_player_names(df)
            players_result[year][file_name] = df
    for year, dataframes in players_result.items():
        for file_name, dataframe in dataframes.items():
            dataframe.to_csv(f"cleaned_data/vct_{year}/players_stats/{file_name}", encoding="utf-8", index=False)

    ids_csv_files = [find_csv_files(f"{os.getcwd()}/vct_{year}/ids", "ids", year) for year in years]
    ids_result = {2021: {}, 2022: {}, 2023: {}}
    for i, file_list in enumerate(ids_csv_files):
        year = years[i]
        for file in file_list:
            file_name = file.split("/")[2]
            df = csv_to_df(file)
            df = remove_white_spaces(df)
            df = remove_white_spaces_in_between(df)
            df = remove_tabs_and_newlines(df)
            df = add_missing_player(df, year)
            df = convert_to_int(df)
            df = convert_to_str(df)
            ids_result[year][file_name] = df
    for year, dataframes in ids_result.items():
        for file_name, dataframe in dataframes.items():
            dataframe.to_csv(f"cleaned_data/vct_{year}/ids/{file_name}", encoding="utf-8", index=False)


    


if __name__ == '__main__':
    main()
