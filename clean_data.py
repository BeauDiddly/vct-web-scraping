from find_csv_files.find_csv_files import find_csv_files
import os
import pandas as pd
from data_clean.data_clean import *

def main():
    years = [2021, 2022, 2023]

    csv_files = [find_csv_files(f"{os.getcwd()}/vct_{year}/matches", year) for year in years]
    print(csv_files)
    result = {2021: {}, 2022: {}, 2023: {}}
    for i, file_list in enumerate(csv_files):
        year = years[i]
        for file in file_list:
            file_name = file.split("/")[2]
            df = pd.read_csv(file)
            df = remove_white_spaces(df)
            df = remove_white_spaces_in_between(df)
            df = remove_tabs_and_newlines(df)
            df = fixed_team_names(df)
            df = insert_missing_players(df)
            result[year][file_name] = df
    for year, dataframes in result.items():
        for file_name, dataframe in dataframes.items():
            dataframe.to_csv(f"cleaned_data/vct_{year}/matches/{file_name}")

if __name__ == '__main__':
    main()
