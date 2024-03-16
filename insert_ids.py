from initialization.add_data import *
import pandas as pd
from Connect.connect import connect, engine
from initialization.create_tables import create_all_tables
from process_df.process_df import csv_to_df

def main():
    conn, curr = connect()
    sql_alchemy_engine = engine()
    all_players_id = csv_to_df("all_vct/all_players_ids.csv")
    all_matches = csv_to_df("all_vct/all_matches_games_ids.csv")
    all_teams_id = csv_to_df("all_vct/all_teams_ids.csv")
    all_tournaments_stages_match_types = csv_to_df("all_vct/all_tournaments_stages_match_types_ids.csv")
    create_all_tables(curr)    
    conn.commit()

    years = [2021, 2022, 2023]

    multiple_teams = set()

    for year in years:
        df = csv_to_df(f"vct_{year}/players_stats/players_stats.csv")
        condition = (df['Team'] != "Stay Small, Stay Second") & (df['Team'].str.contains(','))
        filtered_df = df[condition]
        filtered_values = filtered_df["Team"].drop_duplicates().values
        multiple_teams.update(filtered_values)
    multiple_teams = list(multiple_teams)
    add_tournaments_stages_match_types(all_tournaments_stages_match_types, sql_alchemy_engine)
    add_matches(all_matches, sql_alchemy_engine)
    add_teams(all_teams_id, multiple_teams, sql_alchemy_engine)
    add_players(all_players_id, sql_alchemy_engine)
    curr.close()
    conn.close()

if __name__ == '__main__':
    main()