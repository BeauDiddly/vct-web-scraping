from initialization.add_data import *
import pandas as pd
from Connect.connect import connect, engine
from initialization.create_tables import create_all_tables
from process_df.process_df import csv_to_df

def main():
    conn, curr = connect()
    sql_alchemy_engine = engine()
    all_players_id = csv_to_df("all_ids/all_players_ids.csv")
    all_matches = csv_to_df("all_ids/all_matches_games_ids.csv")
    all_teams_id = csv_to_df("all_ids/all_teams_ids.csv")
    all_tournaments_stages_match_types = csv_to_df("all_ids/all_tournaments_stages_match_types_ids.csv")
    create_all_tables(curr)    
    conn.commit()
    add_agents(sql_alchemy_engine)
    add_maps(sql_alchemy_engine)
    upper_round_id = add_tournaments_stages_match_types(all_tournaments_stages_match_types, sql_alchemy_engine)
    add_matches(all_matches, upper_round_id, sql_alchemy_engine)
    add_teams(all_teams_id, sql_alchemy_engine)
    add_players(all_players_id, sql_alchemy_engine)
    curr.close()
    conn.close()

if __name__ == '__main__':
    main()