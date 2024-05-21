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
    all_tournaments_stages_match_types = process_tournaments_stages_match_types(all_tournaments_stages_match_types)
    upper_round_id = get_upper_round_id(all_tournaments_stages_match_types)
    tournaments_df = process_tournaments(all_tournaments_stages_match_types)
    stages_df = process_stages(all_tournaments_stages_match_types)
    match_types_df = process_match_types(all_tournaments_stages_match_types)
    matches_df = process_matches(all_matches, upper_round_id)
    teams_df = process_teams_ids(all_teams_id)
    players_df = process_players_ids(all_players_id)
    add_tournaments(tournaments_df, sql_alchemy_engine)
    add_stages(stages_df, sql_alchemy_engine)
    add_match_types(match_types_df, sql_alchemy_engine)
    add_matches(matches_df, sql_alchemy_engine)
    add_teams(teams_df, sql_alchemy_engine)
    add_players(players_df, sql_alchemy_engine)
    curr.close()
    conn.close()

if __name__ == '__main__':
    main()