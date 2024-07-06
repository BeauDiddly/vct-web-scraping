from initialization.add_data import add_reference_data, add_agents, add_maps
from Connect.connect import connect, engine
from initialization.create_tables import create_all_tables
from process.process_df import csv_to_df, process_tournaments_stages_match_types, get_upper_round_id, process_tournaments, process_stages, process_match_types, process_matches, process_teams_ids, process_players_ids

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
    add_reference_data(tournaments_df, "tournaments", sql_alchemy_engine)
    add_reference_data(stages_df, "stages", sql_alchemy_engine)
    add_reference_data(match_types_df, "match_types", sql_alchemy_engine)
    add_reference_data(matches_df, "matches", sql_alchemy_engine)
    add_reference_data(teams_df, "teams", sql_alchemy_engine)
    add_reference_data(players_df, "players", sql_alchemy_engine)
    curr.close()
    conn.close()

if __name__ == '__main__':
    main()