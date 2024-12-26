from Connect.execute_query import execute_query
from Connect.connect import connect

conn, curr = connect()
query = """
    DROP TABLE IF EXISTS draft_phase_agents CASCADE;
    DROP TABLE IF EXISTS draft_phase_teams CASCADE;
    DROP TABLE IF EXISTS draft_phase CASCADE;
    DROP TABLE IF EXISTS eco_rounds_agents CASCADE;
    DROP TABLE IF EXISTS eco_rounds_teams CASCADE;
    DROP TABLE IF EXISTS eco_rounds CASCADE;
    DROP TABLE IF EXISTS eco_stats_agents CASCADE;
    DROP TABLE IF EXISTS eco_stats_teams CASCADE;
    DROP TABLE IF EXISTS eco_stats CASCADE;
    DROP TABLE IF EXISTS kills_agents CASCADE;
    DROP TABLE IF EXISTS kills_teams CASCADE;
    DROP TABLE IF EXISTS kills CASCADE;
    DROP TABLE IF EXISTS kills_stats_agents CASCADE;
    DROP TABLE IF EXISTS kills_stats_teams CASCADE;
    DROP TABLE IF EXISTS kills_stats CASCADE;
    DROP TABLE IF EXISTS maps_played_agents CASCADE;
    DROP TABLE IF EXISTS maps_played_teams CASCADE;
    DROP TABLE IF EXISTS maps_played CASCADE;
    DROP TABLE IF EXISTS maps_scores_agents CASCADE;
    DROP TABLE IF EXISTS maps_scores_teams CASCADE;
    DROP TABLE IF EXISTS maps_scores CASCADE;
    DROP TABLE IF EXISTS overview_agents CASCADE;
    DROP TABLE IF EXISTS overview_teams CASCADE;
    DROP TABLE IF EXISTS overview CASCADE;
    DROP TABLE IF EXISTS rounds_kills_teams CASCADE;
    DROP TABLE IF EXISTS rounds_kills_agents CASCADE;
    DROP TABLE IF EXISTS rounds_kills CASCADE;
    DROP TABLE IF EXISTS scores_agents CASCADE;
    DROP TABLE IF EXISTS scores_teams CASCADE;
    DROP TABLE IF EXISTS scores CASCADE;
    DROP TABLE IF EXISTS agents_pick_rates CASCADE;
    DROP TABLE IF EXISTS maps_stats CASCADE;
    DROP TABLE IF EXISTS teams_picked_agents CASCADE;
    DROP TABLE IF EXISTS win_loss_methods_count_agents CASCADE;
    DROP TABLE IF EXISTS win_loss_methods_count_teams CASCADE;
    DROP TABLE IF EXISTS win_loss_methods_count CASCADE;
    DROP TABLE IF EXISTS win_loss_methods_round_number_teams CASCADE;
    DROP TABLE IF EXISTS win_loss_methods_round_number_agents CASCADE;
    DROP TABLE IF EXISTS win_loss_methods_round_number CASCADE;
    DROP TABLE IF EXISTS team_mapping_teams CASCADE;
    DROP TABLE IF EXISTS team_mapping_agents CASCADE;
    DROP TABLE IF EXISTS team_mapping CASCADE;
    DROP TABLE IF EXISTS players_stats_teams CASCADE;
    DROP TABLE IF EXISTS players_stats_agents CASCADE;
    DROP TABLE IF EXISTS players_stats CASCADE;
    DROP TABLE IF EXISTS stages CASCADE;
    DROP TABLE IF EXISTS match_types CASCADE;
    DROP TABLE IF EXISTS matches CASCADE;
    DROP TABLE IF EXISTS games CASCADE;
    DROP TABLE IF EXISTS maps CASCADE;
    DROP TABLE IF EXISTS teams CASCADE;
    DROP TABLE IF EXISTS players CASCADE;
    DROP TABLE IF EXISTS agents CASCADE;
    DROP TABLE IF EXISTS tournaments CASCADE;
"""
execute_query(curr, query)
conn.commit()
curr.close()
conn.close()
