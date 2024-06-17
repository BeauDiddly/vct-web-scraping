from Connect.execute_query import execute_query
import pandas as pd


def create_tournaments_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS tournaments (
         tournament_id INT PRIMARY KEY,
         tournament VARCHAR(255) NOT NULL,
         year INT
   );
   """
   execute_query(curr, query)


def create_stages_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS stages (
         stage_id INT PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage VARCHAR(255) NOT NULL,
         year INT
      );
      """
   execute_query(curr, query)

def create_match_types_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS match_types (
         match_type_id INT PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type VARCHAR(255) NOT NULL,
         year INT
      );
      """
   execute_query(curr, query)


def create_matches_table(curr):
   query = """
   CREATE TABLE IF NOT EXISTS matches (
      match_id INT PRIMARY KEY,
      tournament_id INT REFERENCES tournaments(tournament_id),
      stage_id INT REFERENCES stages(stage_id),
      match_type_id INT REFERENCES match_types(match_type_id),
      match VARCHAR(255) NOT NULL,
      year INT
   );
   """
   execute_query(curr, query)

def create_agents_table(curr):
   query = """
   CREATE TABLE IF NOT EXISTS agents (
      agent_id INT PRIMARY KEY,
      agent VARCHAR(255) NOT NULL
   );
   """
   execute_query(curr, query)

def create_maps_table(curr): 
   query = """
   CREATE TABLE IF NOT EXISTS maps (
      map_id INT PRIMARY KEY,
      map VARCHAR(255) NOT NULL
   );
   """
   execute_query(curr, query)


# def create_games_table(curr):
#    query = """
#    CREATE TABLE IF NOT EXISTS games (
#       game_id INT PRIMARY KEY,
#       tournament_id INT REFERENCES tournaments(tournament_id),
#       stage_id INT REFERENCES stages(stage_id),
#       match_type_id INT REFERENCES match_types(match_type_id),
#       match_id INT REFERENCES matches(match_id),
#       map_id INT REFERENCES maps(map_id),
#       year INT
#    );
#    """
#    execute_query(curr, query)

def create_teams_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS teams (
         team_id INT PRIMARY KEY,
         team VARCHAR(255) NOT NULL
      );
      """
   execute_query(curr, query)


def create_players_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS players (
         player_id INT PRIMARY KEY,
         player VARCHAR(255) NOT NULL
      );
      """
   execute_query(curr, query)


def create_draft_phase_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS draft_phase (
         index INT PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         team_id INT REFERENCES teams(team_id),
         map_id INT REFERENCES maps(map_id),
         action VARCHAR(255),
         year INT
      );
   """
   execute_query(curr, query)

def create_eco_rounds_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS eco_rounds (
         index INT PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         team_id INT REFERENCES teams(team_id),
         map_id INT REFERENCES maps(map_id),
         round_number INT,
         loadout_value VARCHAR(255),
         remaining_credits VARCHAR(255),
         type VARCHAR(255),
         outcome VARCHAR(255),
         year INT NOT NULL

      );
   """
   execute_query(curr, query)

def create_eco_stats_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS eco_stats (
         index INT PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         team_id INT REFERENCES teams(team_id),
         map_id INT REFERENCES maps(map_id),
         type VARCHAR(255),
         initiated INT NULL,
         won INT,
         year INT NOT NULL

      );
   """
   execute_query(curr, query)

def create_kills_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS kills (
         index INT PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         player_team_id INT REFERENCES teams(team_id),
         player_id INT REFERENCES players(player_id),
         enemy_team_id INT REFERENCES teams(team_id),
         enemy_id INT REFERENCES players(player_id),
         map_id INT REFERENCES maps(map_id),
         player_kills INT,
         enemy_kills INT,
         difference INT,
         kill_type VARCHAR(255),
         year INT NOT NULL

      );
   """
   execute_query(curr, query)

def create_kills_stats_agents_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS kills_stats_agents (
         index INT REFERENCES kills_stats(index),
         agent_id INT REFERENCES agents(agent_id),
         year INT,
         PRIMARY KEY (index, agent_id, year)
      );
   """
   execute_query(curr, query)

def create_kills_stats_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS kills_stats (
         index INT PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         team_id INT REFERENCES teams(team_id),
         player_id INT REFERENCES players(player_id),
         map_id INT REFERENCES maps(map_id),
         two_kills INT NULL,
         three_kills INT NULL,
         four_kills INT NULL,
         five_kills INT NULL,
         one_vs_one INT NULL,
         one_vs_two INT NULL,
         one_vs_three INT NULL,
         one_vs_four INT NULL,
         one_vs_five INT NULL,
         econ INT,
         spike_plants INT,
         spike_defuse INT,
         year INT NOT NULL

      );
   """
   execute_query(curr, query)

def create_maps_played_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS maps_played (
         index INT PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         map_id INT REFERENCES maps(map_id) NULL,
         year INT NOT NULL

      );
   """
   execute_query(curr, query)

def create_maps_scores_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS maps_scores (
         index INT PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         map_id INT REFERENCES maps(map_id),
         team_a_id INT REFERENCES teams(team_id),
         team_b_id INT REFERENCES teams(team_id),
         team_a_score INT,
         team_a_attacker_score INT,
         team_a_defender_score INT,
         team_a_overtime_score INT NULL,
         team_b_score INT,
         team_b_attacker_score INT,
         team_b_defender_score INT,
         team_b_overtime_score INT NULL,
         duration INTERVAL NULL,
         year INT NOT NULL

      );
   """
   execute_query(curr, query)

def create_overview_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS overview (
         index INT PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         player_id INT REFERENCES players(player_id),
         team_id INT REFERENCES teams(team_id),
         map_id INT REFERENCES maps(map_id),
         agent_id INT REFERENCES agents(agent_id),
         rating INT NULL,
         acs INT NULL,
         kills INT NULL,
         deaths INT NULL,
         assists INT NULL,
         kd INT NULL,
         kast DECIMAL NULL,
         adpr INT NULL,
         headshot DECIMAL NULL,
         fk INT NULL,
         fd INT NULL,
         fkd INT NULL,
         side VARCHAR(255),
         year INT NOT NULL

      );
   """
   execute_query(curr, query)

def create_overview_agents_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS overview_agents (
         index INT REFERENCES overview(index),
         agent_id INT REFERENCES agents(agent_id),
         year INT,
         PRIMARY KEY (index, agent_id, year)
      );
   """
   execute_query(curr, query)

def create_rounds_kills_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS rounds_kills (
         index INT PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         map_id INT REFERENCES maps(map_id),
         eliminator_team_id INT REFERENCES teams(team_id),
         eliminated_team_id INT REFERENCES teams(team_id),
         eliminator_id INT REFERENCES players(player_id),
         eliminated_id INT REFERENCES players(player_id),
         eliminator_agent_id INT REFERENCES agents(agent_id),
         eliminated_agent_id INT REFERENCES agents(agent_id),
         round_number VARCHAR(255),
         kill_type VARCHAR(255),
         year INT NOT NULL
      );
   """
   execute_query(curr, query)

def create_scores_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS scores (
         index INT PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         team_a_id INT REFERENCES teams(team_id),
         team_b_id INT REFERENCES teams(team_id),
         team_a_score INT,
         team_b_score INT,
         match_result VARCHAR(255),
         year INT NOT NULL
      );
   """
   execute_query(curr, query)

def create_win_loss_methods_count_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS win_loss_methods_count (
         index INT PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         team_id INT REFERENCES teams(team_id),
         map_id INT REFERENCES maps(map_id),
         elimination INT,
         detonated INT,
         defused INT,
         time_expiry_no_plant INT,
         eliminated INT,
         defused_failed INT,
         detonation_denied INT,
         time_expiry_failed_to_plant INT
      );
   """
   execute_query(curr, query)

def create_win_loss_methods_round_number_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS win_loss_methods_round_number (
         index INT PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         team_id INT REFERENCES teams(team_id),
         map_id INT REFERENCES maps(map_id),
         round_number INT,
         method VARCHAR(255),
         outcome VARCHAR(255)
      );
   """
   execute_query(curr, query)

def create_agents_pick_rates_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS agents_pick_rates (
         index INT PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         map_id INT REFERENCES maps(map_id),
         agent_id INT REFERENCES agents(agent_id),
         pick_rate DECIMAL,
         year INT NOT NULL
      );
   """
   execute_query(curr, query)

def create_maps_stats_table(curr):
   query = """
   CREATE TABLE IF NOT EXISTS maps_stats (
      index INT PRIMARY KEY,
      tournament_id INT REFERENCES tournaments(tournament_id),
      stage_id INT REFERENCES stages(stage_id),
      match_type_id INT REFERENCES match_types(match_type_id),
      map_id INT REFERENCES maps(map_id),
      total_maps_played INT,
      attacker_side_win_percentage DECIMAL,
      defender_side_win_percentage DECIMAL,
      year INT NOT NULL
      );
   """
   execute_query(curr, query)

def create_teams_picked_agents_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS teams_picked_agents (
         index INT PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         team_id INT REFERENCES teams(team_id),
         map_id INT REFERENCES maps(map_id),
         agent_id INT REFERENCES agents(agent_id),
         total_wins_by_map INT,
         total_loss_by_map INT,
         total_maps_played INT,
         year INT NOT NULL
      );
   """
   execute_query(curr, query)

def create_players_stats_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS players_stats (
         index INT PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         player_id INT REFERENCES players(player_id),
         rounds_played INT,
         rating DECIMAL,
         acs INT,
         kills_deaths DECIMAL,
         kast DECIMAL,
         adr DECIMAL,
         kpr DECIMAL,
         apr DECIMAL,
         fkpr DECIMAL,
         fdpr DECIMAL,
         headshot DECIMAL,
         clutch_success DECIMAL NULL,
         clutch_won INT NULL,
         clutch_played INT NULL,
         mksp INT,
         kills INT,
         deaths INT,
         assists INT,
         fk INT,
         fd INT,
         year INT NOT NULL
      );
   """
   execute_query(curr, query)

def create_players_stats_agents_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS players_stats_agents (
         index INT REFERENCES players_stats(index),
         agent_id INT REFERENCES agents(agent_id),
         year INT,
         PRIMARY KEY (index, agent_id, year)
      );
   """
   execute_query(curr, query)

def create_players_stats_teams_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS players_stats_teams (
         index INT REFERENCES players_stats(index),
         team_id INT REFERENCES teams(team_id),
         year INT,
         PRIMARY KEY (index, team_id, year)
      );
   """
   execute_query(curr, query)

def create_all_tables(curr):
   create_agents_table(curr)
   create_maps_table(curr)
   create_tournaments_table(curr)
   create_stages_table(curr)
   create_match_types_table(curr)
   create_matches_table(curr)
   # create_games_table(curr)
   create_agents_table(curr)
   create_maps_table(curr)
   create_teams_table(curr)
   create_players_table(curr)
   create_draft_phase_table(curr)
   create_eco_rounds_table(curr)
   create_eco_stats_table(curr)
   create_kills_table(curr)
   create_kills_stats_table(curr)
   create_kills_stats_agents_table(curr)
   create_maps_played_table(curr)
   create_maps_scores_table(curr)
   create_overview_table(curr)
   create_overview_agents_table(curr)
   create_rounds_kills_table(curr)
   create_scores_table(curr)
   create_agents_pick_rates_table(curr)
   create_maps_stats_table(curr)
   create_teams_picked_agents_table(curr)
   create_players_stats_table(curr)
   create_players_stats_agents_table(curr)
   create_players_stats_teams_table(curr)
   create_win_loss_methods_count_table(curr)
   create_win_loss_methods_round_number_table(curr)