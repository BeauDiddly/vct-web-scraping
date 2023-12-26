from Connect.execute_query import execute_query
import pandas as pd


def create_tournament_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS tournaments (
         tournament_id INT PRIMARY KEY,
         tournament_name VARCHAR(255) UNIQUE NOT NULL
   );
   """
   execute_query(curr, query)


def create_stage_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS stages (
         stage_id INT PRIMARY KEY,
         stage_name VARCHAR(255) UNIQUE NOT NULL
      );
      """
   execute_query(curr, query)

def create_match_type_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS match_types (
         match_type_id INT PRIMARY KEY,
         match_type_name VARCHAR(255) UNIQUE NOT NULL
      );
      """
   execute_query(curr, query)


def create_match_name_table(curr):
   query = """
   CREATE TABLE IF NOT EXISTS matches (
      match_id INT PRIMARY KEY,
      match_name VARCHAR(255) UNIQUE NOT NULL
   );
   """
   execute_query(curr, query)


def create_map_table(curr):
   query = """
   CREATE TABLE IF NOT EXISTS maps (
      map_id INT PRIMARY KEY,
      map_name VARCHAR(255) UNIQUE NOT NULL
   );
   """
   execute_query(curr, query)


def create_team_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS teams (
         team_id INT PRIMARY KEY,
         team_name VARCHAR(255) UNIQUE NOT NULL
      );
      """
   execute_query(curr, query)


def create_player_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS players (
         player_id INT PRIMARY KEY,
         player_name VARCHAR(255) UNIQUE NOT NULL
      );
      """
   execute_query(curr, query)


def create_agent_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS agents (
         agent_id INT PRIMARY KEY,
         agent_name VARCHAR(255) UNIQUE NOT NULL
      );
      """
   execute_query(curr, query)


def create_draft_phase_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS drafts (
         draft_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         team_id INT REFERENCES teams(team_id),
         action VARCHAR(255),
         map_id INT REFERENCES maps(map_id)
      );
   """
   execute_query(curr, query)

def create_eco_rounds_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS eco_rounds (
         eco_round_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         map_id INT REFERENCES maps(map_id),
         round_number INT,
         team_id INT REFERENCES teams(team_id),
         credits VARCHAR(255),
         eco_type VARCHAR(255),
         outcome VARCHAR(255)
      );
   """
   execute_query(curr, query)

def create_eco_stats_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS eco_stats (
         eco_stat_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         map_id INT REFERENCES maps(map_id),
         team_id INT REFERENCES teams(team_id),
         eco_type VARCHAR(255),
         initiated INT NULL,
         won INT
      );
   """
   execute_query(curr, query)

def create_kills_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS kills (
         kill_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         map_id INT REFERENCES maps(map_id),
         player_team_id INT REFERENCES teams(team_id),
         player_id INT REFERENCES players(player_id),
         enemy_team_id INT REFERENCES teams(team_id),
         enemy_id INT REFERENCES players(player_id),
         player_kills INT,
         enemy_kills INT,
         difference INT,
         kill_type VARCHAR(255)
      );
   """
   execute_query(curr, query)

def create_kills_stats_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS kills_stats (
         kill_stat_id SERIAL PRIMARY KEY
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         map_id INT REFERENCES maps(map_id),
         team_id INT REFERENCES teams(team_id),
         player_id INT REFERENCES players(player_id),
         agent_id INT REFERENCES agents(agent_id),
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
         spike_defuse INT
      );
   """
   execute_query(curr, query)

def create_maps_played_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS maps_played (
         map_played_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         map_id INT REFERENCES maps(map_id),
      );
   """
   execute_query(curr, query)

def create_maps_scores_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS maps_scores (
         map_score_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         map_id INT REFERENCES maps(map_id),
         team_a_id INT REFERENCES teams(team_id),
         team_a_score INT,
         team_a_attack_score INT,
         team_a_defender_score INT,
         team_a_overtime_score INT NULL,
         team_b_id INT REFERENCES teams(team_id),
         team_b_score INT,
         team_b_attack_score INT,
         team_b_defender_score INT,
         team_b_overtime_score INT NULL,
         duration INT NULL
      );
   """
   execute_query(curr, query)

def create_overview_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS overview (
         overview_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         map_id INT REFERENCES maps(map_id),
         player_id INT REFERENCES players(player_id),
         team_id INT REFERENCES teams(team_id),
         agent_id INT REFERENCES agents(agent_id),
         rating INT,
         average_combat_score INT,
         kills INT,
         deaths INT,
         assists INT,
         kill_deaths INT,
         kast_percentage DECIMAL,
         adr INT,
         headshot_percentage DECIMAL,
         first_kills INT,
         first_deaths INT,
         fkd INT,
         side VARCHAR(255)
      );
   """
   execute_query(curr, query)

def create_rounds_kills_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS rounds_kills (
         round_kill_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         map_id INT REFERENCES maps(map_id),
         round_number INT,
         eliminator_team_id INT REFERENCES teams(team_id),
         eliminator_id INT REFERENCES players(player_id),
         eliminator_agent_id INT REFERENCES agents(agent_id),
         eliminated_team_id INT REFERENCES teams(team_id),
         eliminated_id INT REFERENCES players(player_id),
         eliminated_agent_id INT REFERENCES agents(agent_id),
         kill_type VARCHAR(255)
      );
   """
   execute_query(curr, query)

def create_scores_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS scores (
         score_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         match_id INT REFERENCES matches(match_id),
         winner_id INT REFERENCES teams(team_id),
         loser_id INT REFERENCES teams(team_id),
         winner_score INT,
         loser_score INT
      );
   """
   execute_query(curr, query)

def create_agents_pick_rates_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS agents_pick_rates (
         pick_rate_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         map_id INT REFERENCES maps(map_id)
         match_id INT REFERENCES matches(match_id),
         agent_id INT REFERENCES agents(agent_id),
         pick_rate DECIMAL
      );
   """
   execute_query(curr, query)

def create_maps_stats_table(curr):
   query = """
   CREATE TABLE IF NOT EXISTS maps_stats (
      map_stat_id SERIAL PRIMARY KEY,
      tournament_id INT REFERENCES tournaments(tournament_id),
      stage_id INT REFERENCES stages(stage_id),
      match_type_id INT REFERENCES match_types(match_type_id),
      map_id INT REFERENCES mas(map_id),
      total_maps_played INT
      attacker_win_percentage DECIMAL
      defender_win_percentage DECIMAL
      );
   """
   execute_query(curr, query)

def create_teams_picked_agents_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS teams_picked_agents (
         team_picked_agent_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         map_id INT REFERENCES mas(map_id),
         team_id INT REFERENCES teams(team_id),
         agent_id REFERENCES agents(agent_id),
         total_wins_by_map INT,
         total_loss_by_map INT,
         total_maps_played INT
      );
   """
   execute_query(curr, query)

def create_players_stats_table(curr):
   query = """
      CREATE TABLE IF NOT EXISTS players_stats (
         player_stat_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournaments(tournament_id),
         stage_id INT REFERENCES stages(stage_id),
         match_type_id INT REFERENCES match_types(match_type_id),
         player_id INT REFERENCES players(player_id),
         team_id INT REFERENCES teams(team_id),
         agents_id INT REFERENCES agents(agent_id),
         rounds_played INT
         rating DECIMAL,
         average_combat_score INT,
         kills_deaths DECIMAL,
         kast DECIMAL,
         adr DECIMAL,
         kills_per_round DECIMAL,
         assists_per_round DECIMAL,
         first_kills_per_round DECIMAL,
         first_deaths_per_round DECIMAL,
         headshot_percentage DECIMAL,
         clutch_success DECIMAL NULL,
         clutches_won INT NULL,
         clutches_played INT NULL,
         mksp INT,
         kills INT,
         deaths INT,
         assists INT,
         first_kills INT,
         first_deaths INT
      );
   """