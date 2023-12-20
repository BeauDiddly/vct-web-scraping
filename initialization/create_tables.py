from Connect.execute_query import execute_query
import pandas as pd


def create_tournament_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS tournament (
      tournament_id INT PRIMARY KEY,
      tournament_name VARCHAR(255) UNIQUE NOT NULL
   )
   """
   execute_query(curr, sql)


def create_stage_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS stage (
         stage_id INT PRIMARY KEY,
         stage_name VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)

def create_match_type_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS match_type (
         match_type_id INT PRIMARY KEY,
         match_type_name VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)


def create_match_name_table(curr):
   sql = """
   CREATE TABLE IF NOT EXISTS match (
      match_id INT PRIMARY KEY,
      match_name VARCHAR(255) UNIQUE NOT NULL
   )
   """
   execute_query(curr, sql)


def create_map_table(curr):
   sql = """
   CREATE TABLE IF NOT EXISTS map (
      map_id INT PRIMARY KEY,
      map_name VARCHAR(255) UNIQUE NOT NULL
   )
   """
   execute_query(curr, sql)


def create_team_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS team (
         team_id INT PRIMARY KEY,
         team_name VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)


def create_player_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS player (
         player_id INT PRIMARY KEY,
         player_name VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)


def create_agent_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS agent (
         agent_id INT PRIMARY KEY,
         agent_name VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)


def create_draft_phase_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS draft (
         draft_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournament(tournament_id),
         stage_id INT REFERENCES stage(stage_id),
         match_type_id INT REFERENCES match_type(match_type_id),
         match_id INT REFERENCES match(match_id),
         team_id INT REFERENCES team(team_id),
         action VARCHAR(255),
         map_id INT REFERENCES map(map_id)
      )
   """
   execute_query(curr, sql)

def create_eco_rounds_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS eco_rounds (
         eco_round_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournament(tournament_id),
         stage_id INT REFERENCES stage(stage_id),
         match_type_id INT REFERENCES match_type(match_type_id),
         match_id INT REFERENCES match(match_id),
         map_id INT REFERENCES map(map_id),
         round_number INT,
         team_id INT REFERENCES team(team_id),
         credits VARCHAR(255),
         eco_type VARCHAR(255),
         outcome VARCHAR(255)
      )
   """

def create_eco_stats_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS eco_stats (
         eco_stat_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournament(tournament_id),
         stage_id INT REFERENCES stage(stage_id),
         match_type_id INT REFERENCES match_type(match_type_id),
         match_id INT REFERENCES match(match_id),
         map_id INT REFERENCES map(map_id),
         eco_type VARCHAR(255),
         initiated INT NULL,
         won INT
      )
   """

def create_kills_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS kills (
         kill_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournament(tournament_id),
         stage_id INT REFERENCES stage(stage_id),
         match_type_id INT REFERENCES match_type(match_type_id),
         match_id INT REFERENCES match(match_id),
         map_id INT REFERENCES map(map_id),
         player_team_id INT REFERENCES team(team_id),
         player_id INT references player(player_id),
         enemy_team_id INT REFERENCES team(team_id),
         enemy_id INT references enemy(player_id),
         player_kills INT,
         enemy_kills INT,
         difference INT,
         kill_type VARCHAR(255)
      )
   """

def create_kills_stats_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS kill_stats (
         kill_stat_id SERIAL PRIMARY KEY
         tournament_id INT REFERENCES tournament(tournament_id),
         stage_id INT REFERENCES stage(stage_id),
         match_type_id INT REFERENCES match_type(match_type_id),
         match_id INT REFERENCES match(match_id),
         map_id INT REFERENCES map(map_id),
         team_id INT REFERENCES team(team_id),
         player_id INT REFERENCES player(player_id),
         agent_id INT REFERENCES agent(agent_id),
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
      )
   """

def create_maps_played_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS maps_played (
         map_played_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournament(tournament_id),
         stage_id INT REFERENCES stage(stage_id),
         match_type_id INT REFERENCES match_type(match_type_id),
         match_id INT REFERENCES match(match_id),
         map_id INT REFERENCES map(map_id),
      )
   """

def create_maps_scores_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS maps_scores (
         map_score_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournament(tournament_id),
         stage_id INT REFERENCES stage(stage_id),
         match_type_id INT REFERENCES match_type(match_type_id),
         match_id INT REFERENCES match(match_id),
         map_id INT REFERENCES map(map_id),
         team_a_id INT REFERENCES team(team_id),
         team_a_score INT,
         team_a_attack_score INT,
         team_a_defender_score INT,
         team_a_overtime_score INT NULL,
         team_b_id INT REFERENCES team(team_id),
         team_b_score INT,
         team_b_attack_score INT,
         team_b_defender_score INT,
         team_b_overtime_score INT NULL,
         duration INT NULL
      )
   """

def create_overview_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS overview (
         overview_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournament(tournament_id),
         stage_id INT REFERENCES stage(stage_id),
         match_type_id INT REFERENCES match_type(match_type_id),
         match_id INT REFERENCES match(match_id),
         map_id INT REFERENCES map(map_id),
         player_id INT REFERENCES player(player_id),
         team_id INT REFERENCES team(team_id),
         agent_id INT REFERENCES agent(agent_id),
         rating INT,
         average_combat_score INT,
         kills INT,
         deaths INT,
         assists INT,
         kill_deaths INT,
         kast_percentage VARCHAR(255),
         adr INT,
         headshot_percentage VARCHAR(255),
         first_kills INT,
         first_deaths INT,
         fkd INT,
         side VARCHAR(255)
      )
   """

def create_rounds_kills_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS rounds_kills (
         round_kill_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournament(tournament_id),
         stage_id INT REFERENCES stage(stage_id),
         match_type_id INT REFERENCES match_type(match_type_id),
         match_id INT REFERENCES match(match_id),
         map_id INT REFERENCES map(map_id),
         round_number INT,
         eliminator_team_id INT REFERENCES team(team_id),
         eliminator_id INT REFERENCES player(player_id),
         eliminator_agent_id INT REFERENCES agent(agent_id),
         eliminated_team_id INT REFERENCES team(team_id),
         eliminated_id INT REFERENCES player(player_id),
         eliminated_agent_id INT REFERENCES agent(agent_id),
         kill_type VARCHAR(255)
      )
   """

def create_scores_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS scores (
         score_id SERIAL PRIMARY KEY,
         tournament_id INT REFERENCES tournament(tournament_id),
         stage_id INT REFERENCES stage(stage_id),
         match_type_id INT REFERENCES match_type(match_type_id),
         match_id INT REFERENCES match(match_id),
         winner_id INT REFERENCES team(team_id),
         loser_id INT REFERENCES team(team_id),
         winner_score INT,
         loser_score INT
      )
   """