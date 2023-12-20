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



# def create_match_table():
#    sql = """
#       CREATE TABLE Match (
#          MatchID SERIAL PRIMARY KEY,
#          TournamentID INT REFERENCES Tournament(TournamentID),
#          Stage VARCHAR(255),
#          match_type VARCHAR(255),
#          match_name VARCHAR(255),
#          map_name VARCHAR(255),
#          PRIMARY KEY (tournament_id, stage, match_type, match_name, map_name)
#       )
#       """
   
# def create_player_stats_table():
#    sql = """
#       CREATE TABLE PlayerStats (
      
#    )
#    """

