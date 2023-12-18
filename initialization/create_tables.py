from Connect.execute_query import execute_query
import pandas as pd


def create_tournament_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS tournament (
      tournament_id SERIAL PRIMARY KEY,
      tournament_name VARCHAR(255) UNIQUE NOT NULL
   )
   """
   execute_query(curr, sql)


def create_stage_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS stage (
         stage_id SERIAL PRIMARY KEY,
         stage_name VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)

def create_match_type_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS match_type (
         match_type_id SERIAL PRIMARY KEY,
         match_type_name VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)


def create_match_name_table(curr):
   sql = """
   CREATE TABLE IF NOT EXISTS match (
      match_id SERIAL PRIMARY KEY,
      match_name VARCHAR(255) UNIQUE NOT NULL
   )
   """
   execute_query(curr, sql)


def create_map_table(curr):
   sql = """
   CREATE TABLE IF NOT EXISTS map (
      map_id SERIAL PRIMARY KEY,
      map_name VARCHAR(255) UNIQUE NOT NULL
   )
   """
   execute_query(curr, sql)


def create_team_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS team (
         team_id SERIAL PRIMARY KEY,
         team_name VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)


def create_player_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS player (
         player_id SERIAL PRIMARY KEY,
         player_name VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)


def create_agent_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS agent (
         agent_id SERIAL PRIMARY KEY,
         agent_name VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)


# def create_draft_phase_table(curr):
#    sql = """
#       CREATE TABLE IF NOT EXISTS draft (
#          draft_id SERIAL PRIMARY KEY,
#          tournament_id INT REFERENCES Tournament(TournamentID),
#          stage_id INT REFERENCES Stage(StageID),
#          match_type_id INT REFERENCES MatchType(MatchTypeID),
#          match_id INT REFERENCES MatchName(MatchNameID),
#          team_id INT REFERENCES Team(TeamID),
#          action VARCHAR(255),
#          map_id INT REFERENCES Map(MapID)
#       )
#    """
#    execute_query(curr, sql)

# def add_drafts_to_draft_phase_table(curr):


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

