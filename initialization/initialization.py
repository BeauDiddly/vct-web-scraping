from Connect.connect import connect
from Connect.execute_query import execute_query
import pandas as pd


def create_tournament_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS Tournament (
      TournamentID SERIAL PRIMARY KEY,
      TournamentName VARCHAR(255) UNIQUE NOT NULL
   )
   """
   execute_query(curr, sql)

def add_tournaments(curr):
   tournaments = pd.read_csv("all_values/all_tournaments.csv")
   for index, tournament in tournaments["Tournament"].items():
      query = "INSERT INTO Tournament (TournamentName) VALUES (%s);"
      data = (tournament,)
      execute_query(curr, query, data)


def create_stage_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS Stage (
         StageID SERIAL PRIMARY KEY,
         StageName VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)

def add_stages(curr):
   stages = pd.read_csv("all_values/all_stages.csv")
   for index, stage in stages["Stage"].items():
      query = "INSERT INTO Stage (StageName) VALUES (%s);"
      data = (stage,)
      execute_query(curr, query, data)

def create_match_type_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS MatchType (
         MatchTypeID SERIAL PRIMARY KEY,
         MatchTypeName VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)

def add_match_types(curr):
   match_types = pd.read_csv("all_values/all_match_types.csv")
   for index, match_type in match_types["Match Type"].items():
      query = "INSERT INTO MatchType (MatchTypeName) VALUES (%s);"
      data = (match_type,)
      execute_query(curr, query, data)

def create_match_name_table(curr):
   sql = """
   CREATE TABLE IF NOT EXISTS MatchName (
      MatchNameID SERIAL PRIMARY KEY,
      MatchName VARCHAR(255) UNIQUE NOT NULL
   )
   """
   execute_query(curr, sql)

def add_match_names(curr):
   match_names = pd.read_csv("all_values/all_matches.csv")
   for index, match_name in match_names["Match Name"].items():
      query = "INSERT INTO MatchName (MatchName) VALUES (%s);"
      data = (match_name,)
      execute_query(curr, query, data)

def create_map_table(curr):
   sql = """
   CREATE TABLE IF NOT EXISTS Map (
      MapID SERIAL PRIMARY KEY,
      MapName VARCHAR(255) UNIQUE NOT NULL
   )
   """
   execute_query(curr, sql)

def add_maps(curr):
   maps = pd.read_csv("all_values/all_maps.csv")
   for index, map in maps["Map"].items():
      query = "INSERT INTO Map (MapName) VALUES (%s);"
      data = (map,)
      execute_query(curr, query, data)

def create_team_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS Team (
         TeamID SERIAL PRIMARY KEY,
         TeamName VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)

def add_teams(curr):
   teams = pd.read_csv("all_values/all_teams.csv")
   for index, team in teams["Team"].items():
      query = "INSERT INTO Team (TeamName) VALUES (%s);"
      data = (team,)
      execute_query(curr, query, data)

def create_player_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS Player (
         PlayerID SERIAL PRIMARY KEY,
         PlayerName VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)

def add_players(curr):
   players = pd.read_csv("all_values/all_players.csv")
   for index, player in players["Player"].items():
      query = "INSERT INTO Player (PlayerName) VALUES (%s);"
      data = (player,)
      execute_query(curr, query, data)

def create_agent_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS Agent (
         AgentID SERIAL PRIMARY KEY,
         AgentName VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)

def add_agents(curr):
   agents = pd.read_csv("all_values/all_agents.csv")
   for index, agent in agents["Agents"].items():
      query = "INSERT INTO Agent (AgentName) VALUES (%s);"
      data = (agent,)
      execute_query(curr, query, data)

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

