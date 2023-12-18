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

def add_tournaments(curr):
   tournaments = pd.read_csv("all_values/all_tournaments.csv")
   for index, tournament in tournaments["Tournament"].items():
      query = "INSERT INTO tournament (tournament_name) VALUES (%s);"
      data = (tournament,)
      execute_query(curr, query, data)


def create_stage_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS stage (
         stage_id SERIAL PRIMARY KEY,
         stage_name VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)

def add_stages(curr):
   stages = pd.read_csv("all_values/all_stages.csv")
   for index, stage in stages["Stage"].items():
      query = "INSERT INTO stage (stage_name) VALUES (%s);"
      data = (stage,)
      execute_query(curr, query, data)

def create_match_type_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS match_type (
         match_type_id SERIAL PRIMARY KEY,
         match_type_name VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)

def add_match_types(curr):
   match_types = pd.read_csv("all_values/all_match_types.csv")
   for index, match_type in match_types["Match Type"].items():
      query = "INSERT INTO match_type (match_type_name) VALUES (%s);"
      data = (match_type,)
      execute_query(curr, query, data)

def create_match_name_table(curr):
   sql = """
   CREATE TABLE IF NOT EXISTS match (
      match_id SERIAL PRIMARY KEY,
      match_name VARCHAR(255) UNIQUE NOT NULL
   )
   """
   execute_query(curr, sql)

def add_match_names(curr):
   match_names = pd.read_csv("all_values/all_matches.csv")
   for index, match_name in match_names["Match Name"].items():
      query = "INSERT INTO match (match_name) VALUES (%s);"
      data = (match_name,)
      execute_query(curr, query, data)

def create_map_table(curr):
   sql = """
   CREATE TABLE IF NOT EXISTS map (
      map_id SERIAL PRIMARY KEY,
      map_name VARCHAR(255) UNIQUE NOT NULL
   )
   """
   execute_query(curr, sql)

def add_maps(curr):
   maps = pd.read_csv("all_values/all_maps.csv")
   for index, map in maps["Map"].items():
      query = "INSERT INTO map (map_name) VALUES (%s);"
      data = (map,)
      execute_query(curr, query, data)

def create_team_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS team (
         team_id SERIAL PRIMARY KEY,
         team_name VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)

def add_teams(curr):
   teams = pd.read_csv("all_values/all_teams.csv")
   for index, team in teams["Team"].items():
      query = "INSERT INTO team (team_name) VALUES (%s);"
      data = (team,)
      execute_query(curr, query, data)

def create_player_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS player (
         player_id SERIAL PRIMARY KEY,
         player_name VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)

def add_players(curr):
   players = pd.read_csv("all_values/all_players.csv")
   for index, player in players["Player"].items():
      query = "INSERT INTO player (player_name) VALUES (%s);"
      data = (player,)
      execute_query(curr, query, data)

def create_agent_table(curr):
   sql = """
      CREATE TABLE IF NOT EXISTS agent (
         agent_id SERIAL PRIMARY KEY,
         agent_name VARCHAR(255) UNIQUE NOT NULL
      )
      """
   execute_query(curr, sql)

def add_agents(curr):
   agents = pd.read_csv("all_values/all_agents.csv")
   for index, agent in agents["Agents"].items():
      query = "INSERT INTO agent (agent_name) VALUES (%s);"
      data = (agent,)
      execute_query(curr, query, data)

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

