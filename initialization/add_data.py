from Connect.execute_query import execute_query
import pandas as pd

def add_tournaments(curr):
   tournaments = pd.read_csv("all_values/all_tournaments.csv")
   for index, tournament in tournaments["Tournament"].items():
      query = "INSERT INTO tournament (tournament_name) VALUES (%s);"
      data = (tournament,)
      execute_query(curr, query, data)
    
def add_stages(curr):
   stages = pd.read_csv("all_values/all_stages.csv")
   for index, stage in stages["Stage"].items():
      query = "INSERT INTO stage (stage_name) VALUES (%s);"
      data = (stage,)
      execute_query(curr, query, data)

def add_match_types(curr):
   match_types = pd.read_csv("all_values/all_match_types.csv")
   for index, match_type in match_types["Match Type"].items():
      query = "INSERT INTO match_type (match_type_name) VALUES (%s);"
      data = (match_type,)
      execute_query(curr, query, data)

def add_match_names(curr):
   match_names = pd.read_csv("all_values/all_matches.csv")
   for index, match_name in match_names["Match Name"].items():
      query = "INSERT INTO match (match_name) VALUES (%s);"
      data = (match_name,)
      execute_query(curr, query, data)

def add_maps(curr):
   maps = pd.read_csv("all_values/all_maps.csv")
   for index, map in maps["Map"].items():
      query = "INSERT INTO map (map_name) VALUES (%s);"
      data = (map,)
      execute_query(curr, query, data)

def add_teams(curr):
   teams = pd.read_csv("all_values/all_teams.csv")
   for index, team in teams["Team"].items():
      query = "INSERT INTO team (team_name) VALUES (%s);"
      data = (team,)
      execute_query(curr, query, data)

def add_players(curr):
   players = pd.read_csv("all_values/all_players.csv")
   for index, player in players["Player"].items():
      query = "INSERT INTO player (player_name) VALUES (%s);"
      data = (player,)
      execute_query(curr, query, data)

def add_agents(curr):
   agents = pd.read_csv("all_values/all_agents.csv")
   for index, agent in agents["Agents"].items():
      query = "INSERT INTO agent (agent_name) VALUES (%s);"
      data = (agent,)
      execute_query(curr, query, data)