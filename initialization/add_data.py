from Connect.execute_query import execute_query
import pandas as pd
from retrieve.retrieve import retrieve_foreign_key
from generate.generate_unique_id import generate_unique_id

def add_tournaments(curr, unique_ids):
   tournaments = pd.read_csv("all_values/all_tournaments.csv")
   for index, tournament in tournaments["Tournament"].items():
      id = generate_unique_id(unique_ids)
      query = "INSERT INTO tournament (tournament_id, tournament_name) VALUES (%s, %s);"
      data = (id, tournament)
      execute_query(curr, query, data)
    
def add_stages(curr, unique_ids):
   stages = pd.read_csv("all_values/all_stages.csv")
   for index, stage in stages["Stage"].items():
      id = generate_unique_id(unique_ids)
      query = "INSERT INTO stage (stage_id, stage_name) VALUES (%s, %s);"
      data = (id, stage)
      execute_query(curr, query, data)

def add_match_types(curr, unique_ids):
   match_types = pd.read_csv("all_values/all_match_types.csv")
   for index, match_type in match_types["Match Type"].items():
      id = generate_unique_id(unique_ids)
      query = "INSERT INTO match_type (match_type_id, match_type_name) VALUES (%s, %s);"
      data = (id, match_type)
      execute_query(curr, query, data)

def add_match_names(curr, unique_ids):
   match_names = pd.read_csv("all_values/all_matches.csv")
   for index, match_name in match_names["Match Name"].items():
      id = generate_unique_id(unique_ids)
      query = "INSERT INTO match (match_id, match_name) VALUES (%s, %s);"
      data = (id, match_name)
      execute_query(curr, query, data)

def add_maps(curr, unique_ids):
   maps = pd.read_csv("all_values/all_maps.csv")
   for index, map in maps["Map"].items():
      id = generate_unique_id(unique_ids)
      query = "INSERT INTO map (map_id, map_name) VALUES (%s, %s);"
      data = (id, map)
      execute_query(curr, query, data)

def add_teams(curr, unique_ids):
   teams = pd.read_csv("all_values/all_teams.csv")
   for index, team in teams["Team"].items():
      id = generate_unique_id(unique_ids)
      query = "INSERT INTO team (team_id, team_name) VALUES (%s, %s);"
      data = (id, team)
      execute_query(curr, query, data)

def add_players(curr, unique_ids):
   players = pd.read_csv("all_values/all_players.csv")
   for index, player in players["Player"].items():
      id = generate_unique_id(unique_ids)
      query = "INSERT INTO player (player_id, player_name) VALUES (%s, %s);"
      data = (id, player)
      execute_query(curr, query, data)

def add_agents(curr, unique_ids):
   agents = pd.read_csv("all_values/all_agents.csv")
   for index, agent in agents["Agents"].items():
      id = generate_unique_id(unique_ids)
      query = "INSERT INTO agent (agent_id, agent_name) VALUES (%s, %s);"
      data = (id, agent)
      execute_query(curr, query, data)

def add_drafts(curr):
   drafts = pd.read_csv("matches/draft_phase.csv")
   for index, row in drafts.iterrows():
      query = "INSERT INTO draft (tournament_id, stage_id, match_type_id, match_id, team_id, action, map_id) VALUES (%s, %s, %s, %s, %s, %s, %s);"
      tournament = row["Tournament"]
      stage = row["Stage"]
      match_type = row["Match Type"]
      match_name = row["Match Name"]
      team = row["Team"]
      action = row["Action"]
      map = row["Map"]
      tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournament", "tournament_name", tournament)
      stage_id = retrieve_foreign_key(curr, "stage_id", "stage", "stage_name", stage)
      match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_type", "match_type_name", match_type)
      match_name_id = retrieve_foreign_key(curr, "match_id", "match", "match_name", match_name)
      team_id = retrieve_foreign_key(curr, "team_id", "team", "team_name", team)
      map_id = retrieve_foreign_key(curr, "map_id", "map", "map_name", map)
      data = (tournament_id, stage_id, match_type_id, match_name_id, team_id, action, map_id)
      execute_query(curr, query, data)
