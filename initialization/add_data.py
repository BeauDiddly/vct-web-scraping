from Connect.execute_query import execute_query
import pandas as pd
from retrieve.retrieve import retrieve_foreign_key
from generate.generate_unique_id import generate_unique_id

def add_tournaments(curr, unique_ids):
   tournaments = pd.read_csv("all_values/all_tournaments.csv")
   for index, tournament in tournaments["Tournament"].items():
      id = generate_unique_id(unique_ids)
      query = "INSERT INTO tournaments (tournament_id, tournament_name) VALUES (%s, %s);"
      data = (id, tournament)
      execute_query(curr, query, data)
    
def add_stages(curr, unique_ids):
   stages = pd.read_csv("all_values/all_stages.csv")
   for index, stage in stages["Stage"].items():
      id = generate_unique_id(unique_ids)
      query = "INSERT INTO stages (stage_id, stage_name) VALUES (%s, %s);"
      data = (id, stage)
      execute_query(curr, query, data)

def add_match_types(curr, unique_ids):
   match_types = pd.read_csv("all_values/all_match_types.csv")
   for index, match_type in match_types["Match Type"].items():
      id = generate_unique_id(unique_ids)
      query = "INSERT INTO match_types (match_type_id, match_type_name) VALUES (%s, %s);"
      data = (id, match_type)
      execute_query(curr, query, data)

def add_match_names(curr, unique_ids):
   match_names = pd.read_csv("all_values/all_matches.csv")
   for index, match_name in match_names["Match Name"].items():
      id = generate_unique_id(unique_ids)
      query = "INSERT INTO matches (match_id, match_name) VALUES (%s, %s);"
      data = (id, match_name)
      execute_query(curr, query, data)

def add_maps(curr, unique_ids):
   maps = pd.read_csv("all_values/all_maps.csv")
   for index, map in maps["Map"].items():
      id = generate_unique_id(unique_ids)
      query = "INSERT INTO maps (map_id, map_name) VALUES (%s, %s);"
      data = (id, map)
      execute_query(curr, query, data)

def add_teams(curr, unique_ids):
   teams = pd.read_csv("all_values/all_teams.csv")
   for index, team in teams["Team"].items():
      id = generate_unique_id(unique_ids)
      query = "INSERT INTO teams (team_id, team_name) VALUES (%s, %s);"
      data = (id, team)
      execute_query(curr, query, data)

def add_players(curr, unique_ids):
   players = pd.read_csv("all_values/all_players.csv")
   for index, player in players["Player"].items():
      id = generate_unique_id(unique_ids)
      query = "INSERT INTO players (player_id, player_name) VALUES (%s, %s);"
      data = (id, player)
      execute_query(curr, query, data)

def add_agents(curr, unique_ids):
   agents = pd.read_csv("all_values/all_agents.csv")
   for index, agent in agents["Agents"].items():
      id = generate_unique_id(unique_ids)
      query = "INSERT INTO agents (agent_id, agent_name) VALUES (%s, %s);"
      data = (id, agent)
      execute_query(curr, query, data)

def add_drafts(curr):
   drafts = pd.read_csv("matches/draft_phase.csv")
   query = """
      INSERT INTO drafts (
         tournament_id, stage_id, match_type_id, match_id,
         team_id, action, map_id
      ) VALUES (
         %s, %s, %s, %s, %s, %s, %s
      );
   """
   for index, row in drafts.iterrows():
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
      match_id = retrieve_foreign_key(curr, "match_id", "match", "match_name", match_name)
      team_id = retrieve_foreign_key(curr, "team_id", "team", "team_name", team)
      map_id = retrieve_foreign_key(curr, "map_id", "map", "map_name", map)
      data = (tournament_id, stage_id, match_type_id, match_id, team_id, action, map_id)
      execute_query(curr, query, data)

def add_eco_rounds(curr):
   eco_rounds = pd.read_csv("matches/eco_rounds.csv")
   query = """
      INSERT INTO eco_rounds (
         tournament_id, stage_id, match_type_id, match_id, map_id,
         round_number, team_id, credits, eco_type, outcome
      ) VALUES (
         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
      );
   """
   for index, row in eco_rounds.iterrows():
      tournament = row["Tournament"]
      stage = row["Stage"]
      match_type = row["Match Type"]
      match_name = row["Match Name"]
      map = row["Map"]
      round_number = row["Round Number"]
      team = row["Team"]
      credits = row["Credits"]
      eco_type = row["Type"]
      outcome = row["Outcome"]
      tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournament", "tournament_name", tournament)
      stage_id = retrieve_foreign_key(curr, "stage_id", "stage", "stage_name", stage)
      match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_type", "match_type_name", match_type)
      match_id = retrieve_foreign_key(curr, "match_id", "match", "match_name", match_name)
      team_id = retrieve_foreign_key(curr, "team_id", "team", "team_name", team)
      map_id = retrieve_foreign_key(curr, "map_id", "map", "map_name", map)
      data = (tournament_id, stage_id, match_type_id, match_id, map_id, round_number, team_id, credits, eco_type, outcome)
      execute_query(curr, query, data)
      
      

def add_eco_stats(curr):
   eco_stats = pd.read_csv("matches/eco_stats.csv")
   query = """
      INSERT INTO eco_stats (
         tournament_id, stage_id, match_type_id, match_id, map_id,
         team_id, eco_type, initiated, won
      ) VALUES (
         %s, %s, %s, %s, %s, %s, %s, %s
      );
   """
   for index, row in eco_stats.iterrows():
      tournament = row["Tournament"]
      stage = row["Stage"]
      match_type = row["Match Type"]
      match_name = row["Match Name"]
      map = row["Map"]
      team = row["Team"]
      eco_type = row["Type"]
      initiated = row["Initiated"]
      won = row["Won"]
      tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournament", "tournament_name", tournament)
      stage_id = retrieve_foreign_key(curr, "stage_id", "stage", "stage_name", stage)
      match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_type", "match_type_name", match_type)
      match_id = retrieve_foreign_key(curr, "match_id", "match", "match_name", match_name)
      team_id = retrieve_foreign_key(curr, "team_id", "team", "team_name", team)
      map_id = retrieve_foreign_key(curr, "map_id", "map", "map_name", map)
      data = (tournament_id, stage_id, match_type_id, match_id, map_id, team_id, eco_type, initiated, won)
      execute_query(curr, query, data)
      

def add_kills(curr):
   kills = pd.read_csv("matches/kills.csv")
   query = """
      INSERT INTO kills (
         tournament_id, stage_id, match_type_id, match_id, map_id, 
         player_team_id, player_id,
         enemy_team_id, enemy_id,
         player_kills, enemy_kills, difference, kill_type
      ) VALUES (
         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
      );
   """
   for index, row in kills.iterrows():
      tournament = row["Tournament"]
      stage = row["Stage"]
      match_type = row["Match Type"]
      match_name = row["Match Name"]
      map = row["Map"]
      player_team = row["Player's Team"]
      player = row["Player"]
      enemy_team = row["Enemy's Team"]
      enemy = row["Enemy"]
      player_kills = row["Player's Kills"]
      enemy_kills = row["Enemy's Kills"]
      difference = row["Difference"]
      kill_type = row["Kill Type"]
      tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournament", "tournament_name", tournament)
      stage_id = retrieve_foreign_key(curr, "stage_id", "stage", "stage_name", stage)
      match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_type", "match_type_name", match_type)
      match_id = retrieve_foreign_key(curr, "match_id", "match", "match_name", match_name)
      map_id = retrieve_foreign_key(curr, "map_id", "map", "map_name", map)
      player_team_id = retrieve_foreign_key(curr, "team_id", "team", "team_name", player_team)
      player_id = retrieve_foreign_key(curr, "player_id", "player", "player_name", player)
      enemy_team_id = retrieve_foreign_key(curr, "team_id", "team", "team_name", enemy_team)
      enemy_id = retrieve_foreign_key(curr, "player_id", "player", "player_name", enemy)
      data = (tournament_id, stage_id, match_type_id, match_id, map_id, player_team_id, player_id, enemy_team_id,
            enemy_id, player_kills, enemy_kills, difference, kill_type)
      execute_query(curr, query, data)


def add_kills_stats(curr):
   kills_stats = pd.read_csv("matches/kills_stats.csv")
   query = """
      INSERT INTO kills_stats (
         tournament_id, stage_id, match_type_id, match_id, map_id, team_id, player_id, agent_id,
         two_kills, three_kills, four_kills, one_vs_one, one_vs_two, one_vs_three, one_vs_four, one_vs_five, econ, spike_plants, spike_defuse
      )
      VALUES (
         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
      );
   """
   for index, row in kills_stats.iterrows():
      tournament = row["Tournament"]
      stage = row["Stage"]
      match_type = row["Match Type"]
      match_name = row["Match Name"]
      map = row["Map"]
      team = row["Team"]
      player = row["Player"]
      agent = row["Agent"]
      two_kills = row["2K"]
      three_kills = row["3K"]
      four_kills = row["4K"]
      five_kills = row["5K"]
      one_vs_one = row["1v1"]
      one_vs_two = row["1v2"]
      one_vs_three = row["1v3"]
      one_vs_four = row["1v4"]
      one_vs_five = row["1v5"]
      econ = row["Econ"]
      spike_plants = row["Spike Plants"]
      spike_defuse = row["Spike Defuse"]
      tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournament", "tournament_name", tournament)
      stage_id = retrieve_foreign_key(curr, "stage_id", "stage", "stage_name", stage)
      match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_type", "match_type_name", match_type)
      match_id = retrieve_foreign_key(curr, "match_id", "match", "match_name", match_name)
      map_id = retrieve_foreign_key(curr, "map_id", "map", "map_name", map)
      team_id = retrieve_foreign_key(curr, "team_id", "team", "team_name", team)
      player_id = retrieve_foreign_key(curr, "player_id", "player", "player_name", player)
      agent_id = retrieve_foreign_key(curr, "agent_id", "agent", "agent_name", agent)
      data = (tournament_id, stage_id, match_type_id, match_id, map_id, team_id, player_id, agent_id,
              two_kills, three_kills, four_kills, five_kills, one_vs_one, one_vs_two, one_vs_three,
              one_vs_four, one_vs_five, econ, spike_plants, spike_defuse)
      execute_query(curr, query, data)

def add_maps_played(curr):
   maps_played = pd.read_csv("matches/maps_played.csv")
   query = """
      INSERT INTO maps_played (
         tournament_id, stage_id, match_type_id, match_id, map_id
      ) VALUES (
         %s, %s, %s, %s, %s
      )
   """
   for index, row in maps_played.iterrows():
      tournament = row["Tournament"]
      stage = row["Stage"]
      match_type = row["Match Type"]
      match_name = row["Match Name"]
      map = row["Map"]
      tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournament", "tournament_name", tournament)
      stage_id = retrieve_foreign_key(curr, "stage_id", "stage", "stage_name", stage)
      match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_type", "match_type_name", match_type)
      match_id = retrieve_foreign_key(curr, "match_id", "match", "match_name", match_name)
      map_id = retrieve_foreign_key(curr, "map_id", "map", "map_name", map)
      data = (tournament_id, stage_id, match_type_id, match_id, map_id)
      execute_query(curr, query, data)

def add_maps_scores(curr):
   maps_scores = pd.read_csv("matches/maps_scores.csv")
   query = """
      INSERT INTO maps_scores (
         tournament_id, stage_id, match_type_id, match_id, map_id, 
         team_a_id, team_a_score, team_a_attack_score, team_a_defender_score, team_a_overtime_score,
         team_b_id, team_b_score, team_b_attack_score, team_b_defender_score, team_b_overtime_score,
         duration
      );
   """
   for index, row in maps_scores.iterrows():
      tournament = row["Tournament"]
      stage = row["Stage"]
      match_type = row["Match Type"]
      match_name = row["Match Name"]
      map = row["Map"]
      team_a = row["Team A"]
      team_a_score = row["Team A's Score"]
      team_a_attack_score = row["Team A's Attack Score"]
      team_a_defender_score = row["Team A's Defender Score"]
      team_a_overtime_score = row["Team A's Overtime Score"]
      team_b = row["Team B"]
      team_b_score = row["Team B's Score"]
      team_b_attack_score = row["Team B's Attack Score"]
      team_b_defender_score = row["Team B's Defender Score"]
      team_b_overtime_score = row["Team B's Overtime Score"]
      duration = row["Duration"]
      tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournament", "tournament_name", tournament)
      stage_id = retrieve_foreign_key(curr, "stage_id", "stage", "stage_name", stage)
      match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_type", "match_type_name", match_type)
      match_id = retrieve_foreign_key(curr, "match_id", "match", "match_name", match_name)
      map_id = retrieve_foreign_key(curr, "map_id", "map", "map_name", map)
      team_a_id = retrieve_foreign_key(curr, "team_id", "team", "team_name", team_a)
      team_b_id = retrieve_foreign_key(curr, "team_id", "team", "team_name", team_b)
      data = (tournament_id, stage_id, match_type_id, match_id, map_id,
              team_a_id, team_a_score, team_a_attack_score, team_a_defender_score, team_a_overtime_score,
              team_b_id, team_b_score, team_b_attack_score, team_b_defender_score, team_b_overtime_score,
              duration)
      execute_query(curr, query, data)


def add_overview(curr):
   overview = pd.read_csv("matches/overview.csv")
   query = """
      INSERT INTO overview (
         tournament_id, stage_id, match_type_id, match_id, map_id, player_id, team_id, agent_id,
         rating, average_combat_score, kills, deaths, assists, kill_deaths, kast, adr, headshot_percentage, first_kills, first_deaths, fkd, side
      )
      VALUES (
         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
      );
   """
   for index, row in overview.iterrows():
      tournament = row["Tournament"]
      stage = row["Stage"]
      match_type = row["Match Type"]
      match_name = row["Match Name"]
      map = row["Map"]
      team = row["Team"]
      player = row["Player"]
      agent = row["Agent"]
      rating = row["Rating"]
      average_combat_score = row["Average Combat Score"]
      kills = row["Kills"]
      deaths = row["Deaths"]
      assists = row["Assists"]
      kill_deaths = row["Kill - Deaths (KD)"]
      kast = float(row["Kill, Assist, Trade, Survive %"].strip("%")) / 100.0
      adr = row["Average Damage per Round"]
      headshot_percentage = float(row["Headshot %"].strip("%")) / 100.0
      first_kills = row["First Kills"]
      first_deaths = row["First Deaths"]
      fkd = row["Kills - Deaths (FKD)"]
      side = row["side"]
      tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournament", "tournament_name", tournament)
      stage_id = retrieve_foreign_key(curr, "stage_id", "stage", "stage_name", stage)
      match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_type", "match_type_name", match_type)
      match_id = retrieve_foreign_key(curr, "match_id", "match", "match_name", match_name)
      map_id = retrieve_foreign_key(curr, "map_id", "map", "map_name", map)
      team_id = retrieve_foreign_key(curr, "team_id", "team", "team_name", team)
      player_id = retrieve_foreign_key(curr, "player_id", "player", "player_name", player)
      agent_id = retrieve_foreign_key(curr, "agent_id", "agent", "agent_name", agent)
      data = (tournament_id, stage_id, match_type_id, match_id, map_id, player_id, team_id, agent_id,
              rating, average_combat_score, kills, deaths, assists, kill_deaths, kast, adr, headshot_percentage, first_kills, first_deaths, fkd, side)
      execute_query(curr, query, data)


def add_rounds_kills(curr):
   rounds_kills = pd.read_csv("matches/rounds_kills.csv")
   query = """
      INSERT INTO rounds_kills (
         tournament_id, stage_id, match_type_id, match_id, map_id,round_number,
         eliminator_team_id, eliminator_id, eliminator_agent_id,
         eliminated_team_id, eliminated_id, eliminated_agent_id,
         kill_type
      );
   """
   for index, row in rounds_kills.iterrows():
      tournament = row["Tournament"]
      stage = row["Stage"]
      match_type = row["Match Type"]
      match_name = row["Match Name"]
      map = row["Map"]
      round_number = row["Round Number"]
      eliminator_team = row["Eliminator's Team"]
      eliminator = row["Eliminator"]
      eliminator_agent = row["Eliminator's Agent"]
      eliminated_team = row["Eliminated's Team"]
      eliminated = row["Eliminated"]
      eliminated_agent = row["Eliminated's Team"]
      kill_type = row["Kill Type"]
      tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournament", "tournament_name", tournament)
      stage_id = retrieve_foreign_key(curr, "stage_id", "stage", "stage_name", stage)
      match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_type", "match_type_name", match_type)
      match_id = retrieve_foreign_key(curr, "match_id", "match", "match_name", match_name)
      map_id = retrieve_foreign_key(curr, "map_id", "map", "map_name", map)
      eliminator_team_id = retrieve_foreign_key(curr, "team_id", "team", "team_name", eliminator_team)
      eliminator_id = retrieve_foreign_key(curr, "player_id", "player", "player_name", eliminator)
      eliminator_agent_id = retrieve_foreign_key(curr, "agent_id", "agent", "agent_name", eliminator_agent)
      eliminated_team_id = retrieve_foreign_key(curr, "team_id", "team", "team_name", eliminated_team)
      eliminated_id = retrieve_foreign_key(curr, "player_id", "player", "player_name", eliminated)
      eliminated_agent_id = retrieve_foreign_key(curr, "agent_id", "agent", "agent_name", eliminated_agent)
      data = (tournament_id, stage_id, match_type_id, match_id, map_id, round_number,
              eliminator_team_id, eliminator_id, eliminator_agent_id,
              eliminated_team_id, eliminated_id, eliminated_agent_id,
              kill_type)
      execute_query(curr, query, data)

def add_scores(curr):
   scores = pd.read_csv("matches/scores.csv")
   query = """
      INSERT INTO scores (
         tournament_id, stage_id, match_type_id, match_id,
         winner_id, loser_id,
         winner_score, loser_score
      );
   """
   for index, row in scores.iterrows():
      tournament = row["Tournament"]
      stage = row["Stage"]
      match_type = row["Match Type"]
      match_name = row["Match Name"]
      winner = row["Winner"]
      loser = row["Loser"]
      winner_score = row["Winner's Score"]
      loser_score = row["Loser's Score"]
      tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournament", "tournament_name", tournament)
      stage_id = retrieve_foreign_key(curr, "stage_id", "stage", "stage_name", stage)
      match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_type", "match_type_name", match_type)
      match_id = retrieve_foreign_key(curr, "match_id", "match", "match_name", match_name)
      winner_id = retrieve_foreign_key(curr, "team_id", "team", "team_name", winner)
      loser_id = retrieve_foreign_key(curr, "team_id", "team", "team_name", loser)
      data = (tournament_id, stage_id, match_type_id, match_id,
              winner_id, loser_id,
              winner_score, loser_score)
      execute_query(curr, query, data)
   

def add_agents_pick_rates(curr):
   pick_rates = pd.read_csv("agents/agents_pick_rates.csv")
   query = """
      INSERT INTO agents_pick_rates (
         tournament_id, stage_id, match_type_id, map_id,
         agent_id, pick_rate
      );
   """
   for index, row in pick_rates.iterrows():
      tournament = row["Tournament"]
      stage = row["Stage"]
      match_type = row["Match Type"]
      map = row["Map"]
      agent = row["Agent"]
      pick_rate = float(row["Pick Rate"]) / 100.0
      tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournament", "tournament_name", tournament)
      stage_id = retrieve_foreign_key(curr, "stage_id", "stage", "stage_name", stage)
      match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_type", "match_type_name", match_type)
      map_id = retrieve_foreign_key(curr, "map_id", "map", "map_name", map)
      agent_id = retrieve_foreign_key(curr, "agent_id", "agent", "agent_name", agent)
      data = (tournament_id, stage_id, match_type_id, map_id, agent_id, pick_rate)
      execute_query(curr, query, data)

def add_maps_stats(curr):
   maps_stats = pd.read_csv("agents/maps_stats.csv")
   query = """
      INSERT INTO maps_stats (
         tournament_id, stage_id, match_type_id, map_id, 
         total_maps_played, attacker_win_percentage, defender_win_percentage
      );
   """
   for index, row in maps_stats.iterrows():
      tournament = row["Tournament"]
      stage = row["Stage"]
      match_type = row["Match Type"]
      map = row["Map"]
      total_maps_played = row["Total Maps Played"]
      attacker_win_percentage = float(row["Attacker Side Win Percentage"]) / 100.0
      defender_win_percentage = float(row["Defender Side Win Percentage"]) / 100.0
      tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournament", "tournament_name", tournament)
      stage_id = retrieve_foreign_key(curr, "stage_id", "stage", "stage_name", stage)
      match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_type", "match_type_name", match_type)
      map_id = retrieve_foreign_key(curr, "map_id", "map", "map_name", map)

      data = (tournament_id, stage_id, match_type_id, map_id, total_maps_played, attacker_win_percentage, defender_win_percentage)
      execute_query(curr, query, data)

def add_teams_picked_agents(curr):
   teams_picked_agents = pd.read_csv("agents/teams_picked_agents.csv")
   query = """
      INSERT INTO teams_picked_agents (
         tournament_id, stage_id, match_type_id, map_id, team_id, 
         agent_id, total_wins_by_maps, total_loss_by_maps, total_maps_played
      );
   """
   for index, row in teams_picked_agents.iterrows():
      tournament = row["Tournament"]
      stage = row["Stage"]
      match_type = row["Match Type"]
      map = row["Map"]
      team = row["Team"]
      agent = row["Agent"]
      total_wins_by_map = row["Total Wins By Map"]
      total_loss_by_map = row["Total Loss By Map"]
      total_maps_played = row["Total Maps Played"]
      tournament_id = retrieve_foreign_key(curr, "tournament_id", "tournament", "tournament_name", tournament)
      stage_id = retrieve_foreign_key(curr, "stage_id", "stage", "stage_name", stage)
      match_type_id = retrieve_foreign_key(curr, "match_type_id", "match_type", "match_type_name", match_type)
      map_id = retrieve_foreign_key(curr, "map_id", "map", "map_name", map)
      team_id = retrieve_foreign_key(curr, "team_id", "team", "team_name", team)
      agent_id = retrieve_foreign_key(curr, "agent_id", "agent", "agent_name", agent)

      data = (tournament_id, stage_id, match_type_id, map_id, team_id, agent_id, total_wins_by_map, total_loss_by_map, total_maps_played)

      execute_query(curr, query, data)
      