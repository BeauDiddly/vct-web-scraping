from process_df.process_df import *
import pandas as pd
import asyncio
import time

def add_agents(engine):
   all_agents = ["astra", "breach", "brimstone", "chamber", "cypher", "deadlock", "fade", "gekko", "harbor", "iso", "jett", "kayo",
              "killjoy", "neon", "omen", "phoenix", "raze", "reyna", "sage", "skye", "sova", "viper", "yoru"]
   agent_ids = {agent: sum(ord(char) for char in agent) for agent in all_agents}
   df = pd.DataFrame(list(agent_ids.items()), columns=["agent", "agent_id"])
   df = reorder_columns(df, {"agent_id", "agent"})
   df.to_sql("agents", engine, index=False, if_exists="append")
   
def add_maps(engine):
   all_maps = ["Bind", "Haven", "Split", "Ascent", "Icebox", "Breeze", "Fracture", "Pearl", "Lotus", "Sunset", "All Maps"]
   map_ids = {map: sum(ord(char) for char in map) for map in all_maps}
   df = pd.DataFrame(list(map_ids.items()), columns=["map", "map_id"])
   df = reorder_columns(df, {"map_id", "map"})
   df.to_sql("maps", engine, index=False, if_exists="append")

def add_tournaments(df, engine):
   df.to_sql("tournaments", engine, index=False, if_exists = "append")
    
def add_stages(df, engine):
   df.to_sql("stages", engine, index=False, if_exists="append")

def add_match_types(df, engine):
   df.to_sql("match_types", engine, index=False, if_exists="append")

def add_matches(df, engine):
   df.to_sql("matches", engine, index=False, if_exists="append")

def add_teams(df, engine):
   df.to_sql("teams", engine, index=False, if_exists="append")

def add_players(df, engine):
   df.to_sql("players", engine, index=False, if_exists="append")

async def add_drafts(dfs_dict, engine):
   df = dfs_dict["main"]
   print(df)
   df.to_sql("drafts", engine, index=False, if_exists="append")
# async def add_eco_rounds(dfs_dict, engine):
#    df.to_sql("drafts", engine, index=False, if_exists="append")
# async def add_eco_stats(dfs_dict, engine):
#    df.to_sql("drafts", engine, index=False, if_exists="append")
# async def add_kills(dfs_dict, engine):
#    df.to_sql("drafts", engine, index=False, if_exists="append")
# async def add_kills_stats(dfs_dict, engine):
#    df.to_sql("drafts", engine, index=False, if_exists="append")
# async def add_maps_played(dfs_dict, engine):
#    df.to_sql("drafts", engine, index=False, if_exists="append")
# async def add_maps_scores(dfs_dict, engine):
#    df.to_sql("drafts", engine, index=False, if_exists="append")
# async def add_overview(dfs_dict, engine):
#    df.to_sql("drafts", engine, index=False, if_exists="append")
# async def add_rounds_kills(dfs_dict, engine):
#    df.to_sql("drafts", engine, index=False, if_exists="append")
# async def add_scores(dfs_dict, engine):
#    df.to_sql("drafts", engine, index=False, if_exists="append")
# async def add_win_loss_methods_count(dfs_dict, engine):
#    df.to_sql("drafts", engine, index=False, if_exists="append")
# async def add_win_loss_methods_round_number(dfs_dict, engine): 
#    df.to_sql("drafts", engine, index=False, if_exists="append")
# async def add_agents_pick_rates(dfs_dict, engine):
#    df.to_sql("drafts", engine, index=False, if_exists="append")
# async def add_maps_stats(dfs_dict, engine):
#    df.to_sql("drafts", engine, index=False, if_exists="append")
# async def add_teams_picked_agents(dfs_dict, engine):
#    df.to_sql("drafts", engine, index=False, if_exists="append")
# async def add_players_stats(dfs_dict, engine):
#    df.to_sql("drafts", engine, index=False, if_exists="append")

async def add_data_helper(dfs_dict, file_name, engine):
   match file_name:
      case "draft_phase.csv":
         await add_drafts(dfs_dict, engine)
      # case "eco_rounds.csv":
      #    await add_eco_rounds(dfs_dict, engine)
      # case "eco_stats.csv": 
      #    await add_eco_stats(dfs_dict, engine)
      # case "kills.csv":
      #    await add_kills(dfs_dict, engine)
      # case "kills_stats.csv":
      #    await add_kills_stats(dfs_dict, engine)
      # case "maps_played.csv":
      #    await add_maps_played(dfs_dict, engine)
      # case "maps_scores.csv":
      #    await add_maps_scores(dfs_dict, engine)
      # case "overview.csv":
      #    await add_overview(dfs_dict, engine)
      # case "rounds_kills.csv":
      #    await add_rounds_kills(dfs_dict, engine)
      # case "scores.csv":
      #    await add_scores(dfs_dict, engine)
      # case "win_loss_methods_count.csv":
      #    await add_win_loss_methods_count(dfs_dict, engine)
      # case "win_loss_methods_round_number.csv":
      #    await add_win_loss_methods_round_number(dfs_dict, engine)
      # case "agents_pick_rates.csv":
      #    await add_agents_pick_rates(dfs_dict, engine)
      # case "maps_stats.csv":
      #    await add_maps_stats(dfs_dict, engine)
      # case "teams_picked_agents.csv":
      #    await add_teams_picked_agents(dfs_dict, engine)
      # case "players_stats.csv":
      #    await add_players_stats(dfs_dict, engine)


async def add_data(combined_dfs, engine):
   asyncio.gather(
      *(add_data_helper(dfs_dict, file_name, engine) for file_name, dfs_dict in combined_dfs.items())
   )