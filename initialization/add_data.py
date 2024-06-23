from process_df.process_df import reorder_columns
import pandas as pd
import asyncio

def add_agents(engine):
   all_agents = ["astra", "breach", "brimstone", "chamber", "clove", "cypher", "deadlock", "fade", "gekko", "harbor", "iso", "jett", "kayo",
              "killjoy", "neon", "omen", "phoenix", "raze", "reyna", "sage", "skye", "sova", "viper", "yoru"]
   agent_ids = {agent: sum(ord(char) for char in agent) for agent in all_agents}
   agent_ids[pd.NA] = 0
   df = pd.DataFrame(list(agent_ids.items()), columns=["agent", "agent_id"])
   df = reorder_columns(df, {"agent_id", "agent"})
   df.to_sql("agents", engine, index=False, if_exists="append")
   
def add_maps(engine):
   all_maps = ["Bind", "Haven", "Split", "Ascent", "Icebox", "Breeze", "Fracture", "Pearl", "Lotus", "Sunset", "All Maps"]
   map_ids = {map: sum(ord(char) for char in map) for map in all_maps}
   map_ids[pd.NA] = 0
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


async def add_data_helper(dfs_dict, file_name, engine):
   table_name = file_name.split(".")[0]
   main_df = dfs_dict["main"]
   agents_df = dfs_dict["agents"]
   teams_df = dfs_dict["teams"]
   main_df.to_sql(table_name, engine, index=False, if_exists="append", chunksize=10000)
   if len(agents_df.index) != 0:
      agents_df.to_sql(f"{table_name}_agents", engine, index=False, if_exists="append", chunksize=10000)
   if len(teams_df.index) != 0:
      teams_df.to_sql(f"{table_name}_teams", engine, index=False, if_exists="append", chunksize=10000)


async def add_data(combined_dfs, engine):
   asyncio.gather(
      *(add_data_helper(dfs_dict, file_name, engine) for file_name, dfs_dict in combined_dfs.items())
   )