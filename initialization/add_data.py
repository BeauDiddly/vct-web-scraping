from process.process_df import reorder_columns
from io import StringIO
from Connect.connect import create_db_url
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


def add_reference_data(df, table_name, engine):
   df.to_sql(table_name, engine, index=False, if_exists = "append")

async def byte_generator(csv_data):
   yield csv_data

async def copy_df_to_db(df, pool, table):
   if len(df.index) != 0:
      async with pool.acquire() as conn:
         buffer = StringIO()
         df.to_csv(buffer, header=False, index=False)
         csv_data = buffer.getvalue().encode('utf-8')
         buffer.close() 
         await conn.copy_to_table(
                table,
                source=byte_generator(csv_data),
                columns=list(df.columns),
                delimiter=',',
                null='',
                encoding='utf-8'
            )

async def add_data_helper(dfs_dict, file_name, pool):
   table_name = file_name.split(".")[0]
   main_df = dfs_dict["main"]
   agents_df = dfs_dict["agents"]
   teams_df = dfs_dict["teams"]
   await copy_df_to_db(main_df, pool, table_name)
   await copy_df_to_db(agents_df, pool, f"{table_name}_agents")
   await copy_df_to_db(teams_df, pool, f"{table_name}_teams")


async def add_data(combined_dfs, pool):
   await asyncio.gather(
      *(add_data_helper(dfs_dict, file_name, pool) for file_name, dfs_dict in combined_dfs.items())
   )