import os
from Connect.connect import connect, engine
from initialization.create_tables import create_all_tables
import asyncio
from initialization.add_data import *
from find_csv_files.find_csv_files import find_csv_files
from Connect.config import config

async def main():
    start_time = time.time()
    now = datetime.now()
    years = [2021, 2022, 2023, 2024]
    conn, curr = connect()
    sql_alchemy_engine = engine()
    db_conn_info = config()

    csv_files = [find_csv_files(f"{os.getcwd()}/vct_{year}/agents", "agents", year) for year in years]
    print(csv_files)

    for i, year in enumerate(years):
        await process_year(year, csv_files[i], sql_alchemy_engine)

    # await add_agents_pick_rates(csv_files[2][2], years[2], sql_alchemy_engine)
    # await add_maps_stats(csv_files[2][0], years[2], sql_alchemy_engine)
    # await add_teams_picked_agents(csv_files[2][1], years[2], sql_alchemy_engine)


    end_time = time.time()
    elasped_time = end_time - start_time
    hours, remainder = divmod(elasped_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    print(f"Time: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")
    # rounds_kills = pd.read_csv("matches/rounds_kills.csv")

    # conn.commit()
    # curr.close()
    # conn.close()

if __name__ == '__main__':
    asyncio.run(main())