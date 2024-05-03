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
    years = {2021, 2022, 2023, 2024}
    conn, curr = connect()
    sql_alchemy_engine = engine()
    db_conn_info = config()

    csv_files = [find_csv_files(f"{os.getcwd()}/vct_{year}/matches", "matches", year) for year in years]
    print(csv_files)

    dfs = {}
    csv_files_w_years = {year: csv_files[i] for i, year in enumerate(years)}
    for path_list in csv_files:
        for file_path in path_list:
            file_name = file_path.split("/")[-1]
            dfs[file_name] = {"agents": [], "teams": [], "main": []}


    # for i, year in enumerate(years):
    #     await process_csv_files(csv_files[i], year, dfs)
    await process_years(csv_files_w_years, dfs)

    # print(dfs["draft_phase.csv"].sample(n=20))


    # await add_drafts(csv_files[2][8], years[2], sql_alchemy_engine)
    # await add_eco_rounds(csv_files[2][11], years[2], sql_alchemy_engine)
    # await add_eco_stats(csv_files[2][3], years[2], sql_alchemy_engine)
    # await add_kills(csv_files[2][1], years[2], sql_alchemy_engine)
    # await add_kills_stats(csv_files[2][5], years[2], sql_alchemy_engine)
    # await add_maps_played(csv_files[2][0], years[2], sql_alchemy_engine)
    # await add_maps_scores(csv_files[2][12], years[2], sql_alchemy_engine)
    # await add_overview(csv_files[2][9], years[2], sql_alchemy_engine)
    # await add_rounds_kills(csv_files[1][10], years[1], sql_alchemy_engine)
    # await add_scores(csv_files[1][4], years[1], sql_alchemy_engine)
    # await add_win_loss_methods_count(csv_files[2][6], years[2], sql_alchemy_engine)
    # await add_win_loss_methods_round_number(csv_files[2][2], years[2], sql_alchemy_engine)

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