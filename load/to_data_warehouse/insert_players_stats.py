from initialization.add_data import *
from find_csv_files.find_csv_files import find_csv_files
from process.process_df import process_years, combine_dfs, process_players_stats_agents, process_players_stats_teams
from process.process_records import create_reference_ids_dict
import time
import asyncio
import os
import asyncpg

async def main():
    start_time = time.time()
    years = [2021, 2022, 2023, 2024]

    csv_files = [find_csv_files(f"{os.getcwd()}/vct_{year}/players_stats", "players_stats", year) for year in years]
    print(csv_files)
    combined_dfs = {}
    dfs = {}
    csv_files_w_years = {year: csv_files[i] for i, year in enumerate(years)}
    for path_list in csv_files:
        for file_path in path_list:
            file_name = file_path.split("/")[-1]
            dfs[file_name] = {"agents": [], "teams": [], "main": []}
            combined_dfs[file_name] = {"agents": pd.DataFrame(), "teams": pd.DataFrame(), "main": pd.DataFrame()}
    reference_ids = {
        "tournaments": {2021: {}, 2022: {}, 2023: {}, 2024: {}},
        "stages": {2021: {}, 2022: {}, 2023: {}, 2024: {}},
        "match_types": {2021: {}, 2022: {}, 2023: {}, 2024: {}},
        "matches": {2021: {}, 2022: {}, 2023: {}, 2024: {}},
        "players": {},
        "teams": {},
        "maps": {},
        "agents": {}}

    db_url = create_db_url()
    async with asyncpg.create_pool(db_url) as pool:
        await asyncio.gather(
            *(create_reference_ids_dict(pool, reference_ids, year) for year in years)
        )

        await process_years(csv_files_w_years, dfs, reference_ids, pool)
        combine_dfs(combined_dfs, dfs)
        tasks = [
            asyncio.create_task(process_players_stats_agents(combined_dfs, combined_dfs["players_stats.csv"]["main"], reference_ids)),
            asyncio.create_task(process_players_stats_teams(combined_dfs, combined_dfs["players_stats.csv"]["main"], reference_ids))
        ]
        await asyncio.gather(*tasks)

        await add_data(combined_dfs, pool)

    end_time = time.time()
    elasped_time = end_time - start_time
    hours, remainder = divmod(elasped_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    print(f"Time: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")


if __name__ == '__main__':
    asyncio.run(main())