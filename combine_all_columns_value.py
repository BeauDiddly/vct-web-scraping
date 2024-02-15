import pandas as pd

def main():
    years = ["2021", "2022", "2023"]
    tournaments = set()
    stages = set()
    match_types = set()
    maps = set()
    matches = set()
    agents = set()
    players = set()
    teams = set()
    ids_dfs = {}
    reference_ids_dfs = {}
    ids_df = pd.DataFrame()
    reference_ids_df = pd.DataFrame()
    for year in years:
        ids_dfs[year] = pd.DataFrame()
        reference_ids_dfs[year] = pd.DataFrame()

    for year in years:
        agents_df = pd.read_csv(f"vct_{year}/all_values/all_agents.csv")
        maps_df = pd.read_csv(f"vct_{year}/all_values/all_maps.csv")
        matches_df = pd.read_csv(f"vct_{year}/all_values/all_matches.csv")
        match_types_df = pd.read_csv(f"vct_{year}/all_values/all_match_types.csv")
        players_df = pd.read_csv(f"vct_{year}/all_values/all_players.csv")
        stages_df = pd.read_csv(f"vct_{year}/all_values/all_stages.csv")
        teams_df = pd.read_csv(f"vct_{year}/all_values/all_teams.csv")
        tournaments_df = pd.read_csv(f"vct_{year}/all_values/all_tournaments.csv")
        tournaments_stages_matches_ids_df = pd.read_csv(f"vct_{year}/matches/tournaments_stages_matches_ids.csv")
        tournaments_stages_matches_types_ids_df = pd.read_csv(f"vct_{year}/agents/tournaments_stages_match_types_ids.csv")
        all_agents = set(agents_df['Agents'])
        all_maps = set(maps_df['Map'])
        all_matches = set(matches_df['Match Name'])
        all_match_types = set(match_types_df['Match Type'])
        all_players = set(players_df['Player'])
        all_stages = set(stages_df['Stage'])
        all_teams = set(teams_df['Team'])
        all_tournaments = set(tournaments_df['Tournament'])
        tournaments_stages_matches_ids_df["Stage ID"] = tournaments_stages_matches_ids_df['Stage ID'].astype(str)
        tournaments_stages_matches_types_ids_df["Stage ID"] = tournaments_stages_matches_types_ids_df['Stage ID'].astype(str)
        merged_df = pd.merge(tournaments_stages_matches_ids_df, tournaments_stages_matches_types_ids_df[["Tournament", "Stage", "Match Type", "Match Type ID"]],
                             on=["Tournament", "Stage", "Match Type"], how="left")
        merged_df["Year"] = year
        ids_dfs[year] = pd.concat([ids_dfs[year], merged_df], ignore_index=True)
        match_type_index = ids_dfs[year].columns.get_loc("Match Type")
        match_type_ids = ids_dfs[year].pop("Match Type ID")
        ids_dfs[year].insert(match_type_index + 1, "Match Type ID", match_type_ids)
        reference_ids_dfs[year] = pd.concat([reference_ids_dfs[year], tournaments_stages_matches_types_ids_df], ignore_index=True)
        reference_ids_dfs[year]["Year"] = year
        agents = agents | all_agents
        maps = maps | all_maps
        matches = matches | all_matches
        match_types = match_types | all_match_types
        players = players | all_players
        stages = stages | all_stages
        teams = teams | all_teams
        tournaments = tournaments | all_tournaments


    for year in years:
        ids_df = pd.concat([ids_df, ids_dfs[year]], ignore_index=True)
        reference_ids_df = pd.concat([reference_ids_df, reference_ids_dfs[year]], ignore_index=True)
    # match_type_index = ids_dfs[year].columns.get_loc("Match Type")
    # match_type_ids = ids_dfs.pop("Match Type ID")
    # ids_dfs.insert(match_type_index + 1, "Match Type ID", match_type_ids)

    tournaments_df = pd.DataFrame({'Tournament': list(tournaments)})
    stages_df = pd.DataFrame({'Stage': list(stages)})
    match_types_df = pd.DataFrame({'Match Type': list(match_types)})
    matches_df = pd.DataFrame({'Match Name': list(matches)})
    maps_df = pd.DataFrame({'Map': list(maps)})
    players_df = pd.DataFrame({'Player': list(players)})
    teams_df = pd.DataFrame({'Team': list(teams)})
    agents_df = pd.DataFrame({'Agents': list(agents)})

    tournaments_df.to_csv(f"all_vct/all_tournaments.csv", index=False)
    stages_df.to_csv(f"all_vct/all_stages.csv", index=False)
    match_types_df.to_csv(f"all_vct/all_match_types.csv", index=False)
    matches_df.to_csv(f"all_vct/all_matches.csv", index=False)
    maps_df.to_csv(f"all_vct/all_maps.csv", index=False)
    players_df.to_csv(f"all_vct/all_players.csv", index=False)
    teams_df.to_csv(f"all_vct/all_teams.csv", index=False)
    agents_df.to_csv(f"all_vct/all_agents.csv", index=False)
    ids_df.to_csv(f"all_vct/al_ids.csv")
    reference_ids_df.to_csv("all_vct/all_reference_ids.csv")

if __name__ == "__main__":
    main()