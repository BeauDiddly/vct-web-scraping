import pandas as pd

def main():
    years = ["2021", "2022", "2023"]
    # tournaments = set()
    # stages = set()
    # match_types = set()
    # maps = set()
    # matches = set()
    agents = set()
    players_dfs = {}
    players_df = pd.DataFrame()
    teams_dfs = {}
    teams_df = pd.DataFrame()
    matches_ids_dfs = {}
    tournaments_stages_match_types_ids_dfs = {}
    matches_ids_df = pd.DataFrame()
    tournaments_stages_match_types_ids_df = pd.DataFrame()
    # for year in years:
    #     matches_ids_dfs[year] = pd.DataFrame()
    #     tournaments_stages_match_types_ids_dfs[year] = pd.DataFrame()
    #     players_dfs[year] = pd.DataFrame()
    #     teams_dfs[year] = pd.DataFrame()

    for year in years:
        agents_df = pd.read_csv(f"vct_{year}/all_values/all_agents.csv")
        # maps_df = pd.read_csv(f"vct_{year}/all_values/all_maps.csv")
        # matches_df = pd.read_csv(f"vct_{year}/all_values/all_matches.csv")
        # match_types_df = pd.read_csv(f"vct_{year}/all_values/all_match_types.csv")
        players_df = pd.read_csv(f"vct_{year}/matches/players_ids.csv")
        # stages_df = pd.read_csv(f"vct_{year}/all_values/all_stages.csv")
        teams_df = pd.read_csv(f"vct_{year}/matches/teams_ids.csv")
        # tournaments_df = pd.read_csv(f"vct_{year}/all_values/all_tournaments.csv")
        tournaments_stages_matches_ids_df = pd.read_csv(f"vct_{year}/matches/tournaments_stages_matches_ids.csv")
        tournaments_stages_match_types_ids_df = pd.read_csv(f"vct_{year}/agents/tournaments_stages_match_types_ids.csv")

        all_agents = set(agents_df['Agents'])
        # all_maps = set(maps_df['Map'])
        # all_matches = set(matches_df['Match Name'])
        # all_match_types = set(match_types_df['Match Type'])
        # all_players = set(players_df['Player'])
        # all_stages = set(stages_df['Stage'])
        # all_teams = set(teams_df['Team'])
        # all_tournaments = set(tournaments_df['Tournament'])

        # players_dfs[year] = pd.concat([players, players_df], ignore_index=True)
        # teams_dfs[year] = pd.concat([teams, teams_df], ignore_index=True)
        players_dfs[year] = players_df
        teams_dfs[year] = teams_df

        tournaments_stages_matches_ids_df["Stage ID"] = tournaments_stages_matches_ids_df['Stage ID'].astype(str)
        tournaments_stages_match_types_ids_df["Stage ID"] = tournaments_stages_match_types_ids_df['Stage ID'].astype(str)
        merged_df = pd.merge(tournaments_stages_matches_ids_df, tournaments_stages_match_types_ids_df[["Tournament", "Stage", "Match Type", "Match Type ID"]],
                             on=["Tournament", "Stage", "Match Type"], how="left")
        merged_df["Year"] = year

        matches_ids_dfs[year] = merged_df
        match_type_index = matches_ids_dfs[year].columns.get_loc("Match Type")
        match_type_ids = matches_ids_dfs[year].pop("Match Type ID")

        matches_ids_dfs[year].insert(match_type_index + 1, "Match Type ID", match_type_ids)
        tournaments_stages_match_types_ids_df["Year"] = year
        tournaments_stages_match_types_ids_dfs[year] = tournaments_stages_match_types_ids_df
        # tournaments_stages_match_types_ids_dfs[year] = pd.concat([tournaments_stages_match_types_ids_dfs[year], tournaments_stages_matches_types_ids_df], ignore_index=True)
        # tournaments_stages_match_types_ids_dfs[year]["Year"] = year
        
        agents = agents | all_agents
        # maps = maps | all_maps
        # matches = matches | all_matches
        # match_types = match_types | all_match_types
        # players = players | all_players
        # stages = stages | all_stages
        # teams = teams | all_teams
        # tournaments = tournaments | all_tournaments


    for year in years:
        matches_ids_df = pd.concat([matches_ids_df, matches_ids_dfs[year]], ignore_index=True)
        tournaments_stages_match_types_ids_df = pd.concat([tournaments_stages_match_types_ids_df, tournaments_stages_match_types_ids_dfs[year]], ignore_index=True)
        players_df = pd.concat([players_df, players_dfs[year]], ignore_index=True)
        teams_df = pd.concat([teams_df, teams_dfs[year]], ignore_index=True)
    # tournaments_df = pd.DataFrame({'Tournament': list(tournaments)})
    # stages_df = pd.DataFrame({'Stage': list(stages)})
    # match_types_df = pd.DataFrame({'Match Type': list(match_types)})
    # matches_df = pd.DataFrame({'Match Name': list(matches)})
    # maps_df = pd.DataFrame({'Map': list(maps)})
    # players_df = pd.DataFrame({'Player': list(players)})
    # teams_df = pd.DataFrame({'Team': list(teams)})
    agents_df = pd.DataFrame({'Agents': list(agents)})

    # tournaments_df.to_csv(f"all_vct/all_tournaments.csv", index=False)
    # stages_df.to_csv(f"all_vct/all_stages.csv", index=False)
    # match_types_df.to_csv(f"all_vct/all_match_types.csv", index=False)
    # matches_df.to_csv(f"all_vct/all_matches.csv", index=False)
    # maps_df.to_csv(f"all_vct/all_maps.csv", index=False)
    players_df.drop_duplicates(inplace=True)
    teams_df.drop_duplicates(inplace=True)
    matches_ids_df.drop_duplicates(inplace=True)
    tournaments_stages_match_types_ids_df.drop_duplicates(inplace=True)



    players_df.to_csv(f"all_vct/all_players_ids.csv", index=False)
    teams_df.to_csv(f"all_vct/all_teams_ids.csv", index=False)
    agents_df.to_csv(f"all_vct/all_agents.csv", index=False)
    matches_ids_df.to_csv(f"all_vct/all_matches_ids.csv", index = False)
    tournaments_stages_match_types_ids_df.to_csv("all_vct/all_tournaments_stages_match_types_ids.csv", index = False)

if __name__ == "__main__":
    main()