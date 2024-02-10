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
    for year in years:
        agents_df = pd.read_csv(f"vct_{year}/all_values/all_agents.csv")
        maps_df = pd.read_csv(f"vct_{year}/all_values/all_maps.csv")
        matches_df = pd.read_csv(f"vct_{year}/all_values/all_matches.csv")
        match_types_df = pd.read_csv(f"vct_{year}/all_values/all_match_types.csv")
        players_df = pd.read_csv(f"vct_{year}/all_values/all_players.csv")
        stages_df = pd.read_csv(f"vct_{year}/all_values/all_stages.csv")
        teams_df = pd.read_csv(f"vct_{year}/all_values/all_teams.csv")
        tournaments_df = pd.read_csv(f"vct_{year}/all_values/all_tournaments.csv")
        all_agents = set(agents_df['Agents'])
        all_maps = set(maps_df['Map'])
        all_matches = set(matches_df['Match Name'])
        all_match_types = set(match_types_df['Match Type'])
        all_players = set(players_df['Player'])
        all_stages = set(stages_df['Stage'])
        all_teams = set(teams_df['Team'])
        all_tournaments = set(tournaments_df['Tournament'])
        agents = agents | all_agents
        maps = maps | all_maps
        matches = matches | all_matches
        match_types = match_types | all_match_types
        players = players | all_players
        stages = stages | all_stages
        teams = teams | all_teams
        tournaments = tournaments | all_tournaments


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

if __name__ == "__main__":
    main()