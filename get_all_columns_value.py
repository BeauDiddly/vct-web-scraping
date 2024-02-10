import pandas as pd

def main():
    year = input("Input the VCT year: ")
    df = pd.read_csv(f"vct_{year}/matches/overview.csv")
    df_2 = pd.read_csv(f"vct_{year}/players_stats/players_stats.csv")
    df_3 = pd.read_csv(f"vct_{year}/agents/agents_pick_rates.csv")
    all_tournaments = set(df['Tournament'])
    all_stages = set(df_3['Stage'])
    all_match_types = set(df_3['Match Type'])
    all_maps = set(df_3['Map'])
    all_matches = set(df['Match Name'])
    all_agents = set(df['Agents']) | set(df_2["Agents"]) | set(df_3["Agent"])
    all_players = set(df['Player'])
    all_teams = set(df['Team'])


    tournament_df = pd.DataFrame({'Tournament': list(all_tournaments)})
    stage_df = pd.DataFrame({'Stage': list(all_stages)})
    match_type_df = pd.DataFrame({'Match Type': list(all_match_types)})
    matches_df = pd.DataFrame({'Match Name': list(all_matches)})
    map_df = pd.DataFrame({'Map': list(all_maps)})
    player_df = pd.DataFrame({'Player': list(all_players)})
    team_df = pd.DataFrame({'Team': list(all_teams)})
    agents_df = pd.DataFrame({'Agents': list(all_agents)})

    tournament_df.to_csv(f"vct_{year}/all_values/all_tournaments.csv", index=False)
    stage_df.to_csv(f"vct_{year}/all_values/all_stages.csv", index=False)
    match_type_df.to_csv(f"vct_{year}/all_values/all_match_types.csv", index=False)
    matches_df.to_csv(f"vct_{year}/all_values/all_matches.csv", index=False)
    map_df.to_csv(f"vct_{year}/all_values/all_maps.csv", index=False)
    player_df.to_csv(f"vct_{year}/all_values/all_players.csv", index=False)
    team_df.to_csv(f"vct_{year}/all_values/all_teams.csv", index=False)
    agents_df.to_csv(f"vct_{year}/all_values/all_agents.csv", index=False)

if __name__ == "__main__":
    main()