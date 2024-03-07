import pandas as pd
import math 
def main():
    years = ["2021", "2022", "2023"]

    agents = set()
    players_dfs = {}
    players = pd.DataFrame()
    teams_dfs = {}
    teams = pd.DataFrame()
    matches_ids_dfs = {}
    tournaments_stages_match_types_ids_dfs = {}
    matches_ids = pd.DataFrame()
    tournaments_stages_match_types_ids = pd.DataFrame()
    teams_mapping = pd.DataFrame()
    teams_mapping_dfs = {}

    for year in years:
        # agents_df = pd.read_csv(f"vct_{year}/all_values/all_agents.csv")

        players_df = pd.read_csv(f"vct_{year}/ids/players_ids.csv")

        teams_df = pd.read_csv(f"vct_{year}/ids/teams_ids.csv")

        team_mapping_df = pd.read_csv(f"vct_{year}/matches/team_mapping.csv")

        teams_mapping_dfs[year] = team_mapping_df

        tournaments_stages_matches_games_ids_df = pd.read_csv(f"vct_{year}/ids/tournaments_stages_matches_games_ids.csv")
        tournaments_stages_match_types_ids_df = pd.read_csv(f"vct_{year}/ids/tournaments_stages_match_types_ids.csv")

        # all_agents = set(agents_df['Agents'])

        teams_df["Team ID"] = teams_df["Team ID"].apply(lambda id: int(id) if not math.isnan(id) else pd.NA)
        players_dfs[year] = players_df
        teams_dfs[year] = teams_df

        tournaments_stages_matches_games_ids_df["Stage ID"] = tournaments_stages_matches_games_ids_df['Stage ID'].astype(str)
        tournaments_stages_match_types_ids_df["Stage ID"] = tournaments_stages_match_types_ids_df['Stage ID'].astype(str)
        tournaments_stages_matches_games_ids_df.loc[:, "Match Type"] = tournaments_stages_matches_games_ids_df["Match Type"].str.strip()
        tournaments_stages_matches_games_ids_df.loc[:, "Match Type"] = tournaments_stages_matches_games_ids_df["Match Type"].str.strip()
        merged_df = pd.merge(tournaments_stages_matches_games_ids_df,
                            tournaments_stages_match_types_ids_df[["Tournament", "Stage", "Match Type", "Match Type ID"]],
                            on=["Tournament", "Stage", "Match Type"],
                            how="left")
        merged_df["Year"] = year

        matches_ids_dfs[year] = merged_df
        match_type_index = matches_ids_dfs[year].columns.get_loc("Match Type")
        match_type_ids = matches_ids_dfs[year].pop("Match Type ID")

        matches_ids_dfs[year].insert(match_type_index + 1, "Match Type ID", match_type_ids)

        tournaments_stages_match_types_ids_df["Year"] = year

        tournaments_stages_match_types_ids_dfs[year] = tournaments_stages_match_types_ids_df

        
        # agents = agents | all_agents

    for year in years:
        matches_ids = pd.concat([matches_ids, matches_ids_dfs[year]], ignore_index=True)
        tournaments_stages_match_types_ids = pd.concat([tournaments_stages_match_types_ids,
                                                        tournaments_stages_match_types_ids_dfs[year]],
                                                        ignore_index=True)
        players = pd.concat([players, players_dfs[year]], ignore_index=True)
        teams = pd.concat([teams, teams_dfs[year]], ignore_index=True)
        teams_mapping = pd.concat([teams_mapping, teams_mapping_dfs[year]], ignore_index=True)

    # agents = pd.DataFrame({'Agents': list(agents)})
    
    players.drop_duplicates(inplace=True)
    teams.drop_duplicates(inplace=True)
    matches_ids.drop_duplicates(inplace=True)
    tournaments_stages_match_types_ids.drop_duplicates(inplace=True)
    teams_mapping.drop_duplicates(inplace=True)

    missing_rows = matches_ids[matches_ids["Match Type ID"].isnull()]
    missing_match_types = missing_rows[["Tournament", "Tournament ID", "Stage", "Stage ID", "Match Type", "Match Type ID", "Year"]].drop_duplicates()

    tournaments_stages_match_types_ids = pd.concat([tournaments_stages_match_types_ids, missing_match_types], ignore_index=True) \
                                        .sort_values(by=["Year", "Tournament", "Tournament ID", "Stage", "Stage ID", "Match Type"])
    
    mask = (tournaments_stages_match_types_ids["Stage ID"] == "all") & (tournaments_stages_match_types_ids["Match Type ID"] == "all")

    tournaments_stages_match_types_ids.loc[mask, ["Stage ID", "Match Type ID"]] = pd.NA

    tournaments_stages_match_types_ids["Stage ID"] = pd.to_numeric(tournaments_stages_match_types_ids["Stage ID"], errors="coerce")
    tournaments_stages_match_types_ids["Match Type ID"] = pd.to_numeric(tournaments_stages_match_types_ids["Match Type ID"], errors="coerce")

    players.to_csv(f"all_vct/all_players_ids.csv", index=False)
    teams.to_csv(f"all_vct/all_teams_ids.csv", index=False)
    # agents.to_csv(f"all_vct/all_agents.csv", index=False)
    matches_ids.to_csv(f"all_vct/all_matches_games_ids.csv", index = False)
    tournaments_stages_match_types_ids.to_csv("all_vct/all_tournaments_stages_match_types_ids.csv", index = False)
    teams_mapping.to_csv("all_vct/all_teams_mapping.csv", index=False)

if __name__ == "__main__":
    main()