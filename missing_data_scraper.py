from bs4 import BeautifulSoup
import requests
from WebScraper.web_data_extraction import extract_games_id, extract_methods, extract_maps_headers, extract_overview_stats
import pandas as pd
from data_clean.data_clean import *

def main():
    
    url = "https://www.vlr.gg/12398/typhone-vs-besiktas-esports-champions-tour-turkey-stage-1-challengers-3-ro32"

    team_mapping = {"TP": "Typhone", "BJK": "Beşiktaş Esports"}

    team_ids = {"Typhone": 3464, "Beşiktaş Esports": 2036}

    tournament = {"Champions Tour Turkey Stage 1: Challengers 3": 339}

    stage = {"Open Qualifier": 698}

    match_type = {"Round of 32": 3227}

    match = {"Typhone vs Beşiktaş Esports": 12398}

    results = {"scores": [["Champions Tour Turkey Stage 1: Challengers 3", "Open Qualifier", "Round of 32",
                          "Typhone vs Beşiktaş Esports", "Typhone", "Beşiktaş Esports", 0, 2, "Beşiktaş Esports won"]],
                "maps_played": [],
                "maps_scores": [],
                "win_loss_methods_count": [],
                "win_loss_methods_round_number": [],
                "overview": [],
                "team_mapping": {"TP": "Typhone", "BJK": "Beşiktaş Esports"},
                "teams_ids": {"Typhone": 3464, "Beşiktaş Esports": 2036},
                "players_ids": {},
                "tournaments_stages_matches_games_ids": []}
    dataframes = {}
    
    match_page = requests.get(url)
    match_soup = BeautifulSoup(match_page.content, "html.parser")

    overview_stats = match_soup.find_all("div", class_="vm-stats-game")
    games_id_divs = match_soup.find("div", class_="vm-stats-gamesnav").find_all("div")
    games_id = {}
    extract_games_id(games_id_divs, games_id, results, ["Champions Tour Turkey Stage 1: Challengers 3", "Open Qualifier", "Round of 32", "Typhone vs Beşiktaş Esports",
                                                        339, 689, 12398])
    extract_methods(overview_stats, games_id, results, ["Champions Tour Turkey Stage 1: Challengers 3", "Open Qualifier", "Round of 32", "Typhone vs Beşiktaş Esports",
                                                        "Typhone", "Beşiktaş Esports"])
    maps_headers = match_soup.find_all("div", class_="vm-stats-game-header")
    extract_maps_headers(maps_headers, results, ["Champions Tour Turkey Stage 1: Challengers 3",
                                                  "Open Qualifier",
                                                  "Round of 32", 
                                                  "Typhone vs Beşiktaş Esports",
                                                  "Typhone",
                                                  "Beşiktaş Esports"])

    player_to_team = extract_overview_stats(overview_stats, games_id, team_mapping, results,
                                            ["Champions Tour Turkey Stage 1: Challengers 3",
                                            "Open Qualifier",
                                            "Round of 32", 
                                            "Typhone vs Beşiktaş Esports",
                                            "Typhone",
                                            "Beşiktaş Esports"])


    dataframes["scores"] = pd.DataFrame(results["scores"],
                                        columns=["Tournament", "Stage", "Match Type", "Match Name", "Team A", "Team B", "Team A Score", "Team B Score", "Match Result"])
    dataframes["maps_played"] = pd.DataFrame(results["maps_played"],
                                             columns=["Tournament", "Stage", "Match Type", "Match Name", "Map"])
    dataframes["maps_scores"] = pd.DataFrame(results["maps_scores"],
                                                columns=["Tournament", "Stage", "Match Type", "Match Name", "Map", "Team A", "Team A Score",
                                                        "Team A Attacker Score", "Team A Defender Score", "Team A Overtime Score",
                                                        "Team B", "Team B Score", "Team B Attacker Score", "Team B Defender Score",
                                                        "Team B Overtime Score", "Duration"])
    dataframes["win_loss_methods_count"] = pd.DataFrame(results["win_loss_methods_count"],
                                                   columns=["Tournament", "Stage", "Match Type", "Match Name", "Map", "Team",
                                                           "Elimination", "Detonated", "Defused", "Time Expiry (No Plant)", "Eliminated",
                                                            "Defused Failed", "Detonation Denied", "Time Expiry (Failed to Plant)"])
    dataframes["win_loss_methods_round_number"] = pd.DataFrame(results["win_loss_methods_round_number"],
                                                   columns=["Tournament", "Stage", "Match Type", "Match Name", "Map", "Round Number", "Team",
                                                           "Method", "Outcome"])
    dataframes["overview"] = pd.DataFrame(results["overview"],
                                            columns=["Tournament", "Stage", "Match Type", "Match Name", "Map", "Player", "Team",
                                                    "Agents", "Rating", "Average Combat Score", "Kills", "Deaths",
                                                    "Assists", "Kills - Deaths (KD)", "Kill, Assist, Trade, Survive %",
                                                    "Average Damage per Round", "Headshot %", "First Kills", "First Deaths",
                                                    "Kills - Deaths (FKD)", "Side"])
    dataframes["team_mapping"] = pd.DataFrame(list(results["team_mapping"].items()),
                                              columns=["Abbreviated", "Full Name"])
    dataframes["teams_ids"] = pd.DataFrame(list(results["teams_ids"].items()),
                                           columns=["Team", "Team ID"])
    dataframes["players_ids"] = pd.DataFrame(list(results["players_ids"].items()),
                                           columns=["Player", "Player ID"])
    dataframes["tournaments_stages_matches_games_ids"] = pd.DataFrame(results["tournaments_stages_matches_games_ids"],
                                                            columns=["Tournament", "Tournament ID", "Stage", "Stage ID",
                                                                      "Match Type", "Match Name", "Match ID", "Map", "Game ID"])

    for file_name, dataframe in dataframes.items():
        # dataframe.to_csv(f"vct_{year}/matches/{file_name}.csv", encoding="utf-8", index=False)
        # dataframe.to_csv(f"test/{file_name}.csv", encoding="utf-8", index=False)
        dataframes[file_name] = remove_white_spaces(dataframe)
        dataframes[file_name] = remove_white_spaces_in_between(dataframe)
        dataframes[file_name] = remove_tabs_and_newlines(dataframe)
        dataframes[file_name] = fixed_team_names(dataframe)
        dataframes[file_name] = fixed_player_names(dataframe)
        dataframes[file_name] = convert_to_str(dataframe)

        
    for file_name, dataframe in dataframes.items():
        if file_name == "players_ids" or file_name == "teams_ids":
            original_df = csv_to_df(f"cleaned_data/vct_2021/ids/{file_name}.csv")
            dataframe.reset_index(drop=True, inplace=True)
            original_df.reset_index(drop=True, inplace=True)
            original_df = pd.concat([original_df, dataframe], ignore_index=True)
            original_df = add_missing_player(original_df, 2021)
            original_df = original_df.drop_duplicates()
            original_df = convert_to_int(original_df)
            original_df.reset_index(drop=True, inplace=True)
                    # dataframe.to_csv(f"test/{file_name}.csv", encoding="utf-8", index=False)
            original_df.to_csv(f"cleaned_data/vct_2021/ids/{file_name}.csv", index=False)
        elif file_name == "tournaments_stages_matches_games_ids":
            original_df = csv_to_df(f"cleaned_data/vct_2021/ids/{file_name}.csv")
            first_occurence_index = original_df.index[
                (original_df["Tournament"] == dataframe.loc[0, "Tournament"]) &
                (original_df["Stage"] == dataframe.loc[0, "Stage"]) &
                (original_df["Match Type"] == dataframe.loc[0, "Match Type"])
            ][0]
            original_df = pd.concat([original_df.iloc[:first_occurence_index], dataframe, original_df.iloc[first_occurence_index:]]).reset_index(drop=True)
            original_df = original_df.drop_duplicates()
            original_df = convert_to_int(original_df)
            original_df.reset_index(drop=True, inplace=True)
            original_df.to_csv(f"cleaned_data/vct_2021/ids/{file_name}.csv", index=False)
        elif file_name == "team_mapping":
            original_df = csv_to_df(f"cleaned_data/vct_2021/matches/{file_name}.csv")
            original_df = pd.concat([original_df, dataframe], ignore_index=True)
            original_df = original_df.drop_duplicates(subset=["Abbreviated", "Full Name"])
            original_df.reset_index(drop=True, inplace=True)
            original_df.to_csv(f"cleaned_data/vct_2021/matches/{file_name}.csv", index=False)
        else:
            original_df = csv_to_df(f"cleaned_data/vct_2021/matches/{file_name}.csv")
            first_occurence_index = original_df.index[
                (original_df["Tournament"] == dataframe.loc[0, "Tournament"]) &
                (original_df["Stage"] == dataframe.loc[0, "Stage"]) &
                (original_df["Match Type"] == dataframe.loc[0, "Match Type"])
            ][0]
            original_df = pd.concat([original_df.iloc[:first_occurence_index], dataframe, original_df.iloc[first_occurence_index:]]).reset_index(drop=True)
            original_df = convert_nan_players_teams(original_df)
            original_df.to_csv(f"cleaned_data/vct_2021/matches/{file_name}.csv", index=False)
        # original_df.to_csv(f"test/{file_name}.csv", encoding="utf-8", index=False)



if __name__ == "__main__":
    main()


