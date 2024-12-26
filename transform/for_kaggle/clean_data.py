from utilities.data_clean.data_clean import *
from utilities.aws_s3.s3_utilities import *
import boto3

def main():
    years = [2024]
    s3_client = boto3.client(
        's3'
    )

    bucket_name = "raw-data-vct"

    folder_names = ["matches", "ids", "agents", "players_stats"]

    csv_files_dict = {year: {"matches": [],
                        "ids": [],
                        "agents": [],
                        "players_stats": []} for year in years}



    for year in years:
        for name in folder_names:
            csv_files = list_csv_files_in_folder(s3_client, bucket_name, f"vct_{year}/{name}/")
            csv_files_dict[year][name] = csv_files

    for year in years:
        for csv_file in csv_files_dict[year]["matches"]:
            file_name = csv_file.split("/")[-1]
            df = load_csv_to_dataframe(s3_client, bucket_name, csv_file)
            df = remove_white_spaces(df)
            df = remove_white_spaces_in_between(df)
            df = remove_tabs_and_newlines(df)
            df = remove_forfeited_matches(df)
            df = remove_nan_players_agents(df)
            df = add_missing_abbriev(df, year)
            df = update_player_names(df)
            df = update_team_names(df)
            df = update_match_names(df)
            df = fixed_team_names(df)
            df = fixed_player_names(df)
            df = fixed_match_names(df)
            df = convert_nan_players_teams(df)
            if file_name == "kills_stats.csv":
                df = get_all_agents_played_for_kills_stats(df)
            elif file_name == "rounds_kills.csv":
                df = extract_round_number(df)
            df = convert_to_float(df)
            df = convert_to_int(df)
            df = convert_to_str(df)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            s3_client.put_object(Bucket="cleaned-data-vct", Key=f"{csv_file}.csv", Body=csv_buffer.getvalue())

    for year in years:
        for csv_file in csv_files_dict[year]["agents"]:
            file_name = csv_file.split("/")[-1]
            df = load_csv_to_dataframe(s3_client, bucket_name, csv_file)
            df = remove_white_spaces(df)
            df = remove_white_spaces_in_between(df)
            df = remove_tabs_and_newlines(df)
            df = update_player_names(df)
            df = update_team_names(df)
            df = update_match_names(df)
            df = fixed_team_names(df)
            df = fixed_player_names(df)
            df = convert_nan_players_teams(df)
            df = convert_to_str(df) 
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            s3_client.put_object(Bucket="cleaned-data-vct", Key=f"{csv_file}.csv", Body=csv_buffer.getvalue())

    for year in years:
        for csv_file in csv_files_dict[year]["players_stats"]:
            file_name = csv_file.split("/")[-1]
            df = load_csv_to_dataframe(s3_client, bucket_name, csv_file)
            df = remove_white_spaces(df)
            df = remove_white_spaces_in_between(df)
            df = remove_tabs_and_newlines(df)
            df = update_player_names(df)
            df = update_team_names(df)
            df = update_match_names(df)
            df = fixed_team_names(df)
            df = fixed_player_names(df)
            df = convert_nan_players_teams(df)
            df = convert_to_str(df)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            s3_client.put_object(Bucket="cleaned-data-vct", Key=f"{csv_file}.csv", Body=csv_buffer.getvalue())

    for year in years:
        for csv_file in csv_files_dict[year]["ids"]:
            df = load_csv_to_dataframe(s3_client, bucket_name, csv_file)
            df = remove_white_spaces(df)
            df = remove_white_spaces_in_between(df)
            df = remove_tabs_and_newlines(df)
            df = update_player_names(df)
            df = update_team_names(df)
            df = update_match_names(df)
            df = add_missing_player(df, year)
            df = add_missing_matches_id(df, year)
            df = fixed_match_names(df)
            df = convert_to_int(df)
            df = convert_to_str(df)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            s3_client.put_object(Bucket="cleaned-data-vct", Key=f"{csv_file}.csv", Body=csv_buffer.getvalue())

    


if __name__ == '__main__':
    main()
