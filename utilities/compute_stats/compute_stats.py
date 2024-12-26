import pandas as pd


def compute_stats(all_stat, attack_stat, defend_stat, stat_name, player_dict):
    if stat_name == "Rating" or stat_name == "Average Combat Score" or stat_name == "Average Damage per Round":
        if pd.notna(all_stat) and pd.notna(player_dict[stat_name]["both"]):
            total = (float(all_stat) + float(player_dict[stat_name]["both"])) / 2
            all_stat = f"{int(total * 100) / 100}"
        elif pd.notna(attack_stat) and pd.notna(player_dict[stat_name]["attack"]):
            total = (float(all_stat) + float(player_dict[stat_name]["attack"])) / 2
            attack_stat = f"{int(total * 100) / 100}"
        elif pd.notna(defend_stat) and pd.notna(player_dict[stat_name]["defend"]):
            total = (float(all_stat) + float(player_dict[stat_name]["defend"])) / 2
            defend_stat = f"{int(total * 100) / 100}"
    elif stat_name == "Headshot %" or stat_name == "Kill, Assist, Trade, Survive %":
        if pd.notna(all_stat) and pd.notna(player_dict[stat_name]["both"]):
            total = (float(all_stat.split("%")[0]) + float(player_dict[stat_name]["both"].split("%")[0])) / 2
            all_stat = f"{int(total * 100) / 100}%"
        elif pd.notna(attack_stat) and pd.notna(player_dict[stat_name]["attack"]):
            total = (float(all_stat.split("%")[0]) + float(player_dict[stat_name]["attack"].split("%")[0])) / 2
            attack_stat = f"{int(total * 100) / 100}%"
        elif pd.notna(defend_stat) and pd.notna(player_dict[stat_name]["defend"]):
            total = (float(all_stat.split("%")[0]) + float(player_dict[stat_name]["defend"].split("%")[0])) / 2
            defend_stat = f"{int(total * 100) / 100}%"
    else:
        if pd.notna(all_stat) and pd.notna(player_dict[stat_name]["both"]):
            total = int(all_stat) + int(player_dict[stat_name]["both"])
            all_stat = f"{total}"
        elif pd.notna(attack_stat) and pd.notna(player_dict[stat_name]["attack"]):
            total = int(all_stat) + int(player_dict[stat_name]["attack"])
            attack_stat = f"{total}"
        elif pd.notna(defend_stat) and pd.notna(player_dict[stat_name]["defend"]):
            total = int(all_stat) + int(player_dict[stat_name]["defend"])
            defend_stat = f"{total}"
    if pd.isna(all_stat):
        all_stat = player_dict[stat_name]["both"]
    elif pd.isna(attack_stat):
        attack_stat = player_dict[stat_name]["attack"]
    elif pd.isna(defend_stat):
        defend_stat = defend_stat[stat_name]["defend"]
    return all_stat, attack_stat, defend_stat