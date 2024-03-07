import pandas as pd

misnamed_teams = {"BLISS": "Team Bliss", "WaVii": "Team WaVii", "Arrow": "Team Arrow", "FNLTY": "Finality", "100su": "Mujig 100su",
                    "RRQ": "Rex Regum Qeon", "NXLGA": "NXLG Academy", "57RBB": "57 Rach Bunh Binh", "Kantic": "Kantic Gaming",
                    "NAJIN": "NAJIN Rivals", "ex-R5": "ex-Recon 5", "Swa": "Sway eSports", "SR.GC": "Shopify Rebellion GC",
                    "TGH L": "The Goose House Lunar", "Mustang": "Mustang Gaming", "Baec": "Baecon GG", "Vexed": "Vexed Gaming",
                    "NRAGE": "ENRAGE", "BK ROG": "BK ROG Esports", "kom4ka": "komand04ka", "eXiLe": "eXiLe eSports", "Orbit": "Orbit",
                    "XCN": "VOIN DRIP", "CNL": "VOIN DRIP", "FAV": "FAV gaming", "New": "New Team", "AE": "AKIHABARA ENCOUNT",
                    "Rebe": "Rebellion Arts", "YA": "Ingenuity", "Team": "rööcivarkaat", "ROISS": "Roisseur", "XTM": "VOIN DRIP",
                    "Recca.P'": "Recca Project", "YTX": "VOIN DRIP", "TOKYO": "Tokyo", "INRE": "SchwarzerLand", "ICC": "SchwarzerLand",
                    "INS": "SchwarzerLand", "BB": "SchwarzerLand", "no n": "SchwarzerLand", "tp2luz": "tp i 2 luza", "TMSPACE": "TEAM SPACE",
                    "VEXEC": "ValorantEXEC", "DLWZK": "DELIRAWOW", "Wolves": "Wolves of Hell", "BENCH": "Benchwarmers", "sauce": "sas elle",
                    "Sangal": "Sangal Esports", "Vend": "Recca Project", "R&MB": "rice and meatballs", "Wygers": "Wygers Argentina",
                    "Procyon": "Procyon Team", "PÊEK": "PÊEK Gaming", "Whynot": "WhynotVLR", "anime": "animeshniki", "ZMM.B": "Zero MarksMen Black",
                    "SR.GC": "Shopify Rebellion GC", "ASTRO": "Astro Esports", "BLIND": "Blind Esports", "IGZ": "SchwarzerLand", "VOX.HK": "VOX Hakaishin",
                    "FTZ.R": "ForteZ Regulus", "KRAKEN": "KRAKEN ESC", "STY": "SchwarzerLand", "UAESA.G": "UAESA Green", "ZMM.S'": "Zero MarksMen Silver",
                    "Azules": "Azules Esports", "Reaper": "The Reaper", "SPACE": "Space Walkers", "SPX": "Storks Phalanx", "NTH": "SchwarzerLand",
                    "RIVAL": "Rival Esports Alpha", "TAIYO": "Taiyo eSports", "ex-WC": "ex-Wildcard Gaming", "MXBSS": "MXB South Side", "VCTRY": "VCTRY Esports",
                    "ODIUM": "Odium", "Cavalry": "Cavalry Esports", "Focus": "Team Focus", "Fre": " Frederikssund Vikings", "GLD.GC": "Guild GC", "MANGAL": "TEAM MANGAL",
                    "ŞAK": "Komşu's", "db": "DownBad", "Sampi": "Team Sampi"
}

agent_to_player = {"brimstone": "Nappi", "sova": "maik", "skye": "johkubb", "raze": "Puoli", "cypher": "akumeni", "breach": "johkubb", "omen": "Nappi",
                   "killjoy": "akumeni", "jett": "Puoli"}

def remove_white_spaces(df):
    for column in ["Stage", "Match Type", "Match Name",
                  "Team", "Team A", "Team B",
                  "Eliminator Team", "Eliminated Team", "Player",
                  "Player Team", "Enemy Team", "Enemy",
                  "Eliminator", "Eliminated"]:
        if column in df:
            df[column] = df[column].apply(lambda x: x.strip() if isinstance(x, str) else x)
    return df

def remove_white_spaces_in_between(df):
    if "Match Name" in df:
        df["Match Name"] = df["Match Name"].str.split().apply(lambda x: " ".join(x))
    return df

def fixed_team_names(df):
    for column in ["Team", "Team A", "Team B", "Eliminator Team", "Eliminated Team", "Player Team", "Enemy Team"]:
        if column in df:
            df[column] = df[column].map(misnamed_teams).fillna(df[column])
    return df

def remove_tabs_and_newlines(df):
    if "Map" in df:
        df["Map"] = df["Map"].str.replace("\t", "").replace("\n", "")
        df["Map"] = df["Map"].str.split("\n").str[0]
    return df

def insert_missing_players(df):
    for column in ["Player", "Eliminator", "Eliminated"]:
        for another_column in ["Agent", "Agents"]:
            if column in df and another_column in df:
                missing_player_rows = df[df[column].isna()]

                for i, row in missing_player_rows.iterrows():
                    agent = row[another_column]
                    if agent in agent_to_player:
                        player = agent_to_player[agent]
                        df.at[i, column] = player
                    # else:
                    #     print(row)
    return df

