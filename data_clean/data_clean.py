import pandas as pd

na_values = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND',
            '-1.#QNAN', '-NaN', '-nan', '1.#IND',
            '1.#QNAN', 'N/A', 'NULL', 'NaN',
            'n/a', 'null']

def csv_to_df(file):
    return pd.read_csv(file, na_values = na_values, keep_default_na=False)

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
                    "ŞAK": "Komşu's", "db": "DownBad", "Sampi": "Team Sampi", "tftt": "tftteam", "tftteam, tftt": "tftteam", "GB": "Gamebreakers", "InTenZive": "InTenZiveEU",
                    "Bandits": "Bandit ESC", "Dont": "Dont Wanna Try Hard", "OW": "Overwatch", "tftt, tftteam": "tftteam", "KJlA": "KJlACCHblE", "mcch": "mcchicken",
                    "sile": "silence", "Kron": "KronBars", "Holl": "Hollywood", "sile, silence": "silence", "Holl, Hollywood": "Hollywood", "Kron, KronBars": "KronBars",
                    "Tens": "Tensei Esports", "Thic": "ThiccWRECK", "Synt": "Synthetic", "Inte": "Interstate 95", "Dial": "Dialed In", "ATK": "ATK Mode", "Pira, Pirate Nation": "Pirate Nation",
                    "Wind": "Windstorm", "Swag": "Swagoi", "Pira": "Pirate Nation", "Puls": "Pulsar", "Jumb": "Jumba", "Scru": "Scrub Garden", "Arli": "Arlington E.C.",
                    "Dial, Dialed In": "Dialed In", "Pira, Pireate Nation": "Pirate Nation", "YKS": "Yankees", "FIA": "FREAKSINACTION", "Clan, Clan Infamous": "Clan Infamous",
                    "GMG, GOMA": "GOMA", "GMG": "GOMA", "EPA, Euphoria": "Euphoria", "GHS": "Gehenna Sweepers", "GHS, Gehenna Sweepers": "Gehenna Sweepers", "Gwan, GwangDae": "GwangDae",
                    "OLDG": "OLDGUYZ", "v0rt, v0rteX 5": "v0rteX 5", "MM": "Mob Mentality eSports", "Enig": "Enigma", "Doub": "DoubleCross eSports", "RESE": "Reservoir dogs",
                    "just": "justtry", "Les": "Les Babouches", "Anth": "Anthrax", "74, Nanashi no Gonbee": "Nanashi no Gonbee", "Endavant": "Team Endavant", "Rese": "Reservoir dogs",

}

misnamed_players = {"luk": "lukzera", "Λero": "Aero", "Playboi Joe": "velis", "WeDid": "wedid", "karma": "karmax1", "yeji": "shen", "xvr": "xeric",
                    "Oblivion": "icarus", "nickszxz": "nicksz", "m0rea": "budimeisteR", "mikalulba": "Mikael", "zhar": "Asteriskk", "justreggae": "reggae",
                    "BoBo": "Ender", "1000010": "01000010", "derilasong": "Fuqua", "Arquiza": "rkz", "par scofield": "parscofield", "florance": "flqrance",
                    "stev0r": "stev0rr", "unfaiR aK": "Unfair", "alulba": "aluba", "2": "002"}

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

def convert_to_str(df):
    for column in ["Team", "Team A", "Team B",
                  "Eliminator Team", "Eliminated Team", "Player",
                  "Player Team", "Enemy Team", "Enemy",
                  "Eliminator", "Eliminated"]:
        if column in df:
            df[column] = df[column].astype(str)
    return df

def convert_to_int(df):
    for column in ["Player ID", "Team ID", "Tournament ID", "Stage ID", "Match Type ID", "Match ID", "Game ID"]:
        if column in df:
            df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int32")
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

def fixed_player_names(df):
    for column in ["Player", "Enemy", "Eliminator", "Eliminated"]:
        if column in df:
            df[column] = df[column].map(misnamed_players).fillna(df[column])
    return df

def convert_nan_players_teams(df):
    if "Tournament" in df and "Stage" in df and "Match Type" in df and "Player" in df and "Team" in df:
        player_nan_condition = (df['Tournament'] == 'Champions Tour Philippines Stage 1: Challengers 2') & \
                               (df['Stage'].isin(['All Stages', 'Qualifier 1'])) & \
                               (df['Match Type'].isin(['Round of 16', 'All Match Types'])) & \
                               (df['Player'] == 'nan') & \
                               (df["Team"] == "nan")
        missing_bjk_condition = (df["Tournament"] == "Champions Tour Turkey Stage 1: Challengers 3") & \
                                (df["Stage"] == "Open Qualifier") & \
                                (df["Match Type"] == "Round of 32") & \
                                (df["Player"].isin(["Noffe", "m4rco", "vlt", "daNN", "MrFaliN"])) & \
                                (df["Team"] == "nan")
        pATE_condition = (df['Tournament'].isin(["Champions Tour Europe Stage 3: Challengers 2",
                                                "Champions Tour Europe Stage 3: Challengers 1", "Champions Tour Europe Stage 1: Challengers 1"])) & \
                         (df['Stage'].isin(['Open Qualifier', "Qualifier", 'All Stages'])) & \
                         (df['Match Type'].isin(['Round of 256', "Round of 128", 'All Match Types'])) & \
                         (df['Player'] == 'pATE') & \
                         (df["Team"] == "nan")
        wendigo_2021_condition = (df['Tournament'] == "Champions Tour North America Stage 2: Challengers 2") & \
                         (df['Stage'].isin(['Open Qualifier', 'All Stages'])) & \
                         (df['Match Type'].isin(["Round of 128", 'All Match Types'])) & \
                         (df['Player'] == 'Wendigo') & \
                         (df["Team"] == "nan")
        
        wendigo_2022_condition = (df['Tournament'] == "Champions Tour North America Stage 2: Challengers") & \
                         (df['Stage'].isin(["Open Qualifier #1", "Open Qualifier #2", 'All Stages'])) & \
                         (df['Match Type'].isin(["Round of 128", "Round of 64", 'All Match Types'])) & \
                         (df['Player'] == 'Wendigo') & \
                         (df["Team"] == "nan")

        howie_1_condition = (df['Tournament'] == "Oceania Tour: Stage 1") & \
                         (df['Stage'].isin(['Open Qualifier', 'All Stages'])) & \
                         (df['Match Type'].isin(["Round of 16", "Quarterfinals", "Lower Bracket Semifinals", "Lower Bracket Consolation Finals",
                                                 'All Match Types'])) & \
                         (df['Player'] == 'Howie') & \
                         (df["Team"] == "nan")

        howie_2_condition = (df['Tournament'] == "Oceania Tour: Stage 2") & \
                         (df['Stage'].isin(['Open Qualifier', 'All Stages'])) & \
                         (df['Match Type'].isin(["Round of 16", 'All Match Types'])) & \
                         (df['Player'] == 'Howie') & \
                         (df["Team"] == "nan")
        

        filtered_indices = df.index[player_nan_condition]
        df.loc[filtered_indices, "Team"] = "MGS Spades"

        filtered_indices = df.index[missing_bjk_condition]
        df.loc[filtered_indices, "Team"] = "Beşiktaş Esports"

        filtered_indices = df.index[pATE_condition]
        df.loc[filtered_indices, "Team"] = "EXEN Esports"

        filtered_indices = df.index[wendigo_2021_condition]
        df.loc[filtered_indices, "Team"] = "Chilling in Space"

        filtered_indices = df.index[wendigo_2022_condition]
        df.loc[filtered_indices, "Team"] = "Team MystiC"

        filtered_indices = df.index[howie_1_condition]
        df.loc[filtered_indices, "Team"] = "HEHE"

        filtered_indices = df.index[howie_2_condition]
        df.loc[filtered_indices, "Team"] = "Trident Esports"

    # elif "Player" in df and "Player ID" in df:
    #     player_nan_condiiton = (df["Player ID"] == 10207) & (df["Player"].isnull())
    #     filtered_indices = df.index[player_nan_condiiton]
    #     df.loc[filtered_indices, "Player"] = "nan"
    return df

def get_all_agents_played_for_kills_stats(df):
    filtered_df = df[df["Map"] != "All Maps"]
    filtered_df = df[["Tournament", "Stage", "Match Type", "Match Name", "Team", "Player", "Agent"]]
    agents_dict = {}
    for tournament, stage, match_type, match_name, team, player, agent in \
        zip(filtered_df["Tournament"], filtered_df["Stage"], filtered_df["Match Type"], filtered_df["Match Name"],
            filtered_df["Team"], filtered_df["Player"], filtered_df["Agent"]):
        if pd.notna(player) and pd.notna(agent):
            agents_dict.setdefault((tournament, stage, match_type, match_name, team, player), set()).add(agent)
    for tuple, agents in agents_dict.items():
        agents_dict[tuple] = ", ".join(sorted(list(agents)))
    all_maps_rows = df[df["Map"] == "All Maps"]
    for tuple, agents in agents_dict.items():
        if "," in agents:
            tournament, stage, match_type, match_name, team, player = tuple[0], tuple[1], tuple[2], tuple[3], tuple[4], tuple[5]
            mask = (all_maps_rows["Tournament"] == tournament) & \
                (all_maps_rows["Stage"] == stage) & \
                (all_maps_rows["Match Type"] == match_type) & \
                (all_maps_rows["Match Name"] == match_name) & \
                (all_maps_rows["Team"] == team) & \
                (all_maps_rows["Player"] == player)
            all_maps_rows.loc[mask, "Agent"] = agents
    df.loc[df["Map"] == "All Maps"] = all_maps_rows
    #         mask_dict[tuple] = mask
    
    # for tuple, mask in mask_dict.items():
    #         agents = agents_dict[tuple]
    #         df.loc[mask, "Agent"] = agents
    return df


    

def remove_tabs_and_newlines(df):
    if "Map" in df:
        df["Map"] = df["Map"].str.replace("\t", "").replace("\n", "")
        df["Map"] = df["Map"].str.split("\n").str[0]
    return df

def add_missing_player(df, year):
    if "Player" in df and "Player ID" in df:
        if year == 2021:
            df.loc[len(df.index)] = ["pATE", 9505]
            df.loc[len(df.index)] = ["Wendigo", 26880]
            # missing_pATE = pd.Series(["pATE", 9505], index=df.columns)
            # missing_Wendigo = pd.Series(["Wendigo", 26880], index=df.columns)
            # df = pd.concat([df, missing_pATE, missing_Wendigo], ignore_index=True)
        elif year == 2022:
            df.loc[len(df.index)] = ["Wendigo", 26880]
            # missing_Wendigo = pd.Series(["Wendigo", 26880], index=df.columns)
            # df = pd.concat([df, missing_Wendigo], ignore_index=True)
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

