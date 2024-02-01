import pandas as pd

def extract_maps_id(divs, dict):
    for div in divs:
        if div.get("data-game-id") and div.get("data-disabled") == "0":
            id = div.get("data-game-id")
            map = re.sub(r"\d+|\t|\n", "", div.text.strip())
            dict[id] = map

def extract_maps_notes(divs, result, team_mapping, list):
    tournament_name = list[0]
    stage_name = list[1]
    match_type_name = list[2]
    match_name = list[3]

    try:
        if ";" in divs[-1].text:
            maps_notes = divs[-1].text.strip().split("; ")
            for note in maps_notes:
                if "ban" in note or "pick" in note:
                    team, action, map = note.split()
                    team = team_mapping[team]
                    result["draft_phase"].append([tournament_name, stage_name, match_type_name, match_name, team, action, map])
                
        else:
            print(f"For {tournament_name}, {stage_name}, {match_type_name}, {match_name}, its notes regarding the draft phase is empty")
    except IndexError:
        print(f"For {tournament_name}, {stage_name}, {match_type_name}, {match_name}, its notes regarding the draft phase is empty")

def extract_maps_headers(divs, result, team_a, team_b, list):
    tournament_name = list[0]
    stage_name = list[1]
    match_type_name = list[2]
    match_name = list[3]
    for header in divs:
        left_team_header, map_header, right_team_header = header.find_all(recursive=False)
        lt_score = left_team_header.find("div", class_="score").text.strip()
        lt_rounds_scores = left_team_header.find_all("span")
        map_info = map_header.text.strip().split()
        rt_score = right_team_header.find("div", class_="score").text.strip()
        rt_rounds_scores = right_team_header.find_all("span")
        map = map_info[0]

        lt_attacker_score, lt_defender_score = lt_rounds_scores[0].text.strip(), lt_rounds_scores[1].text.strip()
        rt_attacker_score, rt_defender_score = rt_rounds_scores[1].text.strip(), rt_rounds_scores[0].text.strip()
        try:
            lt_overtime_score = lt_rounds_scores[2].text.strip()
        except IndexError:
            lt_overtime_score = pd.NA
        try:
            rt_overtime_score = rt_rounds_scores[2].text.strip()
        except IndexError:
            rt_overtime_score = pd.NA
        try:
            duration = map_info[2]
        except IndexError:
            duration = pd.NA                


        result["maps_scores"].append([tournament_name, stage_name, match_type_name, match_name,
                                        map, team_a, lt_score, lt_attacker_score,
                                        lt_defender_score, lt_overtime_score,team_b,
                                        rt_score, rt_attacker_score, rt_defender_score,
                                        rt_overtime_score, duration])