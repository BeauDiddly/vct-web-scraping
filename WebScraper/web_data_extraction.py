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
    team = list[4]
    action = list[5]
    map = list[6]

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