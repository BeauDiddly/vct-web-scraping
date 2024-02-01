def get_maps_id(divs, dict):
    for div in divs:
        if div.get("data-game-id") and div.get("data-disabled") == "0":
            id = div.get("data-game-id")
            map = re.sub(r"\d+|\t|\n", "", div.text.strip())
            dict[id] = map

