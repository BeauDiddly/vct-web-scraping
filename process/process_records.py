from Connect.connect import create_db_url
from retrieve.retrieve import get_all_reference_ids
import asyncpg
async def create_reference_ids_dict(reference_ids, year):
    reference_ids_dict = {
        "tournaments": {},
        "stages": {},
        "match_types": {},
        "matches": {},
        "players": {},
        "teams": {},
        "maps": {},
        "agents": {}
    }
    db_url = create_db_url()
    async with asyncpg.create_pool(db_url) as pool:
        tournaments_records = await get_all_reference_ids(pool, "tournaments", year)
        tournament_ids = {record["tournament"]: record["tournament_id"] for record in tournaments_records}
        reference_ids_dict["tournaments"] = tournament_ids

        stages_records = await get_all_reference_ids(pool, "stages", year)
        stages_ids = {(record["stage"], record["tournament_id"]): record["stage_id"] for record in stages_records}
        reference_ids_dict["stages"] = stages_ids

        match_types_records = await get_all_reference_ids(pool, "match_types", year)
        match_types_ids = {(record["match_type"], record["tournament_id"], record["stage_id"]): record["match_type_id"] for record in match_types_records}
        reference_ids_dict["match_types"] = match_types_ids

        matches_records = await get_all_reference_ids(pool, "matches", year)
        matches_ids = {(record["match"], record["tournament_id"], record["stage_id"], record["match_type_id"]): record["match_id"] for record in matches_records}
        reference_ids_dict["matches"] = matches_ids
        
        if not reference_ids["players"] and not reference_ids["teams"] and not reference_ids["maps"] and not reference_ids["agents"]:
            players_records = await get_all_reference_ids(pool, "players", year)
            players_ids = {record["player"]: record["player_id"] for record in players_records}
            reference_ids_dict["players"] = players_ids

            agents_records = await get_all_reference_ids(pool, "agents", year)
            agents_ids = {record["agent"]: record["agent_id"] for record in agents_records}
            reference_ids_dict["agents"] = agents_ids

            teams_records = await get_all_reference_ids(pool, "teams", year)
            teams_ids = {record["team"]: record["team_id"] for record in teams_records}
            reference_ids_dict["teams"] = teams_ids

            maps_records = await get_all_reference_ids(pool, "maps", year)
            maps_ids = {record["map"]: record["map_id"] for record in maps_records}
            reference_ids_dict["maps"] = maps_ids
            
    reference_ids["tournaments"][year] = reference_ids_dict["tournaments"]
    reference_ids["stages"][year] = reference_ids_dict["stages"]
    reference_ids["match_types"][year] = reference_ids_dict["match_types"]
    reference_ids["matches"][year] = reference_ids_dict["matches"]

    if not reference_ids["players"] and not reference_ids["teams"] and not reference_ids["maps"] and not reference_ids["agents"]:
        reference_ids["players"] = reference_ids_dict["players"]
        reference_ids["agents"] = reference_ids_dict["agents"]
        reference_ids["teams"] = reference_ids_dict["teams"]
        reference_ids["maps"] = reference_ids_dict["maps"]