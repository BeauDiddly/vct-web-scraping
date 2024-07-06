import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# async def retrieve_primary_key(pool, primary_key, table, column_name, values, year = None):
#     if table not in ['matches', 'match_types', 'stages', 'tournaments', 'players', 'teams', 'maps', 'agents']:
#         raise ValueError("Invalid table name")
    
#     if column_name not in ["tournament", "stage", "match_type", "match", "team", "player", "agent", "map"]:
#         raise ValueError("Invalid column name")
    
#     async with pool.acquire() as conn:
#         parameter_index = 2
#         query = f"SELECT {primary_key} FROM {table} WHERE {column_name} = $1"
#         data = [values[0]]
#         if table in ["matches", "match_types", "stages"]:
#             query += f" AND tournament_id = ${parameter_index}"
#             data.append(values[1])  # Assumes values is a tuple/list
#             parameter_index += 1

#         if table in ["matches", "match_types"]:
#             query += f" AND stage_id = ${parameter_index}"
#             data.append(values[2])
#             parameter_index += 1

#         if table == "matches":
#             query += f" AND match_type_id = ${parameter_index}"
#             data.append(values[3])
#             parameter_index += 1

#         if table in ["matches", "match_types", "stages", "tournaments"]:
#             query += f" AND year = ${parameter_index}"
#             data.append(int(year))
#         if table not in ["teams", "players", "maps", "agents"]:
#             data = tuple(data)
#         query += ";"
#         data = tuple(data)
#         if len(values) > 1:
#             values = tuple(values)
#         else:
#             values = values[0]
#         try:
#             result = await conn.fetchval(query, *data)
#             return {values: result} if result else None
#         except Exception as e:
#             print(query)
#             logger.error(f"Error querying {table} with data {data} from value: {values}: {str(e)}")
#             return None
        
async def get_all_reference_ids(pool, table, year):
    if table not in ['matches', 'match_types', 'stages', 'tournaments', 'players', 'teams', 'maps', 'agents']:
        raise ValueError("Invalid table name")
    
    async with pool.acquire() as conn:
        columns = []
        match table:
            case "matches":
                columns.extend(["match_id", "tournament_id", "stage_id", "match_type_id", "match"])
            case "match_types":
                columns.extend(["match_type_id", "tournament_id", "stage_id", "match_type"])
            case "stages":
                columns.extend(["stage_id", "tournament_id", "stage"])
            case "tournaments":
                columns.extend(["tournament_id", "tournament"])
            case "agents":
                columns.extend(["agent_id", "agent"])
            case "teams":
                columns.extend(["team_id", "team"])
            case "players":
                columns.extend(["player_id", "player"])
            case "maps":
                columns.extend(["map_id", "map"])
        query = f"SELECT {', '.join(columns)} FROM {table}"
        if table in ["matches", "match_types", "stages", "tournaments"]:
            query += f" WHERE year = {year};"
        else:
            query += f";"
        try:
            result = await conn.fetch(query)
            return result
        except Exception as e:
            print(query)
            logger.error(f"Error querying {table}: {str(e)}")
            return None