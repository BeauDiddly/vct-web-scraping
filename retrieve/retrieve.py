from Connect.execute_query import execute_query
import asyncpg

async def retrieve_primary_key(conn, primary_key, table, column_name, values, year = None):
    
    # if table == "stages":
    #     tournament_id, stage = values


    # if table == "tournaments":
    #     tournament = values[0]
    #     query = "SELECT {} FROM {} WHERE {} = $1 AND year = $2;".format(primary_key, table, column_name)
    #     data = (tournament, year)
        # result = await conn.fetchval(query, column_value, year)
    #     return result if result else None
    # if match_type_id and stage_id and tournament_id:
    #     query = """
    #             SELECT {} FROM {} WHERE {} = $1 AND tournament_id = $2 AND stage_id = $3 AND match_type_id = $4 AND year = $5;
    #             """.format(primary_key, table, column_name)
    #     data = (column_value, tournament_id, stage_id, match_type_id, year)
    # if stage_id and tournament_id:
    #     query = """
    #             SELECT {} FROM {} WHERE {} = $1 AND tournament_id = $2 AND stage_id = $3 and year = $4;
    #             """.format(primary_key, table, column_name)
    #     data = (column_value, tournament_id, stage_id, year)
    if table == "stages":
        tournament_id, stage = values
        query = """
                SELECT {} FROM {} WHERE {} = $1 AND tournament_id = $2 AND year = $3;
                """.format(primary_key, table, column_name)
        data = (stage, tournament_id, year)
    else:
        if table == "tournaments":
            tournament = values
            query = """
                    SELECT {} FROM {} WHERE {} = $1 AND year = $2;
                    """.format(primary_key, table, column_name)
            data = (tournament, year)
        elif table == "teams":
            team = values
            query = """
                    SELECT {} FROM {} WHERE {} = $1;
                    """.format(primary_key, table, column_name)
            data = (team)
    result = await conn.fetchval(query, *data)
    return {values: result} if result else None
    # if result:
    #     return result[0]
    # else:
    #     print(column_value, primary_key, table, column_name, tournament_id, stage_id, match_type_id)
    #     return ""