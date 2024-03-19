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
    if table == "matches":
        tournament_id, stage_id, match_type_id, match = values
        query = """
                SELECT {} FROM {} WHERE {} = $1 AND tournament_id = $2 AND stage_id = $3 AND match_type_id = $4 AND year = $5;
                """.format(primary_key, table, column_name)
        data = (match, tournament_id, stage_id, match_type_id, year)
    elif table == "match_types":
        tournament_id, stage_id, match_type = values
        query = """
                SELECT {} FROM {} WHERE {} = $1 AND tournament_id = $2 AND stage_id = $3 and year = $4;
                """.format(primary_key, table, column_name)
        data = (match_type, tournament_id, stage_id, year)
    elif table == "stages":
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
        elif table == "players":
            value = values
            query = """
                    SELECT {} FROM {} WHERE {} = $1;
                    """.format(primary_key, table, column_name)
            data = (value, )
        elif table == "teams":
            try:
                if values != "Stay Small, Stay Second" and "," in values:
                    teams = [team.strip() for team in values.split(",")]
                    data = [(team, ) for team in teams]
                else:
                    value = values
                    data = (value, )
            except:
                print(values)
                print(values.type)
            query = """
                    SELECT {} FROM {} WHERE {} = $1;
                    """.format(primary_key, table, column_name)
        elif table == "maps":
            value = values
            query = """
                    SELECT {} FROM {} WHERE {} = $1;
                    """.format(primary_key, table, column_name)
            data = (value,)
    if isinstance(data, list):
        result = []
        for team in data:
            result.append(await conn.fetchval(query, *team))
        # try:
        result = int("".join(map(str, result)))
        # except:
        #     print(teams)
    else:
        result = await conn.fetchval(query, *data)
    if result:
        return {values: result} 
    else:
        print(table)
        print(data)
        return None
    # if result:
    #     return result[0]
    # else:
    #     print(column_value, primary_key, table, column_name, tournament_id, stage_id, match_type_id)
    #     return ""