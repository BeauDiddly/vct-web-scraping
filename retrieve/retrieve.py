from Connect.execute_query import execute_query

def retrieve_primary_key(curr, primary_key, table, column_name, column_value, year = None, tournament_id = None, stage_id = None, match_type_id = None):
    if match_type_id and stage_id and tournament_id:
        query = """
                SELECT {} FROM {} WHERE {} = %s AND tournament_id = %s AND stage_id = %s AND match_type_id = %s AND year = %s;
                """.format(primary_key, table, column_name)
        data = (column_value, tournament_id, stage_id, match_type_id, year)
    if stage_id and tournament_id:
        query = """
                SELECT {} FROM {} WHERE {} = %s AND tournament_id = %s AND stage_id = %s and year = %s;
                """.format(primary_key, table, column_name)
        data = (column_value, tournament_id, stage_id, year)
    elif tournament_id:
        query = """
                SELECT {} FROM {} WHERE {} = %s AND tournament_id = %s AND year = %s;
                """.format(primary_key, table, column_name)
        data = (column_value, tournament_id, year)
    else:
        if table == "tournaments":
            query = """
                    SELECT {} FROM {} WHERE {} = %s AND year = %s;
                    """.format(primary_key, table, column_name)
            data = (column_value, year)
        elif table == "teams":
            query = """
                    SELECT {} FROM {} WHERE {} = %s;
                    """.format(primary_key, table, column_name)
            data = (column_value,)
    execute_query(curr, query, data)
    result = curr.fetchone()
    if result:
        return result[0]
    else:
        print(column_value, primary_key, table, column_name, tournament_id, stage_id, match_type_id)
        return ""