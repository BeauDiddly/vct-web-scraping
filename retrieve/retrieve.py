from Connect.execute_query import execute_query

def retrieve_foreign_key(curr, foreign_key, table, column_name, column_value):
    query = """
        SELECT {} FROM {} WHERE {} = %s;
    """.format(foreign_key, table, column_name)
    data = (column_value,)
    execute_query(curr, query, data)
    result = curr.fetchone()
    if result:
        return result[0]
    else:
        return ""
    
    