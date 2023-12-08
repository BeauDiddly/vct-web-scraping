def execute_query(cursor, query, data=None):
    if data:
        cursor.execute(query, data)
    else:
        cursor.execute(query)