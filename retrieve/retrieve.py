from Connect.execute_query import execute_query

def retrieve_foreign_key(curr, foreign_key, table, column_name, column_value):
    sql = f"SELECT {foreign_key} FROM {table} WHERE {column_name} = '{column_value}'"
    execute_query(curr, sql)
    result = curr.fetchone()
    if result:
        return result[0]
    else:
        return ""