import json
from psycopg2 import sql as Sql

def get_exec_time_ms(cursor):
    data = cursor.fetchone()[0][0]
    return data['Execution Time']
