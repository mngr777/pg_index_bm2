import json

def get_exec_time_ms(cursor):
    data = cursor.fetchone()[0][0]
    return data['Execution Time']
