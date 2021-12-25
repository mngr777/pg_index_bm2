import sys
import json

def get_exec_time_ms(cursor):
    data = cursor.fetchone()[0][0]
    return data['Execution Time'] * 1000
