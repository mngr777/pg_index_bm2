#!/usr/bin/python3

import argparse
import datetime
from statistics import mean, median
from psycopg2 import sql as Sql
import cfg
import pg
import knn

gVerbose = False

ColumnDefault = 'geom'
TimesDefault = 10

def vprint(*args, **kwargs):
    if (gVerbose): print(*args, **kwargs)

def create_table(conn, name, columns):
    ident = Sql.Identifier(name)
    query = 'CREATE TABLE {} (' + columns + ')';
    conn.execute(Sql.SQL(query).format(ident))

def drop_table(conn, name):
    ident = Sql.Identifier(name)
    conn.execute(Sql.SQL('DROP TABLE IF EXISTS {}').format(ident))

def get_index_name(table, column):
    return '{}_{}_idx'.format(table, column)

def create_gist_index(conn, table, index, column):
    table_ident = Sql.Identifier(table)
    index_ident = Sql.Identifier(index)
    column_ident = Sql.Identifier(column)
    query = 'CREATE INDEX {} ON {} USING GIST({})'
    conn.execute(Sql.SQL(query).format(index_ident, table_ident, column_ident))

def drop_index(conn, name):
    ident = Sql.Identifier(name)
    conn.execute(Sql.SQL('DROP INDEX IF EXISTS {}').format(ident))

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True, help='Config')
    parser.add_argument('--data', help='Data file')
    parser.add_argument('--knn-points', help='kNN point list')
    parser.add_argument('--srid', default=0, help='Geometry SRID')
    parser.add_argument('--table', required=True, help='Table name')
    parser.add_argument('--column', default=ColumnDefault, help='Geometry column')
    parser.add_argument('--drop-table-before', action='store_true', default=False, help='Drop table before data import')
    parser.add_argument('--drop-table-after', action='store_true', default=False, help='Drop after running tests')
    parser.add_argument('--create-table', action='store_true', default=False, help='Create table before data import')
    parser.add_argument('--table-columns', help='Table columns (required with "--create-table" flag)')
    parser.add_argument('--times', type=int, default=TimesDefault, help='# of times to run tests')
    parser.add_argument('--verbose', action='store_true', default=False, help='Print log messages')
    return parser.parse_args()

# Create GiST index on column, return creation time (including latency) in ms
def test_create_gist_index(conn, args):
    print('CREATE INDEX')
    index_name = get_index_name(args.table, args.column)
    create_times_ms = []
    for i in range(1, args.times + 1):
        # Drop index
        drop_index(conn, index_name)
        # Create index, measure time to complete
        ts = datetime.datetime.now()
        create_gist_index(conn, args.table, index_name, args.column)
        time_ms = (datetime.datetime.now() - ts).total_seconds() * 1000
        create_times_ms.append(time_ms)
    # Print results
    print('mean: {} ms, median: {} ms'.format(mean(create_times_ms), median(create_times_ms)))
    print() # newline

def test_knn_request(conn, args, point, k):
    table_ident = Sql.Identifier(args.table)
    column_ident = Sql.Identifier(args.column)
    query = '''
EXPLAIN (ANALYZE, FORMAT JSON) SELECT {} FROM {}
ORDER BY {} <-> ST_SetSRID(ST_Point(%s, %s), %s) LIMIT %s
'''
    cursor = conn.execute(
        Sql.SQL(query).format(column_ident, table_ident, column_ident),
        (point[0], point[1], args.srid, k))
    return pg.get_exec_time_ms(cursor)

# Load points from args.knn_points file, run kNN args.times times for each point
def test_knn(conn, args, k):
    # Create index
    index_name = get_index_name(args.table, args.column)
    drop_index(conn, index_name)
    create_gist_index(conn, args.table, index_name, args.column)

    # Read point list
    lines = knn.load_data(args.knn_points)
    print('kNN, k={}, {} points'.format(k, len(lines)))
    exec_times_ms = []
    for line in lines:
        for i in range(1, args.times + 1):
            point = line[0]
            time_ms = test_knn_request(conn, args, point, k)
            exec_times_ms.append(time_ms)
    # Print results
    print('mean: {} ms, median: {} ms'.format(mean(exec_times_ms), median(exec_times_ms)))
    print() # newline

def init(conn, args):
    reconnect = False

    # Drop table
    if args.drop_table_before:
        vprint('Dropping table "{}"'.format(args.table))
        drop_table(conn, args.table)
        reconnect = True

    # Create table
    if args.create_table:
        vprint('Creating table "{}", columns: "{}"'.format(args.table, args.columns))
        create_table(conn, args.table, args.columns)
        reconnect = True

    # Import test data
    if args.data:
        vprint('Importing data from "{}"'.format(args.data))
        conn.run(args.data)
        reconnect = True

    if reconnect:
        # Reconnect
        vprint('Reconnecting')
        conn.reconnect()

def cleanup(conn, args):
    # Drop table
    if args.drop_table_after:
        vprint('Dropping table "{}"'.format(args.table))
        drop_table(conn, args.table)

def run(conn_data, args):
    # Connect
    vprint('Connecting to "{}"'.format(conn_data['name']))
    conn = pg.Connection(conn_data['params'], conn_data['name'])

    # Init
    init(conn, args)

    ## Run tests

    # GiST index creation time
    test_create_gist_index(conn, args)

    # kNN request time, k in {1, 100}
    if args.knn_points:
        test_knn(conn, args, 1)
        test_knn(conn, args, 100)

    # Cleanup
    cleanup(conn, args)

def main():
    global gVerbose

    args = parse_args()
    gVerbose = args.verbose

    config = cfg.load(args.config)
    for conn_data in config['connections']:
        run(conn_data, args)

if __name__ == '__main__':
    main()
