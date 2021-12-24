#!/usr/bin/python3

import argparse
import datetime
from psycopg2 import sql as Sql
import cfg
import pg

gVerbose = False

IndexColumnDefault = 'geom'
TimesDefault = 1

def vprint(*args, **kwargs):
    if (gVerbose): print(*args, **kwargs)

def create_table(conn, name, columns):
    ident = Sql.Identifier(name)
    query = 'CREATE TABLE {} (' + columns + ')';
    conn.execute(Sql.SQL(query).format(ident))

def drop_table(conn, name):
    ident = Sql.Identifier(name)
    conn.execute(Sql.SQL('DROP TABLE IF EXISTS {}').format(ident))

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
    parser.add_argument('--knn-srid', help='SRID for kNN request point')
    parser.add_argument('--table', required=True, help='Table name')
    parser.add_argument('--index-column', default=IndexColumnDefault, help='GiST index column')
    parser.add_argument('--drop-table-before', action='store_true', default=False, help='Drop table before data import')
    parser.add_argument('--drop-table-after', action='store_true', default=False, help='Drop after running tests')
    parser.add_argument('--create-table', action='store_true', default=False, help='Create table before data import')
    parser.add_argument('--table-columns', help='Table columns (required with "--create-table" flag)')
    parser.add_argument('--times', type=int, default=TimesDefault, help='# of times to run tests')
    parser.add_argument('--verbose', action='store_true', default=False, help='Print log messages')
    return parser.parse_args()

# Create GiST index on column, return creation time (including latency) in ms
def test_create_gist_index(conn, table, index, column):
    ts = datetime.datetime.now()
    create_gist_index(conn, table, index, column)
    return (datetime.datetime.now() - ts).total_seconds() * 1000

def test_knn(k, point):
    pass

def run(conn_data, args):
    # Connect
    vprint('Connecting to "{}"'.format(conn_data['name']))
    conn = pg.Connection(conn_data['params'], conn_data['name'])

    # Drop table
    if args.drop_table_before:
        vprint('Dropping table "{}"'.format(args.table))
        drop_table(conn, args.table)

    # Create table
    if args.create_table:
        vprint('Creating table "{}", columns: "{}"'.format(args.table, args.columns))
        create_table(conn, args.table, args.columns)

    # Import test data
    if args.data:
        vprint('Importing data from "{}"'.format(args.data))
        conn.run(args.data)

    # Reconnect
    vprint('Reconnecting')
    conn.reconnect()

    # Run tests
    index_name = '{}_{}_idx'.format(args.table, args.index_column)
    create_index_time_ms = []
    for i in range(1, max(1, args.times) + 1):
        drop_index(conn, index_name)

        # Create GiST index
        vprint(' #{} CREATE INDEX: '.format(i), end='')
        time_ms = test_create_gist_index(conn, args.table, index_name, args.index_column)
        vprint('{} ms'.format(time_ms))

        vprint() # newline

    # Drop table
    if args.drop_table_after:
        vprint('Dropping table "{}"'.format(args.table))


def main():
    global gVerbose

    args = parse_args()
    gVerbose = args.verbose

    config = cfg.load(args.config)
    for conn_data in config['connections']:
        run(conn_data, args)

if __name__ == '__main__':
    main()
