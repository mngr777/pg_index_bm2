#!/usr/bin/python3

import argparse
import datetime
from statistics import mean, median
from psycopg2 import sql as Sql
import cfg
import pg
import knn
import tiling

gVerbose = False

ColumnDefault = 'geom'
TimesDefault = 10

def vprint(*args, **kwargs):
    if (gVerbose): print(*args, **kwargs)

def time_ms_round(value):
    return round(value, 4)

def create_table(conn, name, columns):
    ident = Sql.Identifier(name)
    query = 'CREATE TABLE IF NOT EXISTS {} (' + columns + ')'
    conn.execute(Sql.SQL(query).format(ident))

def table_exists(conn, name):
    # TODO: check schema
    query = 'SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name=%s)'
    cursor = conn.execute(Sql.SQL(query), (name,))
    return cursor.fetchone()[0]

def table_is_empty(conn, name):
    ident = Sql.Identifier(name)
    cursor = conn.execute(Sql.SQL('SELECT COUNT(*) FROM {}').format(ident))
    return cursor.fetchone()[0] == 0

def drop_table(conn, name):
    ident = Sql.Identifier(name)
    conn.execute(Sql.SQL('DROP TABLE IF EXISTS {}').format(ident))

def get_index_name(table, column):
    return '{}_{}_idx'.format(table, column)

def create_gist_index(conn, table, index, column, fillfactor=None):
    table_ident = Sql.Identifier(table)
    index_ident = Sql.Identifier(index)
    column_ident = Sql.Identifier(column)
    query = 'CREATE INDEX {} ON {} USING GIST({})'
    params = ()
    if fillfactor is not None:
        query += ' WITH (fillfactor = %s)'
        params = (fillfactor,)
    conn.execute(Sql.SQL(query).format(index_ident, table_ident, column_ident), params)

def gist_index_stat(conn, name):
    cursor = conn.execute(Sql.SQL('SELECT gist_stat(%s)'), (name,))
    return cursor.fetchone()[0]

def drop_index(conn, name):
    ident = Sql.Identifier(name)
    conn.execute(Sql.SQL('DROP INDEX IF EXISTS {}').format(ident))

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True, help='Config')
    parser.add_argument('--data', help='Data file')
    parser.add_argument('--table', required=True, help='Table name')
    parser.add_argument('--column', default=ColumnDefault, help='Geometry column')
    parser.add_argument('--fillfactor', default=None, help='GiST index FILLFACTOR')
    parser.add_argument('--srid', type=int, default=0, help='Geometry SRID')
    parser.add_argument('--knn-points', help='kNN point list')
    parser.add_argument('--tiles', help='Tile list')
    parser.add_argument('--drop-table-before', action='store_true', default=False, help='Drop table before data import')
    parser.add_argument('--drop-table-after', action='store_true', default=False, help='Drop after running tests')
    parser.add_argument('--create-table', action='store_true', default=False, help='Create table before data import')
    parser.add_argument('--table-columns', help='Table columns (required with "--create-table" flag)')
    parser.add_argument('--times', type=int, default=TimesDefault, help='# of times to run tests')
    parser.add_argument('--verbose', action='store_true', default=False, help='Print log messages')
    return parser.parse_args()

# Create GiST index on column, return creation time (including latency) in ms
def test_create_gist_index(conn, args):
    fillfactor_text = ' FILLFACTOR {}'.format(args.fillfactor) if args.fillfactor is not None else ''
    print('CREATE GiST INDEX{}, {} time(s)'.format(fillfactor_text, args.times))
    index_name = get_index_name(args.table, args.column)
    create_times_ms = []
    for i in range(1, args.times + 1):
        # Drop index
        drop_index(conn, index_name)
        # Create index, measure time to complete
        vprint('  #{}'.format(i), end=' ')
        ts = datetime.datetime.now()
        create_gist_index(conn, args.table, index_name, args.column, args.fillfactor)
        time_ms = (datetime.datetime.now() - ts).total_seconds() * 1000
        vprint('{} ms'.format(time_ms_round(time_ms)))
        create_times_ms.append(time_ms)
    # Print results
    print('mean: {} ms, median: {} ms'.format(
        time_ms_round(mean(create_times_ms)),
        time_ms_round(median(create_times_ms))))
    print() # newline
    # Print gist_stat result
    print('gist_stat({}):'.format(index_name))
    print(gist_index_stat(conn, index_name))


def test_self_join_request(conn, args):
    table_ident = Sql.Identifier(args.table)
    column_ident = Sql.Identifier(args.column)
    query = 'EXPLAIN (ANALYZE, FORMAT JSON) SELECT COUNT(*) FROM {} a, {} b WHERE a.{} && b.{}'
    cursor = conn.execute(Sql.SQL(query).format(table_ident, table_ident, column_ident, column_ident))
    return pg.get_exec_time_ms(cursor)

def test_self_join(conn, args):
    # Create index
    index_name = get_index_name(args.table, args.column)
    drop_index(conn, index_name)
    create_gist_index(conn, args.table, index_name, args.column, args.fillfactor)

    # Run self-join query args.times times
    print('Self-join, {} time(s)'.format(args.times))
    exec_times_ms = []
    for i in range(1, args.times + 1):
        vprint('  #{}'.format(i), end=' ')
        time_ms = test_self_join_request(conn, args)
        vprint('{} ms'.format(time_ms_round(time_ms)))
        exec_times_ms.append(time_ms)

    # Print results
    print('mean: {} ms, median: {} ms'.format(
        time_ms_round(mean(exec_times_ms)),
        time_ms_round(median(exec_times_ms))))
    print() # newline

def test_tiling_request(conn, args, tile):
    table_ident = Sql.Identifier(args.table)
    column_ident = Sql.Identifier(args.column)
    env = tiling.tile_to_envelope_3857(tile)
    query = '''
EXPLAIN (ANALYZE, FORMAT JSON)
SELECT {}
FROM {}
WHERE ST_Intersects({}, ST_Transform(ST_MakeEnvelope(%s, %s, %s, %s, 3857), %s))
'''
    cursor = conn.execute(
        Sql.SQL(query).format(column_ident, table_ident, column_ident),
        (env[0], env[1], env[2], env[3], args.srid))
    return pg.get_exec_time_ms(cursor)

def test_tiling(conn, args):
    # Create index
    index_name = get_index_name(args.table, args.column)
    drop_index(conn, index_name)
    create_gist_index(conn, args.table, index_name, args.column, args.fillfactor)

    # Read tile list
    lines = tiling.load_data(args.tiles)
    print('Tiling, {} tiles, {} time(s) per tile'.format(len(lines), args.times))
    exec_times_ms = []
    for linum in range(0, len(lines)):
        # Get tile
        line = lines[linum]
        tile = line[0]
        vprint('  tile:', tiling.tile_to_string(tile), end=', ')
        # Run query args.times times
        tile_exec_times_ms = []
        for i in range(1, args.times + 1):
            time_ms = test_tiling_request(conn, args, tile)
            tile_exec_times_ms.append(time_ms)
        exec_times_ms.extend(tile_exec_times_ms) # add tile query exec times
        if gVerbose:
            vprint('mean: {} ms, median: {} ms'.format(
                time_ms_round(mean(tile_exec_times_ms)),
                time_ms_round(median(tile_exec_times_ms))))

    # Pring results
    print('mean: {} ms, median: {} ms'.format(
        time_ms_round(mean(exec_times_ms)),
        time_ms_round(median(exec_times_ms))))
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
    create_gist_index(conn, args.table, index_name, args.column, args.fillfactor)

    # Read point list
    lines = knn.load_data(args.knn_points)
    print('kNN, k={}, {} points, {} time(s) per point'.format(k, len(lines), args.times))
    exec_times_ms = []
    for linum in range(0, len(lines)):
        line = lines[linum]
        point = line[0]
        vprint('  point:', knn.point_to_string(point), end=', ')
        # Run query args.times times
        point_exec_times_ms = []
        for i in range(1, args.times + 1):
            time_ms = test_knn_request(conn, args, point, k)
            point_exec_times_ms.append(time_ms)
        exec_times_ms.extend(point_exec_times_ms) # add point query exec times
        if gVerbose:
            vprint('mean: {} ms, median: {} ms'.format(
                time_ms_round(mean(point_exec_times_ms)),
                time_ms_round(median(point_exec_times_ms))))

    # Print results
    print('mean: {} ms, median: {} ms'.format(
        time_ms_round(mean(exec_times_ms)),
        time_ms_round(median(exec_times_ms))))
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
        if not table_exists(conn, args.table) or table_is_empty(conn, args.table):
            vprint('Importing data from "{}"'.format(args.data))
            conn.run(args.data)
            reconnect = True
        else:
            print('Table "{}" already exists and is not empty, data will not be imported'.format(args.table))
            print() # newline

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
    print('Connection: "{}"'.format(conn_data['name']))

    # Connect
    vprint('Connecting to "{}"'.format(conn_data['name']))
    conn = pg.Connection(conn_data['params'], conn_data['name'])

    # Init
    init(conn, args)
    vprint() # newline

    ## Run tests

    # 1. GiST index creation time
    test_create_gist_index(conn, args)

    # 2. Self-join
    test_self_join(conn, args)

    # 3. Tiling
    if args.tiles:
        test_tiling(conn, args)

    # 4. kNN request time, k in {1, 100}
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
