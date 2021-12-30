#!/usr/bin/python3

import argparse
import datetime
from statistics import mean, median
from psycopg2 import sql as Sql
import cfg
import pg

MercatorSize = 20037508.3427892 * 2
GridSize=10

gVerbose = False

ColumnDefault = 'geom'
ZoomDefault = 12
TimesDefault = 10

def vprint(*args, **kwargs):
    if (gVerbose): print(*args, **kwargs)

def time_ms_round(value):
    return round(value, 4)

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
    parser.add_argument('--table', required=True, help='Table name')
    parser.add_argument('--column', default=ColumnDefault, help='Geometry column')
    parser.add_argument('--fillfactor', default=None, help='GiST index FILLFACTOR')
    parser.add_argument('--zoom', default=12, help='Zoom level')
    parser.add_argument('--srid', type=int, default=0, help='Geometry SRID')
    parser.add_argument('--times', type=int, default=TimesDefault, help='# of times to run tests')
    parser.add_argument('--verbose', action='store_true', default=False, help='Print log messages')
    return parser.parse_args()

# Create GiST index on column, return creation time (including latency) in ms
def test_create_gist_index(conn, args):
    fillfactor_text = ' FILLFACTOR {}'.format(args.fillfactor) if args.fillfactor is not None else ''
    print('CREATE GiST INDEX{}, {} times'.format(fillfactor_text, args.times))
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
    print('Self-join, {} times'.format(args.times))
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

def test_tiling_request(conn, args):
    table_ident = Sql.Identifier(args.table)
    column_ident = Sql.Identifier(args.column)
    query = '''
EXPLAIN (ANALYZE, FORMAT JSON)
SELECT tile.i, tile.j, objects.geom
FROM (
  SELECT (
    ST_SquareGrid(
      %s,
      (SELECT ST_Transform(ST_SetSRID(ST_Extent({}), %s), 3857) FROM {}))
  ).*
) tile
JOIN LATERAL (
  SELECT {} AS geom FROM {} WHERE ST_Intersects({}, ST_Transform(tile.geom, %s))
) objects
ON true
'''
    cursor = conn.execute(
        Sql.SQL(query).format(column_ident, table_ident, column_ident, table_ident, column_ident),
        (MercatorSize / 2**args.zoom, args.srid, args.srid))
    return pg.get_exec_time_ms(cursor)

def test_tiling(conn, args):
    # Create index
    index_name = get_index_name(args.table, args.column)
    drop_index(conn, index_name)
    create_gist_index(conn, args.table, index_name, args.column, args.fillfactor)

    # Run tiling request args.times times
    print('Tiling, zoom level {}, {} times'.format(args.zoom, args.times))
    exec_times_ms = []
    for i in range(1, args.times + 1):
        vprint('  #{}'.format(i), end=' ')
        time_ms = test_tiling_request(conn, args)
        vprint('{} ms'.format(time_ms))
        exec_times_ms.append(time_ms)
    # Pring results
    print('mean: {} ms, median: {} ms'.format(
        time_ms_round(mean(exec_times_ms)),
        time_ms_round(median(exec_times_ms))))
    print() # newline

def test_knn_request(conn, args, k):
    table_ident = Sql.Identifier(args.table)
    column_ident = Sql.Identifier(args.column)
    query = '''
EXPLAIN (ANALYZE, FORMAT JSON)
SELECT tile.i, tile.j, objects.geom
FROM (
  SELECT (
    ST_SquareGrid(
      %s,
      (SELECT ST_Transform(ST_SetSRID(ST_Extent({}), %s), 3857) FROM {}))
  ).*
) tile
JOIN LATERAL (
  SELECT {} AS geom
  FROM {}
  ORDER BY {} <-> ST_Centroid(ST_Transform(tile.geom, %s))
  LIMIT %s
) objects
ON true
'''
    cursor = conn.execute(
        Sql.SQL(query).format(column_ident, table_ident, column_ident, table_ident, column_ident),
        (MercatorSize / 2**args.zoom, args.srid, args.srid, k))
    return pg.get_exec_time_ms(cursor)

# Load points from args.knn_points file, run kNN args.times times for each point
def test_knn(conn, args, k):
    # Create index
    index_name = get_index_name(args.table, args.column)
    drop_index(conn, index_name)
    create_gist_index(conn, args.table, index_name, args.column, args.fillfactor)

    # Run kNN request args.times times
    print('kNN, k={}, {} times'.format(k, args.times))
    exec_times_ms = []
    for i in range(1, args.times + 1):
        vprint('  #{}'.format(i), end=' ')
        time_ms = test_knn_request(conn, args, k)
        vprint('{} ms'.format(time_ms))
        exec_times_ms.append(time_ms)

    # Print results
    print('mean: {} ms, median: {} ms'.format(
        time_ms_round(mean(exec_times_ms)),
        time_ms_round(median(exec_times_ms))))
    print() # newline

def init(conn, args):
    pass

def cleanup(conn, args):
    # Drop table
    if args.drop_table_after:
        vprint('Dropping table "{}"'.format(args.table))
        drop_table(conn, args.table)

def run(conn_data, args):
    print('Connection: "{}"'.format(conn_data['name']))
    print() # newline

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
    test_tiling(conn, args)

    # 4. kNN request time, k in {1, 100}
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
