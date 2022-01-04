#!/usr/bin/python3

import argparse
import datetime
import json
from pathlib import Path
from statistics import mean, median
from psycopg2 import sql as Sql
import cfg
import pg
import re

MercatorSize = 20037508.3427892 * 2
GridSize=10

gVerbose = False

ColumnDefault = 'geom'
ZoomDefault = 12
TimesDefault = 10

TestKeys = ['create', 'selfjoin', 'tiling', 'knn']

def vprint(*args, **kwargs):
    if (gVerbose): print(*args, **kwargs)

def time_ms_round(value):
    return round(value, 4)

def bytes_to_str(value):
    return value.decode('utf-8')

def get_index_name(table, column):
    return '{}_{}_idx'.format(table, column)

def print_query(cursor):
    print('--------------------')
    print(bytes_to_str(cursor.query))
    print('--------------------')

def parse_gist_stats(stats):
    lines = stats.split('\n')
    pairs = [
        parse_gist_stats_line(line)
        for line in filter(lambda l: l, map(str.strip, lines))
    ]
    data = {}
    for pair in pairs:
        data[pair['key']] = pair['value']
    return data

def parse_gist_stats_line(line):
    parts = re.split(':\s+', line)
    return {'key': parts[0], 'value': parts[1]}

def output_json(path, data):
    s = json.dumps(data, indent=4)
    if path == '-':
        print(s)
    else:
        file_output(path, s)

def file_output(path, data):
    # TODO: overwrite confirmation
    with open(path, 'w') as fd:
        fd.write(data)

def create_gist_index(conn, table, index, column, fillfactor=None):
    table_ident = Sql.Identifier(table)
    index_ident = Sql.Identifier(index)
    column_ident = Sql.Identifier(column)
    query = 'CREATE INDEX {} ON {} USING GIST({})'
    params = ()
    if fillfactor is not None:
        query += ' WITH (fillfactor = %s)'
        params = (fillfactor,)
    return conn.execute(Sql.SQL(query).format(index_ident, table_ident, column_ident), params)

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
    parser.add_argument('--out-json', help='Output JSON file')
    parser.add_argument('--zoom', type=int, default=ZoomDefault, help='Zoom level')
    parser.add_argument('--srid', type=int, default=0, help='Geometry SRID')
    parser.add_argument('--times', type=int, default=TimesDefault, help='# of times to run tests')
    parser.add_argument('--skip', nargs='+', choices=TestKeys, help='List of tests to skip')
    parser.add_argument('--verbose', action='store_true', default=False, help='Print log messages')
    return parser.parse_args()

def get_tile_num(conn, args):
    table_ident = Sql.Identifier(args.table)
    column_ident = Sql.Identifier(args.column)
    query = '''SELECT COUNT(*) FROM (
  SELECT (
    ST_SquareGrid(
      %s,
      (SELECT ST_Transform(ST_SetSRID(ST_Extent({}), %s), 3857) FROM {}))
  ).*
) tiles'''
    cursor = conn.execute(
        Sql.SQL(query).format(column_ident, table_ident),
        (MercatorSize / 2**args.zoom, args.srid))
    return cursor.fetchone()[0]

# Create GiST index on column, return creation time (including latency) in ms
def test_create_gist_index(conn, args, data):
    fillfactor_text = ' FILLFACTOR {}'.format(args.fillfactor) if args.fillfactor is not None else ''
    print('CREATE GiST INDEX{}, {} times'.format(fillfactor_text, args.times))
    index_name = get_index_name(args.table, args.column)
    create_times_ms = []
    for i in range(1, args.times + 1):
        # Drop index
        drop_index(conn, index_name)
        # Create index, measure time to complete
        ts = datetime.datetime.now()
        cursor = create_gist_index(conn, args.table, index_name, args.column, args.fillfactor)
        time_ms = (datetime.datetime.now() - ts).total_seconds() * 1000
        # Print query first time
        if (i == 1):
            print_query(cursor)
        # Print result
        vprint('  #{} {} ms'.format(i, time_ms_round(time_ms)))
        # Store result
        create_times_ms.append(time_ms)

    # Print results
    test_data = {
        'mean': time_ms_round(mean(create_times_ms)),
        'median': time_ms_round(median(create_times_ms))
    }
    data['create_index_ms'] = test_data
    print('mean: {} ms, median: {} ms'.format(test_data['mean'], test_data['median']))
    print() # newline

    # Print gist_stat result
    print('gist_stat({}):'.format(index_name))
    gist_stats = gist_index_stat(conn, index_name)
    data['gist_stats'] = parse_gist_stats(gist_stats)
    print(gist_stats)

def test_self_join_request(conn, args):
    table_ident = Sql.Identifier(args.table)
    column_ident = Sql.Identifier(args.column)
    query = 'EXPLAIN (ANALYZE, FORMAT JSON) SELECT COUNT(*) FROM {} a, {} b WHERE a.{} && b.{}'
    return conn.execute(Sql.SQL(query).format(table_ident, table_ident, column_ident, column_ident))

def test_self_join(conn, args, data):
    # Create index
    index_name = get_index_name(args.table, args.column)
    drop_index(conn, index_name)
    create_gist_index(conn, args.table, index_name, args.column, args.fillfactor)

    # Run self-join query args.times times
    print('Self-join, {} times'.format(args.times))
    exec_times_ms = []
    for i in range(1, args.times + 1):
        # Run test query
        cursor = test_self_join_request(conn, args)
        time_ms = pg.get_exec_time_ms(cursor)
        # Print query first time
        if (i == 1):
            print_query(cursor)
        # Print result
        vprint('  #{} {} ms'.format(i, time_ms_round(time_ms)))
        # Store result
        exec_times_ms.append(time_ms)

    # Print results
    test_data = {
        'mean': time_ms_round(mean(exec_times_ms)),
        'median': time_ms_round(median(exec_times_ms))
    }
    data['self_join_ms'] = test_data
    print('mean: {} ms, median: {} ms'.format(test_data['mean'], test_data['median']))
    print() # newline

def test_tiling_request(conn, args):
    table_ident = Sql.Identifier(args.table)
    column_ident = Sql.Identifier(args.column)
    query = '''EXPLAIN (ANALYZE, FORMAT JSON)
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
ON true'''
    return conn.execute(
        Sql.SQL(query).format(column_ident, table_ident, column_ident, table_ident, column_ident),
        (MercatorSize / 2**args.zoom, args.srid, args.srid))

def test_tiling(conn, args, data, tile_num):
    # Create index
    index_name = get_index_name(args.table, args.column)
    drop_index(conn, index_name)
    create_gist_index(conn, args.table, index_name, args.column, args.fillfactor)

    # Run tiling request args.times times
    print('Tiling, zoom level {}, {} tiles, {} times'.format(args.zoom, tile_num, args.times))
    exec_times_ms = []
    for i in range(1, args.times + 1):
        # Run test query
        cursor = test_tiling_request(conn, args)
        time_ms = pg.get_exec_time_ms(cursor)
        # Print query first time
        if (i == 1):
            print_query(cursor)
        # Print result
        vprint('  #{} {} ms'.format(i, time_ms))
        # Store result
        exec_times_ms.append(time_ms)

    # Print results
    test_data = {
        'mean': time_ms_round(mean(exec_times_ms)),
        'median': time_ms_round(median(exec_times_ms))
    }
    data['tiling_ms'] = test_data
    print('mean: {} ms, median: {} ms'.format(test_data['mean'], test_data['median']))
    print() # newline

def test_knn_request(conn, args, k):
    table_ident = Sql.Identifier(args.table)
    column_ident = Sql.Identifier(args.column)
    query = '''EXPLAIN (ANALYZE, FORMAT JSON)
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
ON true'''
    return conn.execute(
        Sql.SQL(query).format(column_ident, table_ident, column_ident, table_ident, column_ident),
        (MercatorSize / 2**args.zoom, args.srid, args.srid, k))

# Load points from args.knn_points file, run kNN args.times times for each point
def test_knn(conn, args, data, k, tile_num):
    # Create index
    index_name = get_index_name(args.table, args.column)
    drop_index(conn, index_name)
    create_gist_index(conn, args.table, index_name, args.column, args.fillfactor)

    # Run kNN request args.times times
    print('kNN, k={}, zoom level {}, {} tiles, {} times'.format(k, args.zoom, tile_num, args.times))
    exec_times_ms = []
    for i in range(1, args.times + 1):
        # Run test query
        cursor = test_knn_request(conn, args, k)
        time_ms = pg.get_exec_time_ms(cursor)
        # Print query first time
        if (i == 1):
            print_query(cursor)
        # Print result
        vprint('  #{} {} ms'.format(i, time_ms))
        # Store result
        exec_times_ms.append(time_ms)

    # Print results
    test_data = {
        'mean': time_ms_round(mean(exec_times_ms)),
        'median': time_ms_round(median(exec_times_ms))
    }
    data['knn_{}_ms'.format(k)] = test_data
    print('mean: {} ms, median: {} ms'.format(test_data['mean'], test_data['median']))
    print() # newline

def run(conn_data, args):
    print('Connection: "{}"'.format(conn_data['name']))
    print() # newline

    # Connect
    conn = pg.Connection(conn_data['params'], conn_data['name'])

    ## Run tests

    data = {}
    def is_skipped(key):
        return args.skip and key in args.skip

    # 1. GiST index creation time
    if not is_skipped('create'):
        test_create_gist_index(conn, args, data)

    # 2. Self-join
    if not is_skipped('selfjoin'):
        test_self_join(conn, args, data)

    # Calc. number of tiles for tiling/knn
    tile_num = get_tile_num(conn, args)
    data['tile_num'] = tile_num

    # 3. Tiling
    if not is_skipped('tiling'):
        test_tiling(conn, args, data, tile_num)

    # 4. kNN request time, k in {1, 100}
    if not is_skipped('knn'):
        test_knn(conn, args, data, 1, tile_num)
        test_knn(conn, args, data, 100, tile_num)

    # Output JSON
    if args.out_json:
        output_json(args.out_json, data)

def main():
    global gVerbose

    args = parse_args()
    gVerbose = args.verbose

    config = cfg.load(args.config)
    for conn_data in config['connections']:
        run(conn_data, args)

if __name__ == '__main__':
    main()
