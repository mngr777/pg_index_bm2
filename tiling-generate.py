#!/usr/bin/python3

import argparse
import random
from psycopg2 import sql as Sql
import cfg
import pg
import tiling

ColumnDefault = 'geom'
NumDefault = [0, 5, 10]
RandomSeedDefault = 1

def parse_args():
    class NumSplit(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            nums = list(map(lambda v: max(0, int(v)), values.split(',')))
            setattr(namespace, self.dest, nums)

    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True, help='Config')
    parser.add_argument('--connection', required=True, help='Connection name')
    parser.add_argument('--table', required=True, help='Table name')
    parser.add_argument('--column', default=ColumnDefault, help='Geometry column')
    parser.add_argument('--zoom-start', type=int, default=1, help='Zoom level start value for --num')
    parser.add_argument('--num', action=NumSplit, default=NumDefault, help='# of points for each zoom level starting from 1, comma-separated')
    parser.add_argument('--random-seed', type=int, default=RandomSeedDefault, help='Random seed')
    return parser.parse_args()


def get_srid(conn, args):
    table_ident = Sql.Identifier(args.table)
    column_ident = Sql.Identifier(args.column)
    # Select SRID from first record
    query = 'SELECT ST_SRID({}) FROM {} LIMIT 1'
    cursor = conn.execute(Sql.SQL(query).format(column_ident, table_ident))
    return cursor.fetchone()[0]

# Get bounding box in SDID 3857 (Mercator)
def get_bounding_box(conn, args, srid):
    table_ident = Sql.Identifier(args.table)
    column_ident = Sql.Identifier(args.column)
    query = '''
SELECT ST_XMin(t.box), ST_YMin(t.box), ST_XMax(t.box), ST_YMax(t.box)
FROM (SELECT ST_Transform(ST_SetSRID(ST_Extent({}), %s), 3857) AS box FROM {}) AS t
'''
    cursor = conn.execute(Sql.SQL(query).format(column_ident, table_ident), (srid,))
    return cursor.fetchone()

# TODO: dublicates?
def random_tile(bounds, zoom):
    return (
        random.randrange(bounds[0], bounds[2] + 1),
        random.randrange(bounds[1], bounds[3] + 1),
        zoom)

def get_tiles(conn, args, box):
    tiles = []
    for i in range(0, len(args.num)):
        num = args.num[i]
        zoom = i + max(1, args.zoom_start)
        bounds = tiling.tile_bounds_3857(box, zoom)
        for j in range(0, num):
            tiles.append(random_tile(bounds, zoom))
    return tiles

def count_tile_objects(conn, args, srid, tile):
    table_ident = Sql.Identifier(args.table)
    column_ident = Sql.Identifier(args.column)
    env = tiling.tile_to_envelope_3857(tile)
    query = '''
SELECT COUNT(*) AS num
FROM {}
WHERE ST_Intersects({}, ST_Transform(ST_MakeEnvelope(%s, %s, %s, %s, 3857), %s))
'''
    cursor = conn.execute(
        Sql.SQL(query).format(table_ident, column_ident),
        (env[0], env[1], env[2], env[3], srid))
    return cursor.fetchone()[0]

def run(conn, args):
    # Get SRID
    srid = get_srid(conn, args)

    # Get bounding box
    box = get_bounding_box(conn, args, srid)

    # Generate tiles
    for tile in get_tiles(conn, args, box):
        num = count_tile_objects(conn, args, srid, tile)
        line = tiling.tile_to_string(tile) + ';' + str(num)
        print(line)


def main():
    args = parse_args()
    config = cfg.load(args.config)

    # Initialize PRNG
    if args.random_seed > 0:
        random.seed(args.random_seed)

    # Find connection by name and run
    for conn_data in config['connections']:
        if conn_data['name'] == args.connection:
            run(pg.Connection(conn_data['params'], conn_data['name']), args)
            return
    print('Connection name "{}" not found'.format(args.connection))

if __name__ == '__main__':
    main()
