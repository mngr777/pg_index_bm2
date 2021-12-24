#!/usr/bin/python3

import argparse
import random
from psycopg2 import sql as Sql
import cfg
import pg

ColumnDefault = 'geom'
NumDefault = 10
NeighbourNumDefault = 1
RandomSeedDefault = 1

# TODO: handle empty table case

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True, help='Config')
    parser.add_argument('--connection', required=True, help='Connection name')
    parser.add_argument('--table', required=True, help='Table name')
    parser.add_argument('--column', default=ColumnDefault, help='Geometry column')
    parser.add_argument('--num', type=int, default=NumDefault, help='# of points')
    parser.add_argument('--add-neighbours', action='store_true', default=False, help='Get neighbours and add to output')
    parser.add_argument('--neighbour-num', type=int, default=NeighbourNumDefault, help='# of neighbours')
    parser.add_argument('--random-seed', type=int, default=RandomSeedDefault, help='Random seed')
    return parser.parse_args()

def random_coord(lo, hi):
    return random.uniform(lo, hi)

def random_point(xmin, ymin, xmax, ymax):
    return (random_coord(xmin, xmax), random_coord(ymin, ymax))

def point_to_string(point):
    return str(point[0]) + ' ' + str(point[1])

def get_srid(conn, args):
    table_ident = Sql.Identifier(args.table)
    column_ident = Sql.Identifier(args.column)
    # Select SRID from first record
    query = 'SELECT ST_SRID({}) FROM {} LIMIT 1'
    cursor = conn.execute(Sql.SQL(query).format(column_ident, table_ident))
    return cursor.fetchone()[0]

def get_points(conn, args):
    table_ident = Sql.Identifier(args.table)
    column_ident = Sql.Identifier(args.column)
    # Get table bounding box
    query = '''
SELECT ST_XMin(t.box), ST_YMin(t.box), ST_XMax(t.box), ST_YMax(t.box)
FROM (SELECT ST_Extent({}) AS box FROM {}) AS t
'''
    cursor = conn.execute(Sql.SQL(query).format(column_ident, table_ident))
    box = cursor.fetchone()
    # Generate random points within bounding box
    return [random_point(*box) for x in range(args.num)]

def get_neighbours(conn, args, srid, point):
    table_ident = Sql.Identifier(args.table)
    column_ident = Sql.Identifier(args.column)
    # Select neighbours ordered by distance
    query = '''
SELECT ST_AsText({}) FROM {}
ORDER BY {} <-> ST_SetSRID(ST_Point(%s, %s), %s) LIMIT %s
'''
    cursor = conn.execute(
        Sql.SQL(query).format(column_ident, table_ident, column_ident),
        (point[0], point[1], srid, args.neighbour_num))
    return [t[0] for t in cursor.fetchall()]

def run(conn_data, args):
    # Connect
    conn = pg.Connection(conn_data['params'], conn_data['name'])
    # Get SRID
    srid = get_srid(conn, args)
    # Generate random points
    points = get_points(conn, args)
    # Get neighbours
    for point in points:
        line = point_to_string(point)
        # Add neighbours
        if args.add_neighbours:
            line += ';' + ';'.join(get_neighbours(conn, args, srid, point))
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
            run(conn_data, args)
            return
    print('Connection name "{}" not found'.format(args.connection))

if __name__ == '__main__':
    main()

