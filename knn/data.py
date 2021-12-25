import re
from psycopg2 import sql as Sql

PointRe = re.compile('(\d+(\.\d+)?)\s+(\d+(\.\d+)?)')
PointReCoordIndex1 = 1
PointReCoordIndex2 = 3

def read_line(line):
    pos = line.find(';')
    if (pos != -1):
        line = line[:pos]
    match = PointRe.match(line)
    if not match:
        raise Exception('Invalid point string: "{}"'.format(line))
    return ((match[PointReCoordIndex1], match[PointReCoordIndex2]),)

def load_data(path):
    with open(path, 'r') as fd:
        lines = fd.readlines()
        return list(map(read_line, lines))

