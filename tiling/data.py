import re

TileRe = re.compile('(\d+)/(\d+)/(\d+)')
TileReIndices = (1, 2, 3)

def read_line(line):
    pos = line.find(';')
    if pos != -1:
        line = line[:pos]
    match = TileRe.match(line)
    if not match:
        raise Exception('Invalid tile string: "{}"'.format(line))
    return ((
        int(match[TileReIndices[0]]),
        int(match[TileReIndices[1]]),
        int(match[TileReIndices[2]])),)

def load_data(path):
    with open(path, 'r') as fd:
        lines = fd.readlines()
        return list(map(read_line, lines))

def tile_to_string(tile):
    return '{}/{}/{}'.format(tile[0], tile[1], tile[2])
