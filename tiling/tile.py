import math

MercatorMax = 20037508.3427892
MercatorMin = -MercatorMax
MercatorSize = abs(MercatorMax - MercatorMin)

def get_tile_size(zoom):
    return MercatorSize / 2**zoom

# https://github.com/pramsey/minimal-mvt/blob/8b736e342ada89c5c2c9b1c77bfcbcfde7aa8d82/minimal-mvt.py#L63
# tile is (x, y, zoom) tuple
def tile_to_envelope_3857(tile):
    tile_size = get_tile_size(tile[2])
    tile_x = MercatorMin + tile_size * tile[0]
    tile_y = MercatorMin + tile_size * tile[1]
    return (
        tile_x,
        tile_y,
        tile_x + tile_size,
        tile_y + tile_size)


def tile_bounds_3857(box, zoom):
    tile_size = get_tile_size(zoom)
    minx = math.floor((box[0] - MercatorMin) / tile_size)
    miny = math.floor((box[1] - MercatorMin) / tile_size)
    maxx = math.floor((box[2] - MercatorMin) / tile_size)
    maxy = math.floor((box[3] - MercatorMin) / tile_size)
    return (minx, miny, maxx, maxy)
