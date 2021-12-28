## Description
PostgreSQL GiST index benchmark.

### Queries
* Index creation:
```
CREATE INDEX <index-name> ON <relation> USING GIST(<geom-column>)[ WITH FILLFACTOR = <fillfactor>]
```
* Self-join:
```
EXPLAIN (ANALYZE, FORMAT JSON) SELECT COUNT(*) FROM <relation> a, <relation> b WHERE a.<geom-column> && b.<geom-column>
```
* Tiling (find all objects touching tile):
```
EXPLAIN (ANALYZE, FORMAT JSON)
SELECT <geom-column>
FROM <relation>
WHERE ST_Intersects({}, ST_Transform(ST_MakeEnvelope(<min-x>, <min-y>, <max-x>, <max-y>, 3857), <srid>))
```
* k nearest neighbours for k in {1, 100}:
```
EXPLAIN (ANALYZE, FORMAT JSON) SELECT <geom-column> FROM <relation>
ORDER BY <geom-column> <-> ST_SetSRID(ST_Point(<x>, <y>), <srid>) LIMIT <k>
```

## Usage
### Preparing data
Currently used test datasets are roads near Vancouver (https://lists.osgeo.org/pipermail/postgis-devel/2021-November/029225.html, included in this repo, modified to be used without `psql`)
and Belarus from Geofabrik (https://download.geofabrik.de/europe/belarus.html).

`./data/get-data.sh` script unzips `data/roads_rdr_insert.sql.gz`, downloads Belarus SHP files from Geofabrik and converts to SQL using `shp2pgsql`.
```
$ cd ./data
$ ./get-data.sh
```

## Running benchmark
### Configuration
PostreSQL connection names and parameters are set in `config.json` file (`--config` option of `bm.py`), see `config.sample.json`.

### Running
`bm-all.sh` script runs `bm.py` on all datasets. Copy and modify to change `bm.by` parameters as required.
