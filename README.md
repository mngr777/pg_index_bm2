## Description
PostgreSQL GiST index benchmark.

### Queries
* Index creation:
```
CREATE INDEX <index-name> ON <table> USING GIST(<geom-column>)[ WITH FILLFACTOR = <fillfactor>]
```
* Self-join:
```
EXPLAIN (ANALYZE, FORMAT JSON) SELECT COUNT(*) FROM <table> a, <table> b WHERE a.<geom-column> && b.<geom-column>
```
* Tiling:
```
EXPLAIN (ANALYZE, FORMAT JSON)
SELECT tile.i, tile.j, objects.geom
FROM (
  SELECT (
    ST_SquareGrid(
      <MercatorSize / 2^zoom>,
      (SELECT ST_Transform(ST_SetSRID(ST_Extent(<table>), <srid>), 3857) FROM <table>))
  ).*
) tile
JOIN LATERAL (
  SELECT {} AS geom FROM {} WHERE ST_Intersects({}, ST_Transform(tile.geom, <srid>))
) objects
ON true
```
* k nearest neighbours for k in {1, 100}, points are tile centers:
```
EXPLAIN (ANALYZE, FORMAT JSON)
SELECT tile.i, tile.j, objects.geom
FROM (
  SELECT (
    ST_SquareGrid(
      <MercatorSize / 2^zoom>,
      (SELECT ST_Transform(ST_SetSRID(ST_Extent(<table>), <srid>), 3857) FROM <table>))
  ).*
) tile
JOIN LATERAL (
  SELECT <geom-column> AS geom
  FROM <table>
  ORDER BY <geom-column> <-> ST_Centroid(ST_Transform(tile.geom, <table>))
  LIMIT {1,100}
) objects
ON true
```

## Usage
### Preparing data
Currently used test datasets are roads near Vancouver (https://lists.osgeo.org/pipermail/postgis-devel/2021-November/029225.html, included in this repo,
and Belarus latest PBF from Geofabrik (https://download.geofabrik.de/europe/belarus.html).

`./data/get-data.sh` script unzips `data/roads_rdr_insert.sql.gz` and downloads Belarus PBF file:
```
$ cd ./data
$ ./get-data.sh
```

`import-data-roads.sh` and `import-data-belarus.sh` import to database, edit credentials `pg_env.sh`, and add passwords to `~/.pgpass`.

## Running benchmark
### Configuration
PostreSQL connection names and parameters are set in `config.json` file (`--config` option of `bm.py`), see `config.sample.json`.

### Running
`bm-all.sh` script runs `bm.py` on all datasets. Copy and modify to change `bm.by` parameters as required.
