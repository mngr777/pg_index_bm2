#!/bin/bash

# roads_rdr
echo "# Roads around Vancouver"
./bm.py --config config.json \
        --data "data/roads_rdr_insert.sql" \
        --table roads_rdr \
        --srid 3005 \
        --times 10

# OSM Belarus
echo "# OSM Belarus"
./bm.py --config config.json \
        --table "test_belarus" \
        --column "geom" \
        --srid 4326 \
        --times 1

