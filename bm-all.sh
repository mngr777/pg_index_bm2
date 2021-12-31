#!/bin/bash

# roads_rdr
echo "# Roads around Vancouver"
./bm.py --config config.json \
        --table roads_rdr \
        --srid 3005 \
        --zoom 14 \
        --times 10 \
        --out-json -

# OSM Belarus
echo "# OSM Belarus"
./bm.py --config config.json \
        --table "test_belarus" \
        --column "geom" \
        --srid 4326 \
        --zoom 11 \
        --times 3 \
        --out-json -
