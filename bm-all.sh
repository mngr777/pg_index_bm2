#!/bin/bash

# roads_rdr
echo "# Roads around Vancouver"
./bm.py --config config.json \
        --data "data/roads_rdr_insert.sql" \
        --table roads_rdr \
        --srid 3005 \
        --fillfactor 20 \
        --times 10

# OSM Belarus
declare -a belarus_items=('buildings')
for item in "${belarus_items[@]}"; do
    echo "# Belarus: ${item}"
    ./bm.py --config config.json \
            --data "data/test_belarus_${item}.sql" \
            --table "test_belarus_${item}" \
            --srid 4326 \
            --fillfactor 20 \
            --times 10
done

