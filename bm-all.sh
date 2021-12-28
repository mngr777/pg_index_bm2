#!/bin/bash

# roads_rdr
echo "# Roads around Vancouver"
./bm.py --config config.json \
        --data "data/roads_rdr_insert.sql" \
        --table roads_rdr \
        --knn-points "data/roads_rdr/knn.txt" \
        --tiles "data/roads_rdr/tiles.txt" \
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
            --knn-points "data/test_belarus/knn.txt" \
            --tiles "data/test_belarus/tiles.txt" \
            --srid 4326 \
            --fillfactor 20 \
            --times 10
done

