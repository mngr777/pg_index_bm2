#!/bin/bash

GF_DIR='geofabrik'
BELARUS_PBF="belarus-latest.osm.pbf"
BELARUS_PBF_URL="https://download.geofabrik.de/europe/${BELARUS_PBF}"

# Unzip roads_rdr dump
if [ ! -f ./roads_rdr_insert.sql ]; then
    gunzip -c ./roads_rdr_insert.sql.gz > ./roads_rdr_insert.sql
fi

# Get Belarus OSM data from Geofabrik
if [ ! -f "${GF_DIR}/${BELARUS_PBF}" ];  then
    mkdir -p "${GF_DIR}"

    # Download PBF
    wget -O "${GF_DIR}/${BELARUS_PBF}" "${BELARUS_PBF_URL}"
fi
