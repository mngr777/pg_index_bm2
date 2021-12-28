#!/bin/bash

GF_DIR='geofabrik'
BELARUS_SHP_ARCHIVE_NAME="belarus-latest-free.shp.zip"
BELARUS_SHP_ARCHIVE_URL="https://download.geofabrik.de/europe/${BELARUS_SHP_ARCHIVE_NAME}"
BELARUS_BUILDINGS_SHP_FILE="gis_osm_buildings_a_free_1.shp"
BELARUS_BUILDINGS_SQL_TABLE="test_belarus_buildings"
BELARUS_BUILDINGS_SQL_FILE="${BELARUS_BUILDINGS_SQL_TABLE}.sql"
BELARUS_DATA_SRID="4326" # WGS_1984

# Unzip roads_rdr dump
if [ ! -f ./roads_rdr_insert.sql ]; then
    gunzip -c ./roads_rdr_insert.sql.gz > ./roads_rdr_insert.sql
fi

# Get Belarus OSM data from Geofabrik
if [ ! -f "${BELARUS_BUILDINGS_SQL_FILE}" ]; then
    mkdir -p geofabrik

    # Check if required SHP files exist
    if [ ! -f "${GF_DIR}/${BELARUS_BUILDINGS_SHP_FILE}" ]; then
        # Download SHP archive from Geofabrik
        if [ ! -f "${GF_DIR}/${BELARUS_SHP_ARCHIVE_NAME}" ]; then
            wget -O "${GF_DIR}/${BELARUS_SHP_ARCHIVE_NAME}" "${BELARUS_SHP_ARCHIVE_URL}"
        fi
        # Unzip
        unzip -d "${GF_DIR}" "${GF_DIR}/${BELARUS_SHP_ARCHIVE_NAME}"
    fi

    # Convert to SQL
    if [ -f "${GF_DIR}/${BELARUS_BUILDINGS_SHP_FILE}" ]; then
        shp2pgsql -s "${BELARUS_DATA_SRID}" "${GF_DIR}/${BELARUS_BUILDINGS_SHP_FILE}" "${BELARUS_BUILDINGS_SQL_TABLE}" > "${BELARUS_BUILDINGS_SQL_FILE}"
    else
        echo "File \`${BELARUS_BUILDINGS_SQL_FILE}' not found"
        exit
    fi

fi
