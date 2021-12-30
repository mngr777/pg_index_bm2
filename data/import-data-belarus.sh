#!/bin/bash

. pg_env.sh

GF_DIR='geofabrik'
BELARUS_PBF="belarus-latest.osm.pbf"
TABLE="test_belarus"
PSQL="psql --no-password"

DROP_TABLE_SQL="DROP TABLE IF EXISTS ${TABLE}"
CREATE_TABLE_SQL="\
CREATE TABLE ${TABLE} (\
  geom geometry,\
  osm_type TEXT,\
  osm_id BIGINT,\
  osm_user TEXT,\
  ts TIMESTAMPTZ,\
  way_nodes BIGINT[],\
  tags JSONB);\
ALTER TABLE ${TABLE}\
  ALTER geom SET STORAGE EXTERNAL,\
  ALTER osm_type SET STORAGE MAIN,\
  ALTER osm_user SET STORAGE MAIN,\
  ALTER way_nodes SET STORAGE EXTERNAL,\
  ALTER tags SET STORAGE EXTERNAL,\
  SET (FILLFACTOR=100);"
CREATE_TABLE_SQL="\
CREATE TABLE ${TABLE} (\
  geom geometry,\
  osm_type TEXT,\
  osm_id BIGINT,\
  osm_user TEXT,\
  ts TIMESTAMPTZ,\
  way_nodes BIGINT[],\
  tags JSONB);"
IMPORT_SQL="COPY ${TABLE} FROM STDIN"

# Drop table
$PSQL -c "${DROP_TABLE_SQL}"
# Create table and import
$PSQL -c "${CREATE_TABLE_SQL}"
osmium export \
       -c osmium.config.json \
       -f pg \
       "${GF_DIR}/${BELARUS_PBF}" \
       -v --progress \
       | $PSQL -c "${IMPORT_SQL}"

