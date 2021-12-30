#!/bin/bash

. pg_env.sh

FILE="roads_rdr_insert.sql"
TABLE="roads_rdr"
PSQL="psql --no-password"

DROP_TABLE_SQL="DROP TABLE IF EXISTS ${TABLE}"

# Drop table
$PSQL -c "{DROP_TABLE_SQL}"
cat ${FILE} | ${PSQL}
