#!/usr/bin/env bash

export PGPASSWORD=trades_2017
psql -h localhost -p 5433 -U postgres -d trades -f ../sql/prepare-allocations.sql
mkdir ../dist
rm ../dist/AllocationsSource.csv
psql -h localhost -p 5433 -U postgres -d trades -F $'\t' --no-align -c "COPY tmp_allocation_source TO STDOUT WITH CSV DELIMITER E'\t' " > ../dist/AllocationsSource.csv