#!/usr/bin/env bash

export PGPASSWORD=trades_2017
psql -h localhost -p 5433 -U postgres -d trades -f ../sql/prepare-dk-data.sql
psql -h localhost -p 5433 -U postgres -d trades -f ../sql/prepare-sample-data.sql
mkdir ../dist
rm ../dist/BlockTradeFuture.csv
rm ../dist/AllocationsFuture.csv
psql -h localhost -p 5433 -U postgres -d trades -F $'\t' --no-align -c "COPY tmp_allocation_future TO STDOUT WITH CSV DELIMITER E'\t' " > ../dist/AllocationsFuture.csv
psql -h localhost -p 5433 -U postgres -d trades -F $'\t' --no-align -c "COPY tmp_block_future TO STDOUT WITH CSV DELIMITER E'\t' " > ../dist/BlockTradeFuture.csv