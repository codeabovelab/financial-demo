#!/usr/bin/env bash

export PGPASSWORD=trades_2017
psql -h localhost -p 5433 -U postgres -d trades -f ../sql/prepare-block.sql
mkdir ../dist
rm ../dist/BlockTradeSource.csv
psql -h localhost -p 5433 -U postgres -d trades -F $'\t' --no-align -c "COPY tmp_block_source TO STDOUT WITH CSV DELIMITER E'\t' " > ../dist/BlockTradeSource.csv