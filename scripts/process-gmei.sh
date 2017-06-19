#!/usr/bin/env bash

export PGPASSWORD=trades_2017
psql -h localhost -p 5433 -U postgres -d trades -f ../sql/prepare-gmei.sql
mkdir ../dist
rm ../dist/GMEIGoldenSource.csv
psql -h localhost -p 5433 -U postgres -d trades -F $'\t' --no-align -c "COPY tmp_gmei_source TO STDOUT WITH CSV DELIMITER E'\t' " > ../dist/GMEIGoldenSource.csv
rm ../dist/GMEI_full.csv
psql -h localhost -p 5433 -U postgres -d trades -F $'\t' --no-align -c "COPY gmeiissuedfullfile_csv TO STDOUT WITH CSV DELIMITER E'\t' " > ../dist/GMEI_full.csv