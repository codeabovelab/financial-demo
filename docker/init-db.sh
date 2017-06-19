#!/bin/bash
#set -e

psql -v -U postgres <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS "pgcrypto";
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    CREATE USER trades;
    CREATE DATABASE trades;
EOSQL

pg_restore -C -U postgres -d trades < /sql/gmei.sql
pg_restore -C -U postgres -d trades < /sql/block.sql
pg_restore -C -U postgres -d trades < /sql/allocations.sql