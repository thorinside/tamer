#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER nealsanche;
    CREATE DATABASE tamer;
    GRANT ALL PRIVILEGES ON DATABASE tamer TO nealsanche;
EOSQL
