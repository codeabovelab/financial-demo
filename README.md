## Overview
Initial data was supplied as a set of Excel files extracted from some business system. The goal of this small project was to generate 
more sample data basing on these Excel files. We used PostgreSQL for data generation as needed to implement some specific business rules
 like having allocation datetime values in a working hours range etc.

In order to load initial data from Excel files to PostgreSQL we first converted everything to CSV (xlsx2csv python utility) 
and then used a pgfutter utility to create database tables from CSV files.

Then we applied some custom SQL for data generation which can be found in /sql subfolder of the repo.

Results of the generation are exported back to CSV files and uploaded to Arcadia database, creating some intermediate views for derived-data calculations where necessary 
(for instance risk value calculations). See `/scripts/3-risk-upload.sh` file for details on such views.

## Installation & Usage
1. Setup Docker
2. Run Docker image with PostgreSQL server and populated source tables:
   ```
   docker run -it --name trades-postgres -p 5433:5432 -e POSTGRES_PASSWORD=trades_2017 -d glebst/psql_trades
   ```
   
3. Run scripts in /scripts directory.
   
   When using container for the first time you'll need to populate all intermediate block&allocation trade data so 
    run `1-base-csv.sh`.
    
   Then run `2-risk-regen.sh` that will use intermediate tables to generate future data - this takes quite a lot of time 
   (about a hour on a macbook pro for a two-month worth of future data). 
   
   For next runs if you just need to sample more future data and have intermediate tables already - 
   can run just the `2-risk-regen.sh` without the first script.
   
   Run `3-risk-upload.sh` to push generated CSV files for data quality risk assessment to Arcadia.
   Note there are a few views being created within the script. Also note SSH access will require Codeabovelab keys for access. 
    


