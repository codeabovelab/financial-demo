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
   Note there are a few views being created 
    


