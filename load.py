import duckdb
import os
import logging
import time

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

con = None

try:
        # Connect to local DuckDB instance
    con = duckdb.connect(database='emissions.duckdb', read_only=False)
    logger.info("Connected to DuckDB instance")
except Exception as e:
    logger.error(f"Failed to connect to DuckDB: {e}")

def load_parquet_files():

    con.execute(f"""
                DROP TABLE IF EXISTS yellow_taxis; 
                DROP TABLE IF EXISTS green_taxis;
                DROP TABLE IF EXISTS vehicle_emissions;
                """) #get rid of anything pre-existing just in case
    
    logger.info("Dropped table if exists") #I did this so I could keep track of where it was in the process

    #load in vehicle emissions data
    con.execute(f"""
                CREATE TABLE vehicle_emissions AS
                SELECT * FROM read_csv('./data/vehicle_emissions.csv');
                """)
    logger.info("Loaded vehicle emissions data")

    for taxi in ["yellow", "green"]: #two types of taxi data
        logger.info(f"Processing {taxi} taxi data") 
        table_created = False #no tables created yet
        for year in range (2015, 2025): #from 2015 to 2024
            logger.info(f"Processing year {year}") #making sure it went through every level
            for month in range(1, 13): #from January to December
                logger.info(f"Processing month {month:02}") #making sure it went through every level
                url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{year}-{month:02}.parquet"

                try:
                    if not table_created: #making sure table was made but only once
                            con.execute(f"""
                                CREATE TABLE {taxi}_taxis AS
                                SELECT * FROM read_parquet('{url}');
                                """)
                            logger.info(f"Created table {taxi}_taxis and loaded data from {url}")
                            table_created = True
                    else:
                            con.execute(f"""
                                    INSERT INTO {taxi}_taxis 
                                    SELECT * FROM read_parquet('{url}');
                                    """)
                            logger.info(f"Appended data from {url} into table {taxi}_taxis")
                except Exception as e:
                    logger.error(f"Failed to load data from {url}: {e}")
                    continue

                time.sleep(60) #to avoid overwhelming the server
          
    logger.info("Finished loading all parquet files")

if __name__ == "__main__":
    load_parquet_files()



