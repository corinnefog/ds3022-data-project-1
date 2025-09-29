import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log')
logger = logging.getLogger(__name__)

con = duckdb.connect(database='emissions.duckdb', read_only=False)

def table_exists(table_name):
    result = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_name = '{table_name}'
    """).fetchone()[0]
    return result > 0

try:
    if table_exists('yellow_taxis'):  #checking for table name
        logger.info("Deduplicating yellow taxi table...")
        con.execute("""
            CREATE TABLE yellow_taxis_clean AS
            SELECT DISTINCT
                tpep_pickup_datetime,
                tpep_dropoff_datetime,
                passenger_count,
                trip_distance,
                fare_amount
            FROM yellow_taxis
        """)
        con.execute("DROP TABLE yellow_taxis")
        con.execute("ALTER TABLE yellow_taxis_clean RENAME TO yellow_taxis") #renaming the cleaned table back to original name
        logger.info("Successfully deduplicated yellow taxi table")
    else:
        logger.warning("yellow_taxis table not found")
except Exception as e:
    logger.error(f"Failed to deduplicate yellow taxi table: {e}")

# Green taxi deduplication
try:
    if table_exists('green_taxis'):
        logger.info("Deduplicating green taxi table...") 
        con.execute("""
            CREATE TABLE green_taxis_clean AS
            SELECT DISTINCT
                lpep_pickup_datetime,
                lpep_dropoff_datetime,
                passenger_count,
                trip_distance,
                fare_amount
            FROM green_taxis
        """) #only using necessary columns
        con.execute("DROP TABLE green_taxis")
        con.execute("ALTER TABLE green_taxis_clean RENAME TO green_taxis")
        logger.info("Successfully deduplicated green taxi table")
    else:
        logger.warning("green_taxis table not found")
except Exception as e:
    logger.error(f"Failed to deduplicate green taxi table: {e}")


try:
#get rid of trips with null or zero passenger count

    con.execute(f"""
            DELETE FROM yellow_taxis
            WHERE passenger_count IS NULL OR passenger_count = 0;
            DELETE FROM green_taxis
            WHERE passenger_count IS NULL OR passenger_count = 0;
            """)
    print("Removed records with null or zero passenger count")
    logger.info("Removed records with null or zero passenger count")
except Exception as e:
    logger.error(f"Failed to remove records with null or zero passenger count: {e}")

#get rid of trips with null or zero trip distance
try:
    con.execute(f"""
            DELETE FROM yellow_taxis
            WHERE trip_distance IS NULL OR trip_distance <= 0;
            DELETE FROM green_taxis
            WHERE trip_distance IS NULL OR trip_distance <= 0;
            """)
    print("Removed records with null or zero trip distance")
    logger.info("Removed records with null or zero trip distance")
except Exception as e:
    logger.error(f"Failed to remove records with null or zero trip distance: {e}")

#get rid of trips with trip distance greater than 100 miles, they're too long
try:
    con.execute(f"""
            DELETE FROM yellow_taxis
            WHERE trip_distance > 100;
            DELETE FROM green_taxis
            WHERE trip_distance > 100;
            """)
    print("Removed records with trip distance greater than 100 miles")
    logger.info("Removed records with trip distance greater than 100 miles")
except Exception as e:
    logger.error(f"Failed to remove records with trip distance greater than 100 miles: {e}")

#get rid of trips with trip duration greater than 24 hours, they're too long
try:
    con.execute(f"""
            DELETE FROM yellow_taxis
            WHERE tpep_dropoff_datetime - tpep_pickup_datetime > INTERVAL '24 hours'; 
            DELETE FROM green_taxis 
            WHERE lpep_dropoff_datetime - lpep_pickup_datetime > INTERVAL '24 hours';
            """)
    print("Removed records with trip duration greater than 24 hours")
    logger.info("Removed records with trip duration greater than 24 hours")
except Exception as e:
    logger.error(f"Failed to remove records with trip duration greater than 24 hours: {e}")


con.close()
