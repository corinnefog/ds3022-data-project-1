import duckdb
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log')
logger = logging.getLogger(__name__)

con = duckdb.connect(database='emissions.duckdb', read_only=False)

def run_analysis():
    #max CO2 emissions for both taxi types
    con.execute("""
          SELECT 
            (SELECT MAX(trip_co2_kgs) FROM yellow_transform) as max_co2_yellow,
            (SELECT MAX(trip_co2_kgs) FROM green_transform) as max_co2_green
        """)
    max_co2_yellow, max_co2_green = con.fetchone()
    print(f"Max CO2 Yellow: {max_co2_yellow}, Max CO2 Green: {max_co2_green}")
    logger.info(f"Max CO2 Yellow: {max_co2_yellow}, Max CO2 Green: {max_co2_green}")
    
    #hour of day with lowest avg co2 emissions for yellow taxi
    min_avg_co2_yellow_hour = con.execute("""
            SELECT hour_of_day, AVG(trip_co2_kgs) as avg_co2_yellow
            FROM yellow_transform
            GROUP BY hour_of_day
            ORDER BY avg_co2_yellow ASC LIMIT 1
            ; """).fetchone() #limit 1 on asc finds lowest
    
    
    #hour of day with highest avg co2 emissions for yellow taxi
    max_avg_co2_yellow_hour = con.execute("""
            SELECT hour_of_day, AVG(trip_co2_kgs) as avg_co2_yellow
            FROM yellow_transform
            GROUP BY hour_of_day
            ORDER BY avg_co2_yellow DESC LIMIT 1
            ; """).fetchone() #limit 1 on desc finds highest
   
    
    #hour of day with lowest avg co2 emissions for green taxi
    min_avg_co2_green_hour = con.execute("""
            SELECT hour_of_day, AVG(trip_co2_kgs) as avg_co2_green
            FROM green_transform
            GROUP BY hour_of_day
            ORDER BY avg_co2_green ASC LIMIT 1
            ; """).fetchone()
    
    
    #hour of day with highest avg co2 emissions for green taxi
    max_avg_co2_green_hour = con.execute("""
            SELECT hour_of_day, AVG(trip_co2_kgs) as avg_co2_green
            FROM green_transform
            GROUP BY hour_of_day
            ORDER BY avg_co2_green DESC LIMIT 1
            ;
            """).fetchone()
  
    
    
    print(f"Min Avg CO2 Yellow Hour: {min_avg_co2_yellow_hour}, Max Avg CO2 Yellow Hour: {max_avg_co2_yellow_hour}")
    print(f"Min Avg CO2 Green Hour: {min_avg_co2_green_hour}, Max Avg CO2 Green Hour: {max_avg_co2_green_hour}")
    logger.info(f"Min Avg CO2 Yellow Hour: {min_avg_co2_yellow_hour}, Max Avg CO2 Yellow Hour: {max_avg_co2_yellow_hour}")
    logger.info(f"Min Avg CO2 Green Hour: {min_avg_co2_green_hour}, Max Avg CO2 Green Hour: {max_avg_co2_green_hour}")

    #day of week with lowest avg co2 emissions for yellow taxi
    min_avg_co2_yellow_day=con.execute("""
            SELECT day_of_week, AVG(trip_co2_kgs) as avg_co2_yellow
            FROM yellow_transform
            GROUP BY day_of_week
            ORDER BY avg_co2_yellow ASC LIMIT 1
            ; """).fetchone()
    
    #day of week with highest avg co2 emissions for yellow taxi
    max_avg_co2_yellow_day=con.execute("""            
            SELECT day_of_week,
            AVG(trip_co2_kgs) as avg_co2_yellow
            FROM yellow_transform
            GROUP BY day_of_week
            ORDER BY avg_co2_yellow DESC LIMIT 1
            ; """).fetchone()

    #day of week with lowest avg co2 emissions for green taxi
    min_avg_co2_green_day=con.execute("""      
            SELECT day_of_week, AVG(trip_co2_kgs) as avg_co2_green
            FROM green_transform
            GROUP BY day_of_week
            ORDER BY avg_co2_green ASC LIMIT 1
            ; """).fetchone()
   

    #day of week with highest avg co2 emissions for green taxi            
    max_avg_co2_green_day=con.execute("""
            SELECT day_of_week, AVG(trip_co2_kgs) as avg_co2_green
            FROM green_transform
            GROUP BY day_of_week
            ORDER BY avg_co2_green DESC LIMIT 1
            ;
            """).fetchone()
    
    
    print(f"Min Avg CO2 Yellow Day: {min_avg_co2_yellow_day}, Max Avg CO2 Yellow Day: {max_avg_co2_yellow_day}")
    print(f"Min Avg CO2 Green Day: {min_avg_co2_green_day}, Max Avg CO2 Green Day: {max_avg_co2_green_day}")
    logger.info(f"Min Avg CO2 Yellow Day: {min_avg_co2_yellow_day}, Max Avg CO2 Yellow Day: {max_avg_co2_yellow_day}")
    logger.info(f"Min Avg CO2 Green Day: {min_avg_co2_green_day}, Max Avg CO2 Green Day: {max_avg_co2_green_day}")

    #week of year with lowest avg co2 emissions for yellow taxi
    min_avg_co2_yellow_week=con.execute("""
                SELECT week_of_year, AVG(trip_co2_kgs) as avg_co2_yellow
                FROM yellow_transform
                GROUP BY week_of_year
                ORDER BY avg_co2_yellow ASC  LIMIT 1
                ;""").fetchone()

    #week of year with highest avg co2 emissions for yellow taxi
    max_avg_co2_yellow_week=con.execute("""
                SELECT week_of_year, AVG(trip_co2_kgs) as avg_co2_yellow
                FROM yellow_transform
                GROUP BY week_of_year
                ORDER BY avg_co2_yellow DESC LIMIT 1
                ;""").fetchone()

    #week of year with lowest avg co2 emissions for green taxi
    min_avg_co2_green_week=con.execute("""
                SELECT week_of_year, AVG(trip_co2_kgs) as avg_co2_green
                FROM green_transform
                GROUP BY week_of_year
                ORDER BY avg_co2_green ASC LIMIT 1
                ;""").fetchone()

    #week of year with highest avg co2 emissions for green taxi
    max_avg_co2_green_week=con.execute("""
                SELECT week_of_year, AVG(trip_co2_kgs) as avg_co2_green 
                FROM green_transform
                GROUP BY week_of_year
                ORDER BY avg_co2_green DESC LIMIT 1
                ;
                """).fetchone()
    
   
    print(f"Min Avg CO2 Yellow Week: {min_avg_co2_yellow_week}, Max Avg CO2 Yellow Week: {max_avg_co2_yellow_week}")
    print(f"Min Avg CO2 Green Week: {min_avg_co2_green_week}, Max Avg CO2 Green Week: {max_avg_co2_green_week}")
    logger.info(f"Min Avg CO2 Yellow Week: {min_avg_co2_yellow_week}, Max Avg CO2 Yellow Week: {max_avg_co2_yellow_week}")
    logger.info(f"Min Avg CO2 Green Week: {min_avg_co2_green_week}, Max Avg CO2 Green Week: {max_avg_co2_green_week}")

    #month of year with lowest avg co2 emissions for yellow taxi
    min_avg_co2_yellow_month=con.execute("""
            SELECT month_of_year, AVG(trip_co2_kgs) as avg_co2_yellow
            FROM yellow_transform
            GROUP BY month_of_year
            ORDER BY avg_co2_yellow ASC LIMIT 1
            ;""").fetchone()

    #month of year with highest avg co2 emissions for yellow taxi
    max_avg_co2_yellow_month=con.execute("""   
            SELECT month_of_year, AVG(trip_co2_kgs) as avg_co2_yellow
            FROM yellow_transform
            GROUP BY month_of_year
            ORDER BY avg_co2_yellow DESC LIMIT 1
            ;""").fetchone()

    #month of year with lowest avg co2 emissions for green taxi        
    min_avg_co2_green_month=con.execute("""
            SELECT month_of_year, AVG(trip_co2_kgs) as avg_co2_green
            FROM green_transform
            GROUP BY month_of_year
            ORDER BY avg_co2_green ASC LIMIT 1
            ; """).fetchone()

    #month of year with highest avg co2 emissions for green taxi        
    max_avg_co2_green_month=con.execute("""
            SELECT month_of_year, AVG(trip_co2_kgs) as avg_co2_green
            FROM green_transform
            GROUP BY month_of_year
            ORDER BY avg_co2_green DESC LIMIT 1
            ;
            """).fetchone()
    
    print(f"Min Avg CO2 Yellow Month: {min_avg_co2_yellow_month}, Max Avg CO2 Yellow Month: {max_avg_co2_yellow_month}")
    print(f"Min Avg CO2 Green Month: {min_avg_co2_green_month}, Max Avg CO2 Green Month: {max_avg_co2_green_month}")
    logger.info(f"Min Avg CO2 Yellow Month: {min_avg_co2_yellow_month}, Max Avg CO2 Yellow Month: {max_avg_co2_yellow_month}")
    logger.info(f"Min Avg CO2 Green Month: {min_avg_co2_green_month}, Max Avg CO2 Green Month: {max_avg_co2_green_month}")



    # Visualization: Average CO2 emissions by hour of day for both taxi types
    
    yellow_data = con.execute("""
        SELECT month_of_year, SUM(trip_co2_kgs) as total_co2_yellow
        FROM yellow_transform
        GROUP BY month_of_year
        ORDER BY month_of_year;
        """).fetchall()
    green_data = con.execute("""
        SELECT month_of_year, SUM(trip_co2_kgs) as total_co2_green
        FROM green_transform
        GROUP BY month_of_year
        ORDER BY month_of_year;
        """).fetchall()
    



    #create dataframes and sort by month
    yellow_df = pd.DataFrame(yellow_data, columns=['month_of_year', 'total_co2_yellow'])
    green_df = pd.DataFrame(green_data, columns=['month_of_year', 'total_co2_green'])

    #2 plots in one figure
    fig,axs = plt.subplots(1, 2, figsize=(16, 5))
    axs[0].bar(yellow_df['month_of_year'], yellow_df['total_co2_yellow'], color='blue')
    axs[0].set_title('Yellow Taxi Total CO2 Emissions by Month')
    axs[0].set_xlabel('Month of Year')
    axs[0].set_ylabel('Total CO2 Emissions (kgs)')
    axs[0].set_xticks(range(1,13))
    axs[1].bar(green_df['month_of_year'], green_df['total_co2_green'], color='red')
    axs[1].set_title('Green Taxi Total CO2 Emissions by Month')
    axs[1].set_xlabel('Month of Year')
    axs[1].set_ylabel('Total CO2 Emissions (kgs)')
    axs[1].set_xticks(range(1,13))
    plt.tight_layout()
    plt.savefig('co2_by_month.png')
    plt.show()

    con.close()

if __name__ == "__main__":
    run_analysis()

