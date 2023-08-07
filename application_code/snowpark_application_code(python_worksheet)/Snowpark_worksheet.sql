#This worksheet is part 2 of the quickstart. In part1, we have written pyspark code to perform analysis on uber dataset.
#In this worksheet, we will utilize same dataset and perform same analysis using snowpark python.

#import required libraries
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
from snowflake.snowpark import functions as F
from snowflake.snowpark import Window
import pandas as pd
import numpy as np

#The snowflake parameters:
SNOWFLAKE_CONN_PROFILE = {
    "database": "snowpark",
    "schema": "dev",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
}

def main(session: snowpark.Session): 

    session.sql("use role Accountadmin").collect()
    session.sql("create database if not exists  {}".format(SNOWFLAKE_CONN_PROFILE['database'])).collect()
    session.sql("use database {}".format(SNOWFLAKE_CONN_PROFILE['database'])).collect()
    session.sql("create schema if not exists  {}".format(SNOWFLAKE_CONN_PROFILE['schema'])).collect()
    session.sql("use schema {}".format(SNOWFLAKE_CONN_PROFILE['schema'])).collect()
    session.sql("use warehouse {}".format(SNOWFLAKE_CONN_PROFILE['warehouse']))
    print(session.sql('select current_warehouse(), current_database(), current_schema(), current_role()').collect())

#In the following step, we are retrieving the Uber data from the 'Uber' table that was created during the data migration process and loading it into a dataframe.
    df = session.sql('select * from uber')
    df.schema
    print("uber dataframe:")
    df.show()
    uber=data_cleaning(df)

    uber['DATE']=uber['DATE'].dt.tz_localize('UTC')
    uber['DATE_TIME']=uber['DATE_TIME'].dt.tz_localize('UTC')
    uber_df=session.create_dataframe(uber)
    uber_df.show()
    uber_df1=uber_df.select(to_date(col('DATE')).alias('DATE'),"TIME","EYEBALLS","ZEROES","COMPLETED_TRIPS","REQUESTS","UNIQUE_DRIVERS","DATE_TIME")
    uber_df1.show()
    max_trips=max_trips_date(uber_df1)
    print("Date that had the most completed trips during the two-week period")
    max_trips.show()
    highest_trips = highest_completed_trips(uber_df1)
    print("Highest number of completed trips within a 24-hour period")
    print(highest_trips)
    most_requests = most_requests_per_hour(uber_df1)
    print("Hour of the day had the most requests during the two-week period")
    print(most_requests)
    per_zeroes = percentage_zeroes(uber_df1)
    print("Percentages of all zeroes during the two-week period occurred on weekend (Saturday and Sunday)")
    print(per_zeroes)
    weighted_avg = weighted_avg_ratio(uber_df1)
    print("Weighted average ratio of completed trips per driver during the two-week period")
    weighted_avg.show()
    busiest_8_hours = busiest_hours(uber_df1)
    print("Busiest 8 consecutive hours over the two-week period in terms of unique request")
    busiest_8_hours.show()
    zeroes_to_eyeballs = zeroes_to_eyeballs_ratio(uber_df1)
    print("Highest ratio of Zeroes to Eyeballs ")
    zeroes_to_eyeballs.show()
    requests_driver = requests_per_driver(uber_df1)
    print("Requests per driver")
    requests_driver.show()
    true_endday = true_end_day(uber_df1)
    print("True End Day")
    true_endday.show()
    return max_trips

# We can use Pandas on top of snowpark for example, we can convert the 'Date' column to datetime format. 
# The function 'data_cleaning' creates an additional column called 'DATE_TIME'. Since the 'Date' column is initially a string type,
# it is necessary to convert it to datetime format for further analysis. The 'DATE_TIME' column is created by combining the 'Date' and 'Time' columns.
# The data_cleaning function takes an argument,dataframe that contains the uber data.

def data_cleaning(dataframe):
    df1=dataframe.with_column_renamed(col("Time (Local)"), "Time")
    df2=df1.with_column_renamed(col("Completed Trips"), "Completed_Trips")
    df3=df2.with_column_renamed(col("Unique Drivers"), "Unique_Drivers")
    # df3.show()
    uber_pd = df3.toPandas()
    uber_pd['DATE_TIME'] = uber_pd['DATE'].astype('str')+' '+uber_pd['TIME'].astype('str')
    uber_pd['DATE_TIME']=pd.to_datetime(uber_pd['DATE_TIME'], format='%d-%b-%y %H')
    uber_pd['DATE']=pd.to_datetime(uber_pd['DATE'], format='%d-%b-%y')
    uber_pd.head()
    print(" cleaned uber dataframe:")
    return uber_pd

#  1. Which date had the most completed trips during the two-week period?
def max_trips_date(dataframe):
    trips_per_date = dataframe.groupBy('DATE').agg(sum('COMPLETED_TRIPS').alias('total_completed_trips')).sort("total_completed_trips", ascending=False)
    trips_per_date=trips_per_date.select('Date').limit(1)
    return trips_per_date

# 2. What was the highest number of completed trips within a 24-hour period?
def highest_completed_trips(dataframe):
    # Group the data by 24-hour window and sum the completed trips
    completed_trips_24hrs = dataframe.groupBy(F.date_trunc('day', 'DATE_TIME'),floor(hour('DATE_TIME')/24)) \
                                   .agg(F.sum("completed_Trips").alias("Total_Completed_Trips"))

    # Get the highest number of completed trips within a 24-hour period
    highest_completed_trips_in_24_hours =completed_trips_24hrs.select(max('Total_Completed_Trips').alias('Total_Completed_Trips_in_24hrs')).first()[0]
    return highest_completed_trips_in_24_hours
#  3. Which hour of the day had the most requests during the two-week period?

def most_requests_per_hour(dataframe):
    df_hour = dataframe.groupBy("TIME").agg(sum('REQUESTS').alias('Total_Requests')).sort("Total_Requests", ascending=False)
    most_req_hr = df_hour.select('TIME').first()[0]
    return most_req_hr

# 4. What percentages of all zeroes during the two-week period occurred on weekend (Saturday and Sunday)

#In snowpark, satuarday day of week is 6 and Sunday day of week is 0
def percentage_zeroes(dataframe):
    # count number of zeros that occurred on weekends
    weekend_df = dataframe.withColumn("is_weekend", when(dayofweek(col("DATE_TIME")).isin([0,6]), 1).otherwise(0))
    weekend_zeroes = weekend_df.filter(col("is_weekend") == 1).agg(sum('ZEROES').alias('WEEKEND_ZEROES')).select('WEEKEND_ZEROES').first()["WEEKEND_ZEROES"]

    # total number of zeros
    total_zeroes = dataframe.agg(sum("ZEROES").alias('TOTAL_ZEROES')).collect()[0]['TOTAL_ZEROES']
    per_zero=(weekend_zeroes/total_zeroes)*100
    return per_zero

# 5. What is the weighted average ratio of completed trips per driver during the two-week period? Tip: "Weighted average" means your answer should account for the total trip volume in each hour to determine the most accurate number in the whole period.
from snowflake.snowpark.functions import floor
def weighted_avg_ratio(dataframe):
    weighted_avg_ratio = dataframe.withColumn("trips_per_driver", when(dataframe["Unique_Drivers"]==0, 1).otherwise(dataframe["Completed_Trips"] / dataframe["Unique_Drivers"])) \
                 .groupBy("DATE", "Time") \
                 .agg(avg("trips_per_driver").alias("avg_completed_per_driver"), sum("Completed_trips").alias("total_completed_trips")) \
                 .withColumn("weighted_ratio", col("avg_completed_per_driver") * col("total_completed_trips")) \
                 .agg(sum("weighted_ratio") / sum("total_completed_trips"))
    war=weighted_avg_ratio.with_column_renamed(col("DIVIDE(SUM(WEIGHTED_RATIO), SUM(TOTAL_COMPLETED_TRIPS))"), "Weighted_average")\
                          .select(trunc(col("Weighted_average"), lit(3)).alias('Weighted_average'))
    return war

# 6. In drafting a driver schedule in terms of 8 hours shifts, when are the busiest 8 consecutive hours over the two-week period in terms of unique requests? A new shift starts every 8 hours. Assume that a driver will work the same shift each day.
def busiest_hours(dataframe):
    requests_per_hour = dataframe.groupBy('Time').agg(countDistinct('Requests').alias('Total_Requests'))
    window_8hr = Window.orderBy(col('Total_Requests').desc()).rowsBetween(0,7)

    busiest_8_hrs = requests_per_hour.select('*', sum('Total_Requests').over(window_8hr).alias("sum_8_hrs"))\
                                 .orderBy(col("sum_8_hrs").desc()).limit(1)
    return busiest_8_hrs
    
# 7. In which 72-hour period is the ratio of Zeroes to Eyeballs the highest?
def zeroes_to_eyeballs_ratio(dataframe):
    # Group the data by 72-hour periods and calculate the ratio of zeroes to eyeballs for each period
    period_ratios = (dataframe.groupBy((hour(col("DATE_TIME")) / (72*3600)).cast("int"))\
                            .agg(sum("Zeroes").alias("zeroes"), sum("Eyeballs").alias("eyeballs"))\
                            .withColumn("ratio", col("zeroes") / col("eyeballs")))

    # Find the period with the highest ratio
    highest_ratio_period = period_ratios.orderBy(col("ratio").desc()).limit(1)
    hrp=highest_ratio_period.with_column_renamed(col("CAST(DIVIDE(HOUR(DATE_TIME), LITERAL()))"), "PERIOD")
    return hrp
# 8. If you could add 5 drivers to any single hour of every day during the two-week period, which hour should you add them to? Hint: Consider both rider eyeballs and driver supply when choosing
def requests_per_driver(dataframe):
    requests_per_driver = (dataframe.groupBy('Time')\
                         .agg((sum('Requests') / countDistinct('Unique_Drivers')).alias('requests_per_driver')))

    requests_per_driver= requests_per_driver.sort('requests_per_driver', ascending=False).limit(1)
    return requests_per_driver

# 9. Looking at the data from all two weeks, which time might make the most sense to consider a true "end day" instead of midnight? (i.e when are supply and demand at both their natural minimums)
def true_end_day(dataframe):
    true_end_day = dataframe.groupBy('Time')\
       .agg(avg('Completed_Trips').alias('avg_requests'), avg('Unique_Drivers').alias('avg_unique_drivers'))\
       .orderBy('avg_requests', 'avg_unique_drivers').limit(1)
    return true_end_day



