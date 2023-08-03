# In this worksheet we aim to test the snowpark usecases using the test dataset which we migrated from databricks to the snowpark database
# Requirement: Upload the uber_worksheet of the quickstart usecases into the internal stage and add the path to the staged packages.

#Here we will be doing the functional equivalence testing with the test data that contains the first 3 days data from the uber dataset

import snowflake.snowpark as snowpark

#Importing the snowpark worksheet that contains all the functions for the usecases.

from uber_worksheet import *

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

#The test data is migrated from databricks to test_data table in snowflake.
#Next the test data is read into the dataframe from the table for the further testing.  
    test_df = session.sql('select * from test_data')
    test_df.schema
    print(test_df.count())
#Function Data cleaning is present in the snowpark file that does the preliminary data cleaning such as data type changes and column renaming.  
    cleaned_df=data_cleaning(test_df)
    print(cleaned_df)
    cleaned_df['DATE']=cleaned_df['DATE'].dt.tz_localize('UTC')
    cleaned_df['DATE_TIME']=cleaned_df['DATE_TIME'].dt.tz_localize('UTC')
    uber_df=session.create_dataframe(cleaned_df)
    uber_df.show()

    cleaned_uber=uber_df.select(to_date(col('DATE')).alias('DATE'),"TIME","EYEBALLS","ZEROES","COMPLETED_TRIPS","REQUESTS","UNIQUE_DRIVERS","DATE_TIME")
    cleaned_uber.show()

    output1_df = session.sql('select * from output1')
    output1_df.schema
    output2_df = session.sql('select * from output2')
    output2_df.schema

    output3_df = session.sql('select * from output3')
    output3_df.show()

    output4_df = session.sql('select * from output4')
    output4_df.show()
    output5_df = session.sql('select * from output5')
    output5_df.show()
    output6_df = session.sql('select * from output6')
    output6_df.show()
    uber(cleaned_uber,output1_df,output2_df,output3_df,output4_df,output5_df,output6_df)
    return test_df
    

def uber(cleaned_uber,output1_df,output2_df,output3_df,output4_df,output5_df,output6_df):
    print("max trips:")
    max_trips=test_max_trips_date(cleaned_uber,output1_df)
    print(max_trips)
    print("higest completed trips:")
    high_completed=test_highest_completed_trips(cleaned_uber)  
    print(high_completed)
    print("most request per hour:")
    most_request=test_most_requests_per_hour(cleaned_uber)
    print(most_request)
    print("weighted avg ratio:")
    w_vg_ratio=test_weighted_avg_ratio(cleaned_uber,output2_df)
    print(w_vg_ratio)
    print("max busiest hour:")
    bus_hrs=test_busiest_hours(cleaned_uber,output3_df)
    print(bus_hrs)
    print("zeroes to eyeballs ratio:")
    zero_to_eyeball=test_zeroes_to_eyeballs_ratio(cleaned_uber,output4_df)
    print(zero_to_eyeball)
    print("requests per driver:")
    req_driver=test_requests_per_driver(cleaned_uber,output5_df)
    print(req_driver)
    print("true end day:")
    true_end=test_true_end_day(cleaned_uber,output6_df)
    print(true_end)
    
# Functions to compre the actual and expected dataframe:

# compares the Number of row count
def compare_row_count(actual_df, expected_df):
#     Check if the number of rows match
    if actual_df.count() != expected_df.count():
        print("Number of rows do not match: actual={}, expected={}".format(
            actual_df.count(), expected_df.count()))
        return False
    return True
    
# compares the Schema
def compare_schema(actual_df, expected_df):
 # Check if the column names and types match
    if actual_df.schema != expected_df.schema:
        print("Schema does not match: actual={}, expected={}".format(
            actual_df.schema, expected_df.schema))
        return False
    return True

# Row by Row comparison
def compare_dataframes(actual_df, expected_df):
  
    if isinstance(expected_df, int):
        if actual_df == expected_df:
            return True
        else:
            #print("Rows of the data frames do not match")
            return False
    else:
        # Comparing the two DataFrames row by row
        actual_df_collect = actual_df.collect()
        expected_df_collect = expected_df.collect()

        if actual_df_collect == expected_df_collect:

            return True
        else:
            #print("Rows of the data frames do not match")
            return False

#The below function test the max_trips_date function with sample data file(test data ) and the expected output is present in the output1 table in snowflake .
#The function calls the function defined in the quickstart_worksheet for validation and compares if the actual and expected output match
def test_max_trips_date(cleaned_uber,output1_df):
    output_df = max_trips_date(cleaned_uber)
    output_df.show()
    # print(output_df.schema)
    # Define expected output data
    expected_data = output1_df
    expected_data.show()
    if compare_row_count(output_df, expected_data):
        print("Row count of the actual_df and expected_df match")
    else:
        print("Row count of the actual_df and expected_df do not match")
        return False
    if compare_schema(output_df, expected_data):
        print("Schema of the actual_df and expected_df match")
    else:
        print("Schema of the actual_df and expected_df do not match")
        return False
    if compare_dataframes(output_df, expected_data):
        return "Dataframes are equivalent!"
    else:
        return "Dataframes are not equivalent"
        
#The below function test the highest_completed_trip function with sample data file(test data ). 
#The function calls the function defined in the quickstart_worksheet for validation and compares if the actual and expected output match
def test_highest_completed_trips(cleaned_uber):
    output_df = highest_completed_trips(cleaned_uber)
    print("output value",output_df)
    expected_data = 248
    if compare_dataframes(output_df, expected_data):
        return "Dataframes are equivalent!"
    else:
        return "Dataframes are not equivalent"

#The below function test the most_requests_per_hour function with sample data file(test data ). 
#The function calls the function defined in the quickstart_worksheet for validation and compares if the actual and expected output match
def test_most_requests_per_hour(cleaned_uber):
    output_df = most_requests_per_hour(cleaned_uber)
    print("output value",output_df)
    expected_data = 0
#     print(type(expected_data))
#     # display(expected_data)
    if compare_dataframes(output_df, expected_data):
        return "Dataframes are equivalent!"
    else:
        return "Dataframes are not equivalent"

#The below function test the weighted_avg_ratio function with sample data file(test data ). 
#The function calls the function defined in the quickstart_worksheet for validation and compares if the actual and expected output match
def test_weighted_avg_ratio(cleaned_uber,output2_df):
    output = weighted_avg_ratio(cleaned_uber)
#     print(output.schema)
    output2=output.select(trunc(col("Weighted_average"), lit(2)).alias('Weighted_average'))
#     output2.show()
#     print(output2.schema)
    output_df = output2.withColumn("Weighted_average",output2.Weighted_average.cast(DoubleType()))
    output_df.show()
    expected_data=output2_df.select(trunc(col("Weighted_average"), lit(2)).alias('Weighted_average'))
    expected_data.show() 
#     print(expected_data.schema)
    if compare_schema(output_df, expected_data):
        print("Schema of the actual_df and expected_df match")
    else:
        print("Schema of the actual_df and expected_df do not match")
        return False
    if compare_dataframes(output_df, expected_data):
        return "Dataframes are equivalent!"
    else:
        return "Dataframes are not equivalent"

#The below function test the busiest_hours function with sample data file(test data ). 
#The function calls the function defined in the quickstart_worksheet for validation and compares if the actual and expected output match
def test_busiest_hours(cleaned_uber,output3_df):
    output_df = busiest_hours(cleaned_uber)
    output_df.show()
    print(output_df.schema)
    # Define expected output data
    expected_data=output3_df
#         .select(trunc(col("Weighted_average"), lit(2)).alias('Weighted_average'))
    expected_data.show() 
    print(expected_data.schema)
    if compare_row_count(output_df, expected_data):
        print("Row count of the actual_df and expected_df match")
    else:
        print("Row count of the actual_df and expected_df do not match")
        return False
    if compare_dataframes(output_df, expected_data):
        return "Dataframes are equivalent!"
    else:
        return "Dataframes are not equivalent"

#The below function test the zeroes_to_eyeballs_ratio function with sample data file(test data ). 
#The function calls the function defined in the quickstart_worksheet for validation and compares if the actual and expected output match
def test_zeroes_to_eyeballs_ratio(cleaned_uber,output4_df):
    output4 = zeroes_to_eyeballs_ratio(cleaned_uber)
#     output4.show()
    output_df = output4.withColumn("RATIO",output4.RATIO.cast(DoubleType()))
    output_df.show()
    expected_data =output4_df
    expected_data.show()
    if compare_row_count(output_df, expected_data):
        print("Row count of the actual_df and expected_df match")
    else:
        print("Row count of the actual_df and expected_df do not match")
        return False
    if compare_schema(output_df, expected_data):
        print("Schema of the actual_df and expected_df match")
    else:
        print("Schema of the actual_df and expected_df do not match")
        return False
    if compare_dataframes(output_df, expected_data):
        return "Dataframes are equivalent!"
    else:
        return "Dataframes are not equivalent"

#The below function test the requests_per_driver function with sample data file(test data ). 
#The function calls the function defined in the quickstart_worksheet for validation and compares if the actual and expected output match
def test_requests_per_driver(cleaned_uber,output5_df):
    output5 = requests_per_driver(cleaned_uber)
#     output5.show()
    output_df = output5.withColumn("REQUESTS_PER_DRIVER",output5.REQUESTS_PER_DRIVER.cast(DoubleType()))
    output_df.show()
    # Define expected output data
    
    expected_data = output5_df
    if compare_row_count(output_df, expected_data):
        print("Row count of the actual_df and expected_df match")
    else:
        print("Row count of the actual_df and expected_df do not match")
        return False
    if compare_schema(output_df, expected_data):
        print("Schema of the actual_df and expected_df match")
    else:
        print("Schema of the actual_df and expected_df do not match")
        return False
    if compare_dataframes(output_df, expected_data):
        return "Dataframes are equivalent!"
    else:
        return "Dataframes are not equivalent"

#The below function test the true_end_day function with sample data file(test data ). 
#The function calls the function defined in the quickstart_worksheet for validation and compares if the actual and expected output match
def test_true_end_day(cleaned_uber,output6_df):
    output6 = true_end_day(cleaned_uber)
    output_df = output6.withColumn("AVG_REQUESTS",output6.AVG_REQUESTS.cast(DoubleType()))
    output_df = output_df.withColumn("AVG_UNIQUE_DRIVERS", output6_df.AVG_UNIQUE_DRIVERS.cast(LongType()))
    output_df.show()
    # Define expected output data

    expected_data = output6_df
    expected_data.show()
    if compare_row_count(output_df, expected_data):
        print("Row count of the actual_df and expected_df match")
    else:
        print("Row count of the actual_df and expected_df do not match")
        return False
    if compare_schema(output_df, expected_data):
        print("Schema of the actual_df and expected_df match")
    else:
        print("Schema of the actual_df and expected_df do not match")
        return False
    if compare_dataframes(output_df, expected_data):
        return "Dataframes are equivalent!"
    else:
        return "Dataframes are not equivalent"