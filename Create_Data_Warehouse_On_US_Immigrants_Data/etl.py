import pandas as pd, re
from datetime import datetime, timedelta
import configparser

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql import Window
import pyspark

import utils as util_lib

# Read Config file
config = configparser.ConfigParser()
config.read('/home/hadoop/spark.cfg')

# Create Spark Session
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .getOrCreate()
    return spark


# Func to covert Pandas DF to Spark DF after fetching additional Data
# Collect data from custom generated file extracted from I94_SAS_Labels_Description file
def gather_additional_data_after_cleaning(spark):
    try:
        # Covert  Port Details Pandas DF to Spark DF
        ports_pandas_df = util_lib.fetch_port_details()
        ports_spark_df=spark.createDataFrame(ports_pandas_df.astype(str))
        
        # Covert Country Names Pandas DF to Spark DF
        countries_pandas_df = util_lib.fetch_country_names()
        countries_spark_df = spark.createDataFrame(countries_pandas_df.astype(str))
        
        # Covert Visa Type  Pandas DF to Spark DF
        visa_types_pandas_df= util_lib.fetch_visa_type()
        visa_types_spark_df = spark.createDataFrame(visa_types_pandas_df.astype(str))
        
        # Covert Mode  Pandas DF to Spark DF
        mode_details_pandas_df= util_lib.I94_mode_details()
        mode_details_spark_df = spark.createDataFrame(mode_details_pandas_df.astype(str))
        
        # Covert  Month Pandas DF to Spark DF
        month_pandas_df = util_lib.create_month_df()
        month_spark_df = spark.createDataFrame(month_pandas_df.astype(str))
        
        # Return all Dataframes
        return ports_spark_df,countries_spark_df,visa_types_spark_df,mode_details_spark_df,month_spark_df
        
        
    except Exception as e:
        print(str(e))

# Func to clean Temprature Data
def temprature_data_cleaning(spark,temprature_df):
    try:
        
        # Filter data only for United States as Country as relative Immigration data is only for United States
        temprature_df = temprature_df.where(f.col("Country") == "United States")
        
        temprature_df.registerTempTable("temp_dfs")
        
        # Fetch the latest temprature and Lat/Long data for each City in United States , Remove Redundancies
        temprature_dfs = spark.sql("""select  sub_a.dt,sub_a.City,a.Latitude,a.Longitude,a.AverageTemperature,a.AverageTemperatureUncertainty  from temp_dfs as a join (select max(dt) as dt,City  from temp_dfs group by City) as sub_a on a.dt==sub_a.dt and a.City=sub_a.City order by a.City,a.dt desc """)
        
        # Drop Column "dt" as it is not required
        temprature_df = temprature_dfs.dropDuplicates(["City"]).drop("dt")
        
        # Type Cast Temprature datafrom String to Float
        temprature_df = temprature_df.withColumn("average_temperature",temprature_dfs.AverageTemperature.cast("float")).withColumn("average_temperature_uncertainty",temprature_dfs.AverageTemperatureUncertainty.cast("float")).drop("AverageTemperature","AverageTemperatureUncertainty")
        
        
        return temprature_df
           
    except Exception as e:
        print(str(e))

# Func to Convert SAS datetime to Python datetime
def to_date_time(x):
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None

# Initialize  UDF to convert SAS
udf_to_date_time_sas = udf(lambda x: to_date_time(x), DateType())


# Func to convert in  datetime format mmddYYYY
def to_date_time_mm_dd_yy(x):
    try:
        return datetime.strptime(x, '%m%d%Y')
    except:
        return None

# Initialize UDF
udf_to_date_time_mm_yy_dd = udf(lambda x: to_date_time_mm_dd_yy(x), DateType())


# Func to convert in  datetime format YYYYmmdd
def to_date_time_yy_mm_dd(x):
    try:
        return datetime.strptime(x, '%Y%m%d')
    except:
        return None

# Initialize UDF
udf_to_date_time_yy_mm_dd = udf(lambda x: to_date_time_yy_mm_dd(x), DateType())

# Func to Read, Clean & Model Immigration data into Immigration Fact
def immigration_data_cleaning(spark,data_file_path,ports_spark_df):
    try:
        # Read Immigration data from file
        immigration_spark_df = spark.read.format('com.github.saurfang.sas.spark').load(data_file_path)
        
        # Create port list from Port Dimention Data
        port_list = [row['port_code'] for row in ports_spark_df.collect()]
        
        # Remove entries where i94port is invalid
        immigration_spark_df = immigration_spark_df.filter(immigration_spark_df.i94port.isin(port_list))
        
        # Remove entries with no iport94 code
        immigration_spark_df=immigration_spark_df.filter(immigration_spark_df.i94port != 'null')
        
        # Type cast variables and drop redundant columns
        immigration_spark_df=immigration_spark_df \
        .withColumn("travel_mode_code",immigration_spark_df.i94mode.cast("int")) \
        .withColumn("visa_code",immigration_spark_df.i94visa.cast("int")) \
        .withColumn("month",immigration_spark_df.i94mon.cast("int")) \
        .withColumn("year",immigration_spark_df.i94yr.cast("int")) \
        .withColumn("age",immigration_spark_df.i94bir.cast("int")) \
        .withColumn("birth_year",immigration_spark_df.biryear.cast("int")) \
        .withColumn("city_code",immigration_spark_df.i94cit.cast("int")) \
        .withColumn("country_code",immigration_spark_df.i94res.cast("int")) \
        .withColumn("cic_id",immigration_spark_df.cicid.cast("int")) \
        .withColumn("count",f.col("count").cast("int")) \
        .withColumn("arrival_date", udf_to_date_time_sas("arrdate"))\
        .withColumn("departure_date", udf_to_date_time_sas("depdate"))\
        .withColumn("date_added_i_94_files", udf_to_date_time_yy_mm_dd("dtadfile"))\
        .withColumn("date_till_allowed_to_stay", udf_to_date_time_mm_yy_dd("dtaddto"))\
        .withColumnRenamed("i94port","port_code") \
        .withColumnRenamed("i94addr","state_code") \
        .withColumnRenamed("visapost","visa_post") \
        .withColumnRenamed("occup","occupation") \
        .withColumnRenamed("fltno","flight_number") \
        .withColumnRenamed("admnum","adm_num") \
        .drop("i94mode","i94visa","i94mon","i94yr","i94bir","biryear","i94cit","i94res","depdate","arrdate","cicid","dtadfile","dtaddto","entdepa","entdepd","entdepu","matflag")

        return immigration_spark_df
        
        
    except Exception as e:
        print(str(e))
        

def create_city_dimention(spark,cities_fact_df,temprature_df):
    try:
        # Select distinct cities
        cities_dims_df  = cities_fact_df.select("City","State","State Code").distinct()
        
        # Assign unique id to each city as city_code
        cities_window = Window.orderBy(f.col("City"),f.col("State"))
        cities_dims_df = cities_dims_df.withColumn("city_code",f.row_number().over(cities_window)).withColumnRenamed("State Code","state_code")
        
        #Joining with Temprature DF to get Lat ,Long & Temp
        cities_dims_df = cities_dims_df.join(temprature_df ,on = ['City'] , how = 'left')
        
        return cities_dims_df
        
    except Exception as e:
        print(str(e))

def create_race_dimention(cities_fact_df):
    try:
        # Select distinct race types
        race_dim_df = cities_fact_df.select("Race").distinct()
        
        # Assign unique id for each race type as race_type_id
        race_window = Window.orderBy(f.col("Race"))
        race_dim_df = race_dim_df.withColumn("race_type_id",f.row_number().over(race_window))
        
        return race_dim_df
        
    except Exception as e:
        print(str(e))

        
def create_city_demographics_fact(cities_fact_df,race_dim_df,cities_dims_df):
    try:
        # Join with Race Dimention to get race_type_id
        city_fact_df = cities_fact_df.join(race_dim_df,on = ["Race"],how = "inner")
        
        city_fact_df = city_fact_df.withColumnRenamed("State Code","state_code")
        
        #Join with cities_dimention to get unique city_code
        city_fact_df = city_fact_df.join(cities_dims_df ,on = ['City','state_code'] , how = "inner")
        
        drop_column_list = ["City","Race","State","Total Population","Male Population","Female Population","Number of Veterans","Foreign-born","Median Age","Average Household Size","Count"]
        
        # Perform type cast and drop redundant columns
        city_fact_df = city_fact_df.withColumn("race_count",f.col("Count").cast("int")) \
                                    .withColumn("total_population",f.col("Total Population").cast("int")) \
                                    .withColumn("male_population",f.col("Male Population").cast("int")) \
                                    .withColumn("female_population",f.col("Female Population").cast("int")) \
                                    .withColumn("number_of_veterans",f.col("Number of Veterans").cast("int")) \
                                    .withColumn("foreign_born",f.col("Foreign-born").cast("int")) \
                                    .withColumn("median_age",f.col("Median Age").cast("float")) \
                                    .withColumn("average_household_size",f.col("Average Household Size").cast("int")) \
                                    .drop(*drop_column_list)
        
        city_fact_df = city_fact_df.select("city_code","state_code","total_population","male_population","female_population","number_of_veterans","median_age","foreign_born","average_household_size","race_type_id","race_count")
        
        # Find distinct values to feed city_race_dimention table
        city_race_dimention = city_fact_df.select("city_code","race_type_id","race_count").distinct()
        
        # Drop Duplicates which have same values except ("race_type_id" & "race_count")
        city_fact_df = city_fact_df.dropDuplicates(["city_code","state_code","total_population","male_population","female_population","number_of_veterans","median_age","foreign_born","average_household_size"])
        
        # Drop columns ("race_type_id" & "race_count") as they are already feeded in city_race_dimention and can be joined with city_code
        city_fact_df = city_fact_df.drop("race_type_id","race_count")
        
        return city_fact_df,city_race_dimention
    
    except Exception as e:
        print(str(e))

        
# Func to create and insert data into Fact Tables
def create_load_dimention_tables(port_code,visa_code,visa_mode,country_code,month_names,cities_dims_df,race_dim_df,city_race_df):
    try:
        # Used Spark-Redshift connector to write data into Redshift
        
        # Create table as (mode = overwrite) and insert into Port Code Dimention
        port_code.write \
        .format("com.databricks.spark.redshift") \
        .option("url", "jdbc:redshift://{}:5439/{}?user={}&password={}".format(config.get('redshift_config','redshift_host'),config.get('redshift_config','redshift_db'),config.get('redshift_config','redshift_user'),config.get('redshift_config','redshift_pass'))) \
        .option("dbtable", config.get('redshift_config','port_code_table')) \
        .option("aws_iam_role", config.get('access_keys','IAM_ROLE'))  \
        .option("tempdir", config.get('redshift_config','redshift_s3_temp_dir')) \
        .option("diststyle","ALL") \
        .option("extracopyoptions", "TRUNCATECOLUMNS") \
        .mode(config.get('redshift_config','full_refresh')) \
        .save()
        
        # Create table as (mode = overwrite) and insert into Visa Code Dimention
        visa_code.write \
        .format("com.databricks.spark.redshift") \
        .option("url", "jdbc:redshift://{}:5439/{}?user={}&password={}".format(config.get('redshift_config','redshift_host'),config.get('redshift_config','redshift_db'),config.get('redshift_config','redshift_user'),config.get('redshift_config','redshift_pass'))) \
        .option("dbtable", config.get('redshift_config','visa_code_table')) \
        .option("aws_iam_role", config.get('access_keys','IAM_ROLE'))  \
        .option("tempdir", config.get('redshift_config','redshift_s3_temp_dir')) \
        .option("diststyle","ALL") \
        .option("extracopyoptions", "TRUNCATECOLUMNS") \
        .mode(config.get('redshift_config','full_refresh')) \
        .save()
        
        # Create table as (mode = overwrite) and insert into Visa Mode Dimention
        visa_mode.write \
        .format("com.databricks.spark.redshift") \
        .option("url", "jdbc:redshift://{}:5439/{}?user={}&password={}".format(config.get('redshift_config','redshift_host'),config.get('redshift_config','redshift_db'),config.get('redshift_config','redshift_user'),config.get('redshift_config','redshift_pass'))) \
        .option("dbtable", config.get('redshift_config','visa_mode_table')) \
        .option("aws_iam_role", config.get('access_keys','IAM_ROLE'))  \
        .option("tempdir", config.get('redshift_config','redshift_s3_temp_dir')) \
        .option("diststyle","ALL") \
        .option("extracopyoptions", "TRUNCATECOLUMNS") \
        .mode(config.get('redshift_config','full_refresh')) \
        .save()
        
        # Create  table as (mode = overwrite) and insert into Counry Dimention
        country_code.write \
        .format("com.databricks.spark.redshift") \
        .option("url", "jdbc:redshift://{}:5439/{}?user={}&password={}".format(config.get('redshift_config','redshift_host'),config.get('redshift_config','redshift_db'),config.get('redshift_config','redshift_user'),config.get('redshift_config','redshift_pass'))) \
        .option("dbtable", config.get('redshift_config','country_code_table')) \
        .option("aws_iam_role", config.get('access_keys','IAM_ROLE'))  \
        .option("tempdir", config.get('redshift_config','redshift_s3_temp_dir')) \
        .option("diststyle","ALL") \
        .option("extracopyoptions", "TRUNCATECOLUMNS") \
        .mode(config.get('redshift_config','full_refresh')) \
        .save()
        
        # Create table as (mode = overwrite) and insert into Month Dimention
        month_names.write \
        .format("com.databricks.spark.redshift") \
        .option("url", "jdbc:redshift://{}:5439/{}?user={}&password={}".format(config.get('redshift_config','redshift_host'),config.get('redshift_config','redshift_db'),config.get('redshift_config','redshift_user'),config.get('redshift_config','redshift_pass'))) \
        .option("dbtable", config.get('redshift_config','month_names_table')) \
        .option("aws_iam_role", config.get('access_keys','IAM_ROLE'))  \
        .option("tempdir", config.get('redshift_config','redshift_s3_temp_dir')) \
        .option("diststyle","ALL") \
        .option("extracopyoptions", "TRUNCATECOLUMNS") \
        .mode(config.get('redshift_config','full_refresh')) \
        .save()
        
        # Create table as (mode = overwrite) and insert into  Cities Dimention
        cities_dims_df.write \
        .format("com.databricks.spark.redshift") \
        .option("url", "jdbc:redshift://{}:5439/{}?user={}&password={}".format(config.get('redshift_config','redshift_host'),config.get('redshift_config','redshift_db'),config.get('redshift_config','redshift_user'),config.get('redshift_config','redshift_pass'))) \
        .option("dbtable", config.get('redshift_config','city_table')) \
        .option("aws_iam_role", config.get('access_keys','IAM_ROLE'))  \
        .option("tempdir", config.get('redshift_config','redshift_s3_temp_dir')) \
        .option("diststyle","ALL") \
        .option("extracopyoptions", "TRUNCATECOLUMNS") \
        .mode(config.get('redshift_config','full_refresh')) \
        .save()
        
        # Create table as (mode = overwrite) and insert into Race Dimention
        race_dim_df.write \
        .format("com.databricks.spark.redshift") \
        .option("url", "jdbc:redshift://{}:5439/{}?user={}&password={}".format(config.get('redshift_config','redshift_host'),config.get('redshift_config','redshift_db'),config.get('redshift_config','redshift_user'),config.get('redshift_config','redshift_pass'))) \
        .option("dbtable", config.get('redshift_config','race_table')) \
        .option("aws_iam_role", config.get('access_keys','IAM_ROLE'))  \
        .option("tempdir", config.get('redshift_config','redshift_s3_temp_dir')) \
        .option("diststyle","ALL") \
        .option("extracopyoptions", "TRUNCATECOLUMNS") \
        .mode(config.get('redshift_config','full_refresh')) \
        .save()
        
        
        # Create table as (mode = overwrite) and insert into City_Race Dimention
        city_race_df.write \
        .format("com.databricks.spark.redshift") \
        .option("url", "jdbc:redshift://{}:5439/{}?user={}&password={}".format(config.get('redshift_config','redshift_host'),config.get('redshift_config','redshift_db'),config.get('redshift_config','redshift_user'),config.get('redshift_config','redshift_pass'))) \
        .option("dbtable", config.get('redshift_config','city_race_table')) \
        .option("aws_iam_role", config.get('access_keys','IAM_ROLE'))  \
        .option("tempdir", config.get('redshift_config','redshift_s3_temp_dir')) \
        .option("diststyle","ALL") \
        .option("extracopyoptions", "TRUNCATECOLUMNS") \
        .mode(config.get('redshift_config','full_refresh')) \
        .save()
        
    except Exception as e:
        print(str(e))
    
# Func to create Fact Tables and insert data into fact tables
def create_load_fact_tables(imm_df,city_fact_df):
    try:
        # Used Spark-Redshift connector to write data into Redshift
        
        # Create table as (mode = overwrite) and Insert into Immigration Fact 
        imm_df.write \
        .format("com.databricks.spark.redshift") \
        .option("url", "jdbc:redshift://{}:5439/{}?user={}&password={}".format(config.get('redshift_config','redshift_host'),config.get('redshift_config','redshift_db'),config.get('redshift_config','redshift_user'),config.get('redshift_config','redshift_pass'))) \
        .option("dbtable", config.get('redshift_config','immigration_fact')) \
        .option("aws_iam_role", config.get('access_keys','IAM_ROLE'))  \
        .option("tempdir", config.get('redshift_config','redshift_s3_temp_dir')) \
        .option("diststyle","ALL") \
        .option("extracopyoptions", "TRUNCATECOLUMNS") \
        .mode(config.get('redshift_config','full_refresh')) \
        .save()
        
        # Create table as (mode = overwrite) and Insert into City Demographics Fact
        city_fact_df.write \
        .format("com.databricks.spark.redshift") \
        .option("url", "jdbc:redshift://{}:5439/{}?user={}&password={}".format(config.get('redshift_config','redshift_host'),config.get('redshift_config','redshift_db'),config.get('redshift_config','redshift_user'),config.get('redshift_config','redshift_pass'))) \
        .option("dbtable", config.get('redshift_config','visa_code_table')) \
        .option("aws_iam_role", config.get('access_keys','IAM_ROLE'))  \
        .option("tempdir", config.get('redshift_config','city_demographics')) \
        .option("diststyle","ALL") \
        .option("extracopyoptions", "TRUNCATECOLUMNS") \
        .mode(config.get('redshift_config','full_refresh')) \
        .save()
        
    except Exception as e:
        print(str(e))
        

        
# Perform Data Quality Checks
def data_quality_check_1(df, table_name):
    try:
        data_count = df.count()
        if data_count == 0:
            print("Data quality check1 failed for {} with zero records".format(table_name))
        else:
            print("Data quality check1 passed for {} with {} records".format(table_name, data_count))
        return 0
        
        
    except Exception as e:
        print(str(e))

# Perform Data Quality Check to identify duplication in data
def data_quality_check_2(df,table_name):
    try:
        data_count = df.count()
        data_count_after_dedup = df.dropDuplicates().count()
        data_diff = data_count - data_count_after_dedup 
        
        if data_count == data_count_after_dedup:
            print("Data quality check2 passed for {} with {} records".format(table_name, data_count))
        else:
            print("Data quality check2 failed for {} with difference of {} in  records".format(table_name,abs(data_diff)))
            
        return 0
        
        
    except Exception as e:
        print(str(e))
        

# Main Func
def main():
    # Create Spark Session
    spark = create_spark_session()
    
    # Fetch additional data for dimentions
    adds = gather_additional_data_after_cleaning(spark)
    
    # Fetch and Clean Immigration Data
    immigration_data = '/home/hadoop/data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    imm_df = immigration_data_cleaning(spark,immigration_data,adds[0])
    imm_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    
    # Fetch Temprature Data
    temprature_df = spark.read.format("csv").option("header", "true").load("/home/hadoop/data/GlobalLandTemperaturesByCity.csv")
    
    # Clean Temprature Data
    temprature_df = temprature_data_cleaning(spark,temprature_df)
    temprature_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    
    # Fetch City Demographics Data
    cities_fact_df = spark.read.option('delimiter',';').csv('us-cities-demographics.csv',header = True, mode = 'DROPMALFORMED')

    # Clean City Demographics Data and Model into Dimentions and Facts
    cities_dims_df = create_city_dimention(spark,cities_fact_df,temprature_df)
    race_dim_df = create_race_dimention(cities_fact_df)
    city_fact_and_city_race_tuple = create_city_demographics_fact(cities_fact_df,race_dim_df,cities_dims_df)
    
    # Load Dimentions Data
    create_load_dimention_tables(adds[0],adds[2],adds[3],adds[1],adds[4],cities_dims_df,race_dim_df,city_fact_and_city_race_tuple[1])
    
    # Load Facts Data
    create_load_fact_tables(imm_df,city_fact_and_city_race_tuple[0])
    
    # Perform Data Quality Checks for Immigration Fact
    data_quality_check_1(imm_df,"immigration_fact")
    data_quality_check_2(imm_df,"immigration_fact")
    
    # Perform Data Quality Checks for Cities Demographics Fact
    data_quality_check_1(city_fact_and_city_race_tuple[0],"City_Demographics_fact")
    data_quality_check_2(city_fact_and_city_race_tuple[0],"City_Demographics_fact")
    
    # Kill Spark Session
    spark.stop()


if __name__ == "__main__":
    main()