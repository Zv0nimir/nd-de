import configparser
import os
import pandas as pd
import re
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, year, month, dayofmonth, hour, weekofyear, date_format, upper, avg, count, desc, round

config = configparser.ConfigParser()
config.read('project.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

input_data= config.get('PATH', 'INPUT_DATA')
output_data= config.get('PATH', 'OUTPUT_DATA')


def create_spark_session():
    """
        Create and return Apache Spark session used to process the data
    Returns:
        spark(object): Spark session
    """
    
    spark = SparkSession.builder \
                        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
                        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
                        .enableHiveSupport() \
                        .getOrCreate()
    
    print('\nSpark session started')
    
    return spark


def stage_i94_immigration_data(spark, data_source, table_name):
    """
    This function:
        Load data into Spark session, clean them and write them into stage tables
    Paramters:
        spark (object) - Spark session
        data_source (string) - data source path
        table_name (string) - output stage table name
    """
    
    print('\nStart of stage_i94_immigration_data')
    
    #load I94 immigration data
    df_spark_i94 = spark.read.format('com.github.saurfang.sas.spark').load(data_source)
    
    #create dictionary of valid US ports
    port_re_filter = re.compile(r'\'(.*)\'.*\'(.*)\'')
    i94_port_dict = {}

    with open(os.path.join(input_data, 'map/I94-us_ports.txt')) as file:
        for line in file:
            groups = port_re_filter.search(line)
            if 'No PORT Code' not in groups[2] and 'Collapsed' not in groups[2]:
                i94_port_dict[groups[1]] = groups[2]
                
    #clean I94 immigration data
    df_spark_i94_clean = df_spark_i94.dropDuplicates() \
                                .filter(df_spark_i94.i94port.isin(list(i94_port_dict.keys()))) \
                                .na.drop(subset=["depdate"]) \
                                .na.drop(subset=["i94mode"]) \
                                .na.drop(subset=["matflag"])
    
    #write data to the stage table
    df_spark_i94_clean.write.mode('overwrite').partitionBy('arrdate').parquet(os.path.join(output_data, table_name))
    
    print('End of stage_i94_immigration_data')
    
    
def stage_city_temperature_data(spark, data_source, table_name):
    """
    This function:
        Load US city temperature data into Spark session, clean them and write them into stage tables
    Paramters:
        spark (object) - Spark session
        data_source (string) - data source path
        table_name (string) - output stage table name
    """
    
    print('\nStart of stage_city_temperature_data')
    
    #load city temperature data
    df_spark_temperature = spark.read.options(header='True', inferSchema='True').csv(os.path.join(input_data, data_source))
    
    #convert column names to lower case
    df_spark_temperature = df_spark_temperature.toDF(*[c.lower() for c in df_spark_temperature.columns])
    
    #clean city temperature data
    df_spark_temperature_clean = df_spark_temperature.dropDuplicates() \
                                                .filter(df_spark_temperature.country == 'US') \
                                                .filter(df_spark_temperature.year == 2016) \
                                                .filter(df_spark_temperature.month == 4) \
                                                .withColumn('state', upper(df_spark_temperature.state)) \
                                                .withColumn('city', upper(df_spark_temperature.city)) \
 
    #write data to the stage table
    df_spark_temperature_clean.write.mode('overwrite').parquet(os.path.join(output_data, table_name))
    
    print('End of stage_city_temperature_data')
    

def load_dim_countries(spark, country_data_source, country_table_name):
    """
    This function:
        Create and load dimension table for countries
    Paramters:
        spark (object) - Spark session
        country_data_source (string) - path to the data source containing countries
        country_table_name (string) - output dimension table name
    """
    
    print('\nStart of load_dim_countries')
    
    #load data into pandas dataframe
    df_countries = pd.read_csv(os.path.join(input_data, country_data_source), sep="=", header=None, names = ['code', 'name'])
    
    #clear data in pandas dataframe
    df_countries['code'].apply(pd.to_numeric)
    df_countries['name'] = df_countries['name'].str.replace("'","").replace(to_replace=['No Country.*', 'INVALID.*', 'Collapsed.*'], value = 'Other countries', regex = True).str.strip()

    #convert pandas dataframe to spark dataframe
    df_spark_countries = spark.createDataFrame(df_countries)
    
    #write data to the dimension table
    df_spark_countries.write.mode('overwrite').parquet(os.path.join(output_data, country_table_name))
    
    print('End of load_dim_countries')


def load_dim_travel_modes(spark, mode_data_source, mode_table_name):
    """
    This function:
        Create and load dimension table for travel modes
    Paramters:
        spark (object) - Spark session
        mode_data_source (string) - path to the data source containing travel modes
        mode_table_name (string) - output dimension table name
    """
    
    print('\nStart of load_dim_travel_modes')
    
    #load data into pandas dataframe
    df_travel_modes = pd.read_csv(os.path.join(input_data, mode_data_source), sep="=", header=None, names = ['code', 'name'])
    
    #clear data in pandas dataframe
    df_travel_modes['code'].apply(pd.to_numeric)
    df_travel_modes['name'] = df_travel_modes['name'].str.replace("'","").str.strip()
    
    #convert pandas dataframe to spark dataframe
    df_spark_travel_modes = spark.createDataFrame(df_travel_modes)
    
    #write data to the dimension table
    df_spark_travel_modes.write.mode('overwrite').parquet(os.path.join(output_data, mode_table_name))

    print('End of load_dim_travel_modes')
    

def load_dim_us_visas(spark, visas_data_source, visas_table_name):
    """
    This function:
        Create and load dimension table for US visas
    Paramters:
        spark (object) - Spark session
        visas_data_source (string) - path to the data source containing US visas
        visas_table_name (string) - output dimension table name
    """
    
    print('\nStart of load_dim_us_visas')
    
    #load data into pandas dataframe
    df_us_visas = pd.read_csv(os.path.join(input_data, visas_data_source), sep="=", header=None, names = ['code', 'name'])
    
    #clear data in pandas dataframe
    df_us_visas['code'].apply(pd.to_numeric)
    df_us_visas['name'].str.strip()
    
    #convert pandas dataframe to spark dataframe
    df_spark_us_visas = spark.createDataFrame(df_us_visas)
    
    #write data to the dimension table
    df_spark_us_visas.write.mode('overwrite').parquet(os.path.join(output_data, visas_table_name))
    
    print('End of load_dim_us_visas')
    
    
def load_dim_us_ports(spark, port_data_source, state_data_source, port_table_name):
    """
    This function:
        Create and load dimension table for US ports
    Paramters:
        spark (object) - Spark session
        port_data_source (string) - path to the data source containing US ports
        state_data_source (string) - path to the data source containing US states
        port_table_name (string) - output dimension table name
    """
    
    print('\nStart of load_dim_us_ports')
    
    #load US ports data into pandas dataframe
    df_us_ports = pd.read_csv(os.path.join(input_data, port_data_source), sep="=", header=None, names = ['code', 'city'])
    
    #clear US ports data
    df_us_ports['code'] = df_us_ports['code'].str.replace("'","").str.replace("\t","").str.strip()
    df_us_ports['city'] = df_us_ports['city'].str.replace("'","").str.replace("\t","").replace(to_replace=['No PORT Code.*', 'Collapsed.*'], value = 'Other US ports', regex = True).str.strip()
    
    #split port/city name column to obtain city name and state code
    us_ports_split_col = df_us_ports['city'].str.split(",", n=1, expand=True)
    
    #update values in column city
    df_us_ports['city'] = us_ports_split_col[0].str.strip()
    
    #add and format new column for state 
    df_us_ports['state'] = us_ports_split_col[1]
    df_us_ports['state'] = df_us_ports['state'].str.strip()
    
    
    #load US states data into pandas dataframe
    df_us_states = pd.read_csv(os.path.join(input_data, 'map/I94-us_states.txt'), sep="=", header=None, names = ['state_code', 'state_name'])
    
    #clear US states data
    df_us_states['state_code'] = df_us_states['state_code'].str.replace("'","").str.replace("\t","").str.strip()
    df_us_states['state_name'] = df_us_states['state_name'].str.replace("'","").str.replace("\t","").str.strip()
    
    #join US ports and US states data
    df_us_ports = df_us_ports.join(df_us_states.set_index('state_code'), on='state', how='inner', lsuffix='p', rsuffix='s')
        
    #convert pandas dataframe to spark dataframe
    df_spark_us_ports = spark.createDataFrame(df_us_ports)
    
    #write data to the dimension table
    df_spark_us_ports.write.mode('overwrite').parquet(os.path.join(output_data, port_table_name))
    
    print('End of load_dim_us_ports')
    

def build_fact_i94_visits(spark, stage_i94_table_name, stage_temperature_table_name, dim_ports_table_name, visits_fact_table):
    """
    This function:
        Create and load fact table for I94 visits
    Paramters:
        spark (object) - Spark session
        stage_i94_table_name (string) - stage table containing I94 immigration data
        stage_temperature_table_name (string) - stage table containing city temperature data
        dim_ports_table_name (string) - dimension table containing US ports data
        visits_fact_table (string) - output fact table
    Returns:
        dataframe to be used from load_dim_date function as a parameter
    """
    
    print('\nStart of build_fact_i94_visits')
    
    #load stage immigration data
    df_fact_I94_visits = spark.read.parquet(os.path.join(output_data, stage_i94_table_name))
    
    #load stage city temperature data
    df_dim_temperatures = spark.read.parquet(os.path.join(output_data, stage_temperature_table_name))
    
    #load dimension US port data
    df_dim_us_ports = spark.read.parquet(os.path.join(output_data, dim_ports_table_name))
    
    #create udf to convert SAS date type to ISO date type
    get_date_from_sas = udf(lambda x: (datetime(1960, 1, 1).date() + timedelta(x)).isoformat() if x else None)

    #create udf to get date from date parts
    get_date = udf(lambda x, y, z: (datetime(x, y, z).date()).isoformat())

    #create udf to get visitor's stay
    get_stay = udf(lambda x, y: int(x - y))
    
    #prepare df_fact_I94_visits table
    df_fact_I94_visits = df_fact_I94_visits.withColumn('stay', get_stay(df_fact_I94_visits.depdate, df_fact_I94_visits.arrdate)) \
                                            .withColumn('arrdate', get_date_from_sas(df_fact_I94_visits.arrdate)) \
                                            .withColumn('depdate', get_date_from_sas(df_fact_I94_visits.depdate))
    
    #prepare df_dim_temperatures temperature table
    df_dim_temperatures = df_dim_temperatures.withColumn('date', get_date(df_dim_temperatures.year, df_dim_temperatures.month, df_dim_temperatures.day))

    #join df_fact_I94_visits dataframe with df_dim_us_ports dataframe
    df_fact_I94_visits = df_fact_I94_visits.join(df_dim_us_ports, df_fact_I94_visits.i94port == df_dim_us_ports.code, how='inner').drop(df_dim_us_ports.code)
    
    #join df_fact_I94_visits dataframe with df_dim_temperatures dataframe
    df_fact_I94_visits = df_fact_I94_visits.join(df_dim_temperatures, [df_fact_I94_visits.arrdate == df_dim_temperatures.date, \
                                                                       df_fact_I94_visits.city == df_dim_temperatures.city, \
                                                                       df_fact_I94_visits.state_name == df_dim_temperatures.state] , how='inner') \
                                            .drop(df_dim_temperatures.city)
    
    #select necessary columns and write fact table partitioned by arrival date
    df_fact_I94_visits = df_fact_I94_visits.select('cicid', 'arrdate', 'depdate', 'stay', 'i94port', 'i94cit', 'i94mode', 'i94visa', 'visatype', 'avgtemperature')
    
    #write data to the fact table
    df_fact_I94_visits.write.mode('overwrite').partitionBy('arrdate').parquet(os.path.join(output_data, visits_fact_table))
    
    print('End of build_fact_i94_visits')
    
    #return dataframe to be used from load_dim_date function as a parameter
    return df_fact_I94_visits.select('arrdate').dropDuplicates()
    
    
def load_dim_date(spark, df_date, date_table_name):
    """
    This function:
        Create and load dimension table for US visas
    Paramters:
        spark (object) - Spark session
        df_date (dataframe) - dataframe containing dates        
        date_table_name (string) - output dimension table
    """
    
    print('\nStart of load_dim_date')
    
    #prepare date dataframe
    df_date = df_date.withColumn('day', dayofmonth(df_date.arrdate)) \
                 .withColumn('weekday', date_format(df_date.arrdate, 'E')) \
                 .withColumn('week', weekofyear(df_date.arrdate)) \
                 .withColumn('month', month(df_date.arrdate)) \
                 .withColumn('year', year(df_date.arrdate))
    
    #write data to the dimension table
    df_date.write.mode('overwrite').parquet(os.path.join(output_data, date_table_name))    
    
    print('End of load_dim_date')
    

def check_column_exists(spark, table_name, column_name):
    """
    This function:
        Check if column exists in table
    Paramters:
        spark (object) - Spark session
        table_name (dataframe) - table name to check
        column_name (string) - column name to check
    """
    
    print('\nCheck if exists column named "{}" in table "{}"'.format(column_name, table_name))
    
    df = spark.read.parquet(os.path.join(output_data, table_name)).limit(1)
    if column_name in df.columns:
        print('Column "{}" does exists in table "{}"'.format(column_name, table_name))
    else:
        raise ValueError('Column "{}" DOES NOT exists in table "{}"'.format(column_name, table_name))
        
        
def check_column_type(spark, table_name, column_name, expected_type):
    """
    This function:
        Check if column has a certain data type in table
    Paramters:
        spark (object) - Spark session
        table_name (dataframe) - table name to check
        column_name (string) - column name to check
        expected_type (string) - type to check
    """
    
    print('\nCheck if exists column {} with data type "{}" in table"{}"'.format(column_name, expected_type, table_name))
    
    df = spark.read.parquet(os.path.join(output_data, table_name)).limit(1)
    
    column_exists = False
    for name, dtype in df.dtypes:
        if name == column_name:
            column_exists = True
            if dtype == expected_type:
                print('Column "{}.{}" has expected data type'.format(table_name, column_name)) 
                return
            else:
                raise ValueError('Column "{}.{}" DOES NOT HAVE expected data type'.format(column_name, table_name))

    if not column_exists: 
        raise ValueError('Column "{}" DOES NOT exists in table "{}"'.format(column_name, table_name))
        

def check_unique_key(spark, table_name, column_list):
    """
    This function:
        Check if column has a certain data type in table
    Paramters:
        spark (object) - Spark session
        table_name (dataframe) - table name to check
        column_list (list) - list of columns to compare
    """
    
    print('\nCheck if unique key on table "{}"'.format(table_name))
    
    df = spark.read.parquet(os.path.join(output_data, table_name))
    
    rec_count = df.count()
    
    if df.count() > df.dropDuplicates(column_list).count():
        raise ValueError('Error checking unique key on table. Key has duplicates for {}'.format(table_name, column_list))
    else:
        print("Succesfull unique key check on table {}.".format(table_name))
        


def run_unit_test_on_I94_arrival_date(spark, fact_table_name):
    """
    This function:
        Run unit test to check arrival dates in fact table
    Paramters:
        spark (object) - Spark session
        table_name (dataframe) - table name to check
        column_list (list) - list of columns to compare
    """
        
    print('\nRunning unit test on table "{}"'.format(fact_table_name))
    
    df = spark.read.parquet(os.path.join(output_data, fact_table_name))
    
    rec_count = df.filter((year(df.arrdate) != 2016) | (month(df.arrdate) != 4)).count()
                          
    if rec_count == 0:
        print("Unit test has passed. Fact table contains only records for April 2016.")
    else:
        raise ValueError("Unit test has failed. Fact table contains records outside expected period.")
        

        
def check_row_count(spark, table_name):
    """
    This function:
        Check row count on table
    Paramters:
        spark (object) - Spark session
        table_name (dataframe) - table name to check
    """
    
    print('\nCheck row count on table "{}"'.format(table_name))
    
    df = spark.read.parquet(os.path.join(output_data, table_name))
    
    rec_count = df.count()
    
    if rec_count == 0:
        raise ValueError('Quality check for table "{}" has failed. The table has no records.'.format(table_name))
    else:
        print("Succesfull quality check on table {}. The table has {} records.".format(table_name, rec_count))
        


def get_top_10_warmest_states(spark, fact_visit_table, dim_port_table):
    """
    This function:
        Get average temperature in top 10 states ordered by number od arrivals descending
    Paramters:
        spark (object) - Spark session
        fact_visit_table (string) - fact table containing visits
        dim_port_table (string) - dimension table containing US ports
    """
    
    print('\nGet average temperature in top 10 states ordered by number od arrivals descending\n')
    
    #load necessary data
    df_read_visits = spark.read.parquet(os.path.join(output_data, fact_visit_table))
    df_read_ports = spark.read.parquet(os.path.join(output_data, dim_port_table))
    
    #join dataframe by port code
    df_query = df_read_visits.join(df_read_ports, df_read_visits.i94port == df_read_ports.code, how='inner').drop(df_read_ports.code)
    
    #get top 10 desc
    df_query.groupBy('state_name').agg(round(avg('avgtemperature'), 2).alias('avg_temperature'), count('*').alias('count')).orderBy(desc('count')).show(10)   
    

def get_number_of_arrivals_in_top_5_warmest_states(spark, fact_visit_table, dim_port_table):
    """
    This function:
        Get number of arrival in top 5 warmest states
    Paramters:
        spark (object) - Spark session
        fact_visit_table (string) - fact table containing visits
        dim_port_table (string) - dimension table containing US ports
    """
    
    print('\nGet number of arrival in top 5 warmest states\n')
    
    #load necessary data
    df_read_visits = spark.read.parquet(os.path.join(output_data, fact_visit_table))
    df_read_ports = spark.read.parquet(os.path.join(output_data, dim_port_table))
    
    #join dataframe by port code
    df_query = df_read_visits.join(df_read_ports, df_read_visits.i94port == df_read_ports.code, how='inner').drop(df_read_ports.code)
    
    #get top 5 desc
    df_query.groupBy('state_name').agg(round(avg('avgtemperature'), 2).alias('avg_temperature'), count('*').alias('count')).orderBy(desc('avg_temperature')).show(5)     
    
    
def get_top_10_warmest_cities(spark, fact_visit_table, dim_port_table, state_code):
    """
    This function:
        Get top 10 warmest cities in state with number of arrivals
    Paramters:
        spark (object) - Spark session
        fact_visit_table (string) - fact table containing visits
        dim_port_table (string) - dimension table containing US ports
        state_code (string) - state code
    """
    
    print('\nGet top 10 warmest cities in state with number of arrivals\n')
    
    #load necessary data
    df_read_visits = spark.read.parquet(os.path.join(output_data, fact_visit_table))
    df_read_ports = spark.read.parquet(os.path.join(output_data, dim_port_table))
    
    #join dataframe by port code
    df_query = df_read_visits.join(df_read_ports, df_read_visits.i94port == df_read_ports.code, how='inner').drop(df_read_ports.code)
    
    #get top 10 desc
    df_query.filter(df_query.state == state_code).groupBy('city').agg(round(avg('avgtemperature'), 2).alias('avg_temperature'), count('*').alias('count')).orderBy(desc('count')).show(5)

    
        
def main():
    """
    This function:
        Builds a ETL pipeline to process I94 immigration data and US city temperature data into fact and dimension tables on datalake
    Args:
        No args.
    """

    #create spark session
    spark = create_spark_session()
    
    
    #load I94 immigration data into Spark session, clean them and write them into stage tables
    stage_i94_immigration_data(spark, '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat', 'stage_i94_immigration')
    
    #load US city temperature data into Spark session, clean them and write them into stage tables
    stage_city_temperature_data(spark, 'city_temperature.csv', 'stage_city_temperatures')

    
    #create and load dimension table for countries
    load_dim_countries(spark, 'map/I94-country-codes.txt', 'dim_countries')
    
    #create and write dimension table for travel modes
    load_dim_travel_modes(spark, 'map/I94-travel_modes.txt', 'dim_travel_modes')
    
    #create and write dimension table for US visas
    load_dim_us_visas(spark, 'map/I94-us_visas.txt', 'dim_us_visas')
    
    #create and write dimension table for US ports
    load_dim_us_ports(spark, 'map/I94-us_ports.txt', 'map/I94-us_states.txt', 'dim_us_ports')
    
    
    #create and write fact table for I94 immigration data and US city temperature data
    df_visits = build_fact_i94_visits(spark, 'stage_i94_immigration', 'stage_city_temperatures', 'dim_us_ports', 'fact_i94_visits')
    
    
    #create and write dimension table for US visas
    load_dim_date(spark, df_visits, 'dim_date')
    
    
    #quality checks - start
    
    #check if exists column named 'cicid' in table named 'fact_i94_visits'
    check_column_exists(spark, 'fact_i94_visits', 'cicid')
    
    #check if exists column named 'cicid' with data type {double} in table named 'fact_i94_visits'
    check_column_type(spark, 'fact_i94_visits', 'cicid', 'double')
    
    #run quality check by checking the unique key
    check_unique_key(spark, 'fact_i94_visits', ['cicid'])
    
    #run unit test to check arrival dates in fact table
    run_unit_test_on_I94_arrival_date(spark, 'fact_i94_visits')
    
    #run quality check by checking the row count
    check_row_count(spark, 'dim_us_ports')
    
    #quality checks - end
    
    
    #sample queries - run 
    
    #get average temperature in top 10 states ordered by number od arrivals descending
    get_top_10_warmest_states(spark, 'fact_i94_visits', 'dim_us_ports')
    
    #get number of arrival in top 5 warmest states
    get_number_of_arrivals_in_top_5_warmest_states(spark, 'fact_i94_visits', 'dim_us_ports')
    
    #get top 10 warmest cities in State of California with number of arrivals
    get_top_10_warmest_cities(spark, 'fact_i94_visits', 'dim_us_ports', 'CA')
    
    #sample queries - end
    
    #end spark session
    spark.stop()

if __name__ == "__main__":
    main()