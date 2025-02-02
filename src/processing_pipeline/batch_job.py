from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import (from_json, dayofweek, dayofmonth, 
                                   date_format, when, col, hour, month, year)
from utils_spark import (get_SparkSession, 
                         convert_kafka_data_to_df, 
                         generate_traffic_features,
                         generate_weather_features)
from datetime import datetime


def generate_date_time_columns(df: SparkSession) -> DataFrame:
    """
    Args:
        df: Spark DataFrame
    Returns:
        df: Spark DataFrame with additional date and time columns
    """

    #add part of day column
    df = df.withColumn("part_of_day", \
                        when((hour(df['execution_time']).between(0, 5)), "night"). \
                        when((hour(df['execution_time']).between(6, 11)), "morning"). \
                        when((hour(df['execution_time']).between(12, 17)), "afternoon"). \
                        otherwise("evening"))
    
    #add date and time columns
    df = df.withColumn("day_of_week", date_format(df['execution_time'], "EEEE")) \
           .withColumn("day", dayofmonth(df['execution_time'])) \
           .withColumn("month", month(df['execution_time'])) \
           .withColumn("year", year(df['execution_time'])) \
           .withColumn("time_of_day", date_format(df['execution_time'], "HH:mm:ss"))
    
    return df
    

""" Function to process traffic data from kafka"""
def process_kafka_traffic_data(spark: SparkSession,
                               kafka_server: str = 'kafka1:19092',
                               topic: str = 'traffic_data'):
    """
    Args:
        spark: SparkSession object
        kafka_server: kafka server url
        topic: kafka topic
    """
    
    #check params
    if not isinstance(spark, SparkSession):
        raise ValueError("spark should be a SparkSession object")
    
    traffic_df = spark.read \
                      .format("kafka") \
                      .option("kafka.bootstrap.servers", kafka_server) \
                      .option("subscribe", topic) \
                      .option("startingOffsets", "latest") \
                      .load()
    
    #parse json data
    traffic_df = convert_kafka_data_to_df(traffic_df, "traffic")

    #filter data by execution time
    traffic_df = traffic_df.filter(traffic_df['execution_time'] == \
                                   datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    #rename columns
    traffic_df = traffic_df.withColumnRenamed("traffic light", "traffic_light") \
                           .withColumnRenamed("stop sign", "stop_sign")
    
    #generate traffic features
    traffic_df = generate_traffic_features(traffic_df)
    traffic_df = generate_date_time_columns(traffic_df)

    

if __name__ == "__main__":
    with get_SparkSession("Spark batch") as spark:
        read_Kafka_batch_data(spark, "traffic_data")