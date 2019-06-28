import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from zipfile import ZipFile
from pyspark.sql.functions import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWSkeys','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWSkeys','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    The Function reads the song data S3 path and reads json files from that
    path into a pyspark dataframe. Selects particular columns for the
    songs and artists table and then writes the data back to S3 in parquet files
    format.

    Args:
        spark: Spark connection string.
        input_data: Input data path where songs and log files reside on S3.
        output_data: Output data path where songs and artists table
                    will get written out to the S3 path.

    Returns:
        None
    """
    
    # get filepath to song data file
    song_data = 's3a://udacity-dend/song_data/*/*/*/*.json'
    
    # read song data file
    df_s = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df_s[['song_id', 'title', 'artist_id', 'year', 'duration']] 
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data,"songs"), "overwrite")

    # extract columns to create artists table
    artists_table = df_s[['artist_id', 'artist_name', 'artist_location', \
                          'artist_latitude', 'artist_longitude']]
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,"artists"), "overwrite")


def process_log_data(spark, input_data, output_data):
    """
    The Function reads the log data S3 path and reads json files from that
    path into a pyspark dataframe. Selects particular columns for the
    users and songplays table and then writes the data back to S3 in parquet files
    format.

    Args:
        spark: Spark connection string.
        input_data: Input data path where songs and log files reside on S3.
        output_data: Output data path where users and songplays table
                    will get written out to the S3 path.

    Returns:
        None
    """
    
    # get filepath to log data file
    log_data = 's3a://udacity-dend/log_data/*/*/*.json'

    # read log data file
    df_l = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_l = df_l[df_l['page'] == 'NextSong']

    # extract columns for users table    
    users_table = df_l[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    # write users table to parquet files
    users_table.write.parquet("users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: to_timestamp((x/1000).cast('timestamp'), "yyyy-MM-dd hh:mm:ss"))
    df_l = df_l.withColumn("timestamp_col", to_timestamp(df_l.ts))
     
    df_l=df_l.toPandas()
    df_l['start_time']=pd.to_datetime(df_l['ts'], unit='ms')
    df_l['hour'] = pd.to_datetime(df_l['start_time']).dt.hour
    df_l['day'] = pd.to_datetime(df_l['start_time']).dt.day
    df_l['week'] = pd.to_datetime(df_l['start_time']).dt.week
    df_l['month'] = pd.to_datetime(df_l['start_time']).dt.month
    df_l['year'] = pd.to_datetime(df_l['start_time']).dt.year
    df_l['weekday'] = pd.to_datetime(df_l['start_time']).dt.weekday
    df_l = spark.createDataFrame(df_l)
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: int(datetime.datetime.fromtimestamp(x / 1000.0))
    df_l = df_l.withColumn("datetime_col", get_datetime(df_l.ts))

    # extract columns to create time table
    time_table = df_l[['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']]
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data,"time"), "overwrite")

    # read in song data to use for songplays table
    song_data = 'song_data/A/A/A/TRAAAAW128F429D538.json'
    df_s = spark.read.json(song_data)
    df_songs_log_table = df_s.join(df_l, df_s.title == df_l.song)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_songs_log_table[['start_time', 'userId', 'level', 'song_id', \
                                          'artist_id','sessionId', 'location', 'userAgent']]

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data,"songplays"), "overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend/analytics/songplays"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
