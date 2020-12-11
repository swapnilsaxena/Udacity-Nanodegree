import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Processes song data and creates the song and artist tables
    Parameters:
        - spark        : SparkSession
        - input_data   : path to input files
        - output_data  : path to store results
    '''
    # get filepath to song data file
    song_data = input_data + 'song-data/*/*/*/*.json'
    #song_data = os.path.join(input_data, "song-data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data).dropDuplicates()
    
    # extract columns to create songs table
    songs_table = df.select(
        col('song_id'), 
        col('title'),
        col('artist_id'), 
        col('year'), 
        col('duration')
    ).distinct() 
    songs_table.createOrReplaceTempView('songs')

    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')
    print("songs.parquet completed")

    # extract columns to create artists table
    artists_table = df.select(
        col('artist_id'), 
        col('artist_name').alias('name'), 
        col('artist_location').alias('location'),
        col('artist_latitude').alias('latitude'), 
        col('artist_longitude').alias('longitude')
    ).distinct()
    artists_table.createOrReplaceTempView('artists')
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')
    print("artists.parquet completed")


def process_log_data(spark, input_data, output_data):
    '''
    Process log data and creates the user, time, and songsplay tables
    Parameters:
        - spark        : SparkSession
        - input_data   : path to input files
        - output_data  : path to store results
    '''    
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'
    #log_data = os.path.join(input_data,'log_data/*.json')

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # extract columns for users table    
    users_table = df.select(
        col('userId').alias('user_id'),
        col('firstName').alias('first_name'), 
        col('lastName').alias('last_name'), 
        col('gender'), 
        col('level')
    ).distinct()
    users_table.createOrReplaceTempView('users')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')
    print("users.parquet completed")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("start_time", get_datetime(df.ts))
    
    # extract columns to create time table
    df = df.withColumn('hour', hour('timestamp'))
    df = df.withColumn('day', dayofmonth('timestamp'))
    df = df.withColumn('month', month('timestamp'))
    df = df.withColumn('year', year('timestamp'))
    df = df.withColumn('week', weekofyear('timestamp'))
    df = df.withColumn('weekday', dayofweek('timestamp'))
    
    time_table = df.select(
        col('start_time'), 
        col('hour'), 
        col('day'), 
        col('week'),
        col('month'),
        col('year'), 
        col('weekday')
    ).distinct()
    time_table.createOrReplaceTempView('time_table')
       
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')
    print("time.parquet completed")                                               

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, "song-data/*/*/*/*.json")
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    df = df.alias('log_df')
    song_df = song_df.alias('song_df')
    joined_df = df.join(song_df, col('log_df.artist') == col(
        'song_df.artist_name'), 'inner')
    songplays_table = joined_df.select(
        col('log_df.start_time').alias('start_time'),
        col('log_df.userId').alias('user_id'),
        col('log_df.level').alias('level'),
        col('song_df.song_id').alias('song_id'),
        col('song_df.artist_id').alias('artist_id'),
        col('log_df.sessionId').alias('session_id'),
        col('log_df.location').alias('location'), 
        col('log_df.userAgent').alias('user_agent'),
        year('log_df.start_time').alias('year'),
        month('log_df.start_time').alias('month')) \
        .withColumn('songplay_id', monotonically_increasing_id()) 
    
                                                               
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')
    print("songplays.parquet completed")                                           


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    #input_data = 'data/'
    output_data = "s3a://data-lake-project-out-swapnil/"
    #output_data = 'data/output/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
