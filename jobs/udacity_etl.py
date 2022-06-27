from pyspark.sql import SparkSession
import pyspark.sql.functions as Function
from pyspark.sql import SQLContext


def create_spark_session():
    spark = SparkSession.builder \
        .master("yarn") \
        .appName('dataproc-udacity') \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Description: This function can be used to read the song data in the
        filepath (bucket/song_data) to get the song and artist info and
        used to populate the songs and artists dim tables.
    Parameters:
        spark: the cursor object.
        input_path: the path to the bucket containing song data.
        output_path: the path to destination bucket where the parquet files
            will be stored.
    Returns:
        None
    """
    # get filepath to song data file
    song_data = f'{input_data}/song-data/*/*/*/*.json'
    # read song data file
    df = spark.read.json(song_data)
    print('Read song_data from GCS')
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id',
                            'year', 'duration').dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(f'{output_data}/songs_table',
                              mode='overwrite',
                              partitionBy=['year', 'artist_id'])
    print('Writing songs_table to parquet')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name',
                              'artist_location', 'artist_latitude',
                              'artist_longitude').dropDuplicates()
    # write artists table to parquet files
    artists_table.write.parquet(f'{output_data}/artists_table',
                                mode='overwrite')
    print('Writting artists_table to parquet')
def process_log_data(spark, input_data, output_data):
    """
    Description: This function can be used to read the log data in the
        filepath (bucket/log_data) to get the info to populate the
        user, time and song dim tables as well as the songplays fact table.
    Parameters:
        spark: the cursor object.
        input_path: the path to the bucket containing song data.
        output_path: the path to destination bucket where the parquet files
            will be stored.
    Returns:
        None
    """
    # get filepath to log data file
    log_data = f'{input_data}/log-data/*.json'
    # read log data file
    df = spark.read.json(log_data)
    print('Reading log_data from GCS')
    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table
    user_table = df.select('userId', 'firstName', 'lastName',
                           'gender', 'level').dropDuplicates()
    # write users table to parquet files
    user_table.write.parquet(f'{output_data}/user_table', mode='overwrite')
    print('Writing user_table to parquet')
    # create timestamp column from original timestamp column
    df = df.withColumn('start_time', Function.from_unixtime(Function.col('ts')/1000))
    print('Converting ts to timestamp')
    # create datetime column from original timestamp column
    time_table = df.select('ts', 'start_time') \
                   .withColumn('year', Function.year('start_time')) \
                   .withColumn('month', Function.month('start_time')) \
                   .withColumn('week', Function.weekofyear('start_time')) \
                   .withColumn('weekday', Function.dayofweek('start_time')) \
                   .withColumn('day', Function.dayofyear('start_time')) \
                   .withColumn('hour', Function.hour('start_time')).dropDuplicates()
    print('Extract DateTime Columns')

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(f'{output_data}/time_table',
                             mode='overwrite',
                             partitionBy=['year', 'month'])
    print('Write time_table to parquet')

    # read in song data to use for songplays table
    song_data = f'{input_data}/song-data/A/A/A/*.json'
    song_dataset = spark.read.json(song_data)
    print('Read song_dataset from GCS')
    # join & extract cols from song and log datasets to create songplays table
    song_dataset.createOrReplaceTempView('song_dataset')
    time_table.createOrReplaceTempView('time_table')
    df.createOrReplaceTempView('log_dataset')
    songplays_table = spark.sql("""SELECT DISTINCT
                                       l.ts as ts,
                                       t.year as year,
                                       t.month as month,
                                       l.userId as user_id,
                                       l.level as level,
                                       s.song_id as song_id,
                                       s.artist_id as artist_id,
                                       l.sessionId as session_id,
                                       s.artist_location as artist_location,
                                       l.userAgent as user_agent
                                   FROM song_dataset s
                                   JOIN log_dataset l
                                       ON s.artist_name = l.artist
                                       AND s.title = l.song
                                       AND s.duration = l.length
                                   JOIN time_table t
                                       ON t.ts = l.ts
                                   """).dropDuplicates()
    print('SQL Query')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(f'{output_data}/songplays_table',
                                  mode='overwrite',
                                  partitionBy=['year', 'month'])
    print('Writing songplays_table to parquet')
def main():
    spark = create_spark_session()
    input_data = "gs://udacity_songs/data/"
    output_data = "gs://udacity_songs/dataoutput"
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
if __name__ == "__main__":
    main()