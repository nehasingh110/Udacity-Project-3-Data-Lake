### Purpose of the database
1. Sparkify, a music streaming startup, has grown their user base and song database and want to move their processes and data onto the cloud. 
2. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
3. I've built an ETL pipeline that extracts their data from S3, stages them in Spark, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 


### Database schema design 
1. The schema contains a fact table called songplay that was populated from songplays_table pyspark sql dataframe which was obtained by joining df_s and df_l pyspark dataframes. This fact table contains some facts like start_time, level, location etc and few ID fields/keys that can be used to join this fact table to the dimension tables to get more information about artists, songs etc.
2. There are 4 dimension tables around 1 fact table in this star schema design.
3. The 4 dimension tables are users, song, artists and time. They contain data about users using the Sparkify, songs available in their databse, artists that song belongs to and the duration of songs respectively.


### ETL Pipeline
1. The connection to S3 is set up by getting AWS access keys from dl.cfg.
2. A spark session is created and passed to a function 'process_song_data' that reads the song data S3 path and reads json files from that path into a pyspark dataframe. Selects particular columns for the songs and artists table and then writes the data back to S3 in parquet files format.
3. Similarly another function reads the log data S3 path and reads json files from that path into a pyspark dataframe. Selects particular columns for the users and songplays table and then writes the data back to S3 in parquet files format.


### Steps to run files
1. Run etl.py in python console using the following command : %run etl.py
