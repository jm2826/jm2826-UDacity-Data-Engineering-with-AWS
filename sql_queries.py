import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
users_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create = ("""
    CREATE TABLE staging_events (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession int,
        lastName varchar,
        length float,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration float,
        sessionid int,
        song varchar,
        status int,
        ts bigint,
        userAgent varchar,
        userid int);
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs int,
        artist_id varchar,
        artist_latitude float,
        artist_longitude float,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration float,
        year int );
""")




# Fact Table for songplays
'''
songplay_table_create = ("""
    CREATE TABLE songplay (
        songplay_id int IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp,
        userid int,
        level varchar,
        song_id varchar,
        artist_id varchar,
        sessionid int,
        location varchar,
        userAgent varchar);
""")
'''
songplay_table_create = ("""
    CREATE TABLE songplay (
        songplay_id int IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp REFERENCES time(start_time),
        userid int REFERENCES users(userid),
        level varchar,
        song_id int REFERENCES song(song_id),
        artist_id int REFERENCES artist(artist_id),
        sessionid int,
        location varchar,
        userAgent varchar);
""")





# Dimensional Tables Describing Songplays
users_table_create = ("""
    CREATE TABLE users (
        userid int PRIMARY KEY,
        firstName varchar,
        lastName varchar,
        gender varchar,
        level varchar);
""")

song_table_create = ("""
    CREATE TABLE song (
        song_id varchar PRIMARY KEY,
        title text,
        artist_id varchar,
        year int,
        duration float);
""")

artist_table_create = ("""
    CREATE TABLE artist (
        artist_id varchar PRIMARY KEY,
        artist_name text,
        artist_location text,
        artist_lattitude float,
        artist_longitude float);
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time timestamp PRIMARY KEY,
        hour smallint NOT NULL,
        day smallint NOT NULL,
        week smallint NOT NULL,
        month smallint NOT NULL,
        year smallint NOT NULL,
        weekday varchar);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM '{}'
    credentials '{}'
    format as json '{}'
    STATUPDATE ON
    region 'us-west-2'
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs FROM '{}'
    credentials '{}'
    format as json 'auto'
    region 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])






# FINAL TABLES
'''
songplay_table_insert = ("""
    INSERT INTO songplay (start_time, userid, level, song_id, artist_id, sessionid, location, userAgent)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second' AS start_time,
        e.userid,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionid,
        e.location,
        e.userAgent
    FROM staging_events e
    JOIN staging_songs s
    ON e.artist = s.artist_name AND e.song = s.title
    WHERE e.page = 'NextSong';        
""")
'''
songplay_table_insert = ("""
    INSERT INTO songplay (level, sessionid, location, userAgent)
    SELECT DISTINCT
        e.level,
        e.sessionid,
        e.location,
        e.userAgent
    FROM staging_events e
    JOIN staging_songs s
    ON e.artist = s.artist_name AND e.song = s.title
    WHERE e.page = 'NextSong';        
""")








users_table_insert = ("""
    INSERT INTO users (userid, firstName, lastName, gender, level)
    SELECT DISTINCT
        userid,
        firstName,
        lastName,
        gender,
        level      
    FROM staging_events
    WHERE page = 'NextSong'
    AND userid IS NOT NULL; 
""")

song_table_insert = ("""
    INSERT INTO song (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration       
    FROM staging_songs
    WHERE artist_id IS NOT NULL; 
""")

artist_table_insert = ("""
    INSERT INTO artist (artist_id, artist_name, artist_location, artist_lattitude, artist_longitude)
        SELECT DISTINCT
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
    FROM staging_songs; 
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' AS start_time,
        EXTRACT(HOUR FROM start_time) AS hour,
        EXTRACT(DAY FROM start_time) AS day,
        EXTRACT(WEEKS FROM start_time) AS week,
        EXTRACT(MONTH FROM start_time) AS month,
        EXTRACT(YEAR FROM start_time) AS year,
        to_char(start_time, 'Day') AS weekday        
    FROM staging_events
    WHERE page = 'NextSong';        
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, users_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, users_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, users_table_insert, song_table_insert, artist_table_insert, time_table_insert]
