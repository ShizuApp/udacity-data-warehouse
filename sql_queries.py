import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artits;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE staging_events
    (
        artist          varchar,
        auth            varchar,
        firstName       varchar
        gender          varchar,
        itemSession     integer,
        lastName        varchar,
        lenght          float,
        level           varchar,
        location        varchar,
        method          varchar,
        page            varchar,
        registration    float,
        sessionId       integer, 
        song            varchar,
        status          integer,
        ts              timestamp,
        userAgent       varchar,
        userId          integer
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_events
    (
        num_songs           integer,
        artist_id           varchar,
        artist_latitude     double precision,
        artist_longitude    double precision,
        artist_location     varchar,
        artist_name         varchar,
        song_id             varchar,
        title               varchar,
        duration            float,
        year                integer
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays
    (  
        songplay_id     integer IDENTITY(0,1) not null distkey,
        start_time      timestamp not null sortkey,
        user_id         bigint not null,
        level           varchar,
        song_id         varchar not null,
        artist_id       varchar not null,
        session_id      varchar not null,
        location        varchar,
        user_agent      varchar
    );
""")

user_table_create = ("""
    CREATE TABLE users
    (  
        user_id     bigint not null,
        first_name  varchar,
        last_name   varchar,
        gender      varchar,
        level       varchar
    )
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE songs
    (  
        song_id     varchar not null,
        title       varchar not null,
        artist_id   varchar not null,
        year        integer,
        duration    float not null
    )
    diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE artists
    (
        artist_id   varchar not null,
        name        varchar not null,
        location    varchar,
        latitude    double precision,
        longitude   double precision
    )
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE time
    (   
        start_time  timestamp not null sortkey, 
        hour        smallint,
        day         smallint,
        week        smallint,
        month       varchar,
        year        integer,
        weekday     smallint
    )
    diststyle all;
""")


# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON {};
""").format(data_bucket=config['S3']['LOG_DATA'], 
            role_arn=config['IAM_ROLE']['ARN'], 
            log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2';
""").format(data_bucket=config['S3']['SONG_DATA'], 
            role_arn=config['IAM_ROLE']['ARN'])


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(e.ts)  AS start_time, 
            e.userId        AS user_id, 
            e.level         AS level, 
            s.song_id       AS song_id, 
            s.artist_id     AS artist_id, 
            e.sessionId     AS session_id, 
            e.location      AS location, 
            e.userAgent     AS user_agent
    FROM staging_events e
    JOIN staging_songs  s   ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(userId)    AS user_id,
            firstName           AS first_name,
            lastName            AS last_name,
            gender,
            level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT(song_id),
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT(artist_id),
            artist_name         AS name,
            artist_location     AS location,
            artist_latitude     AS latitude,
            artist_longitude    AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT(start_time),
            EXTRACT(hour FROM start_time),
            EXTRACT(day FROM start_time),
            EXTRACT(week FROM start_time),
            EXTRACT(month FROM start_time),
            EXTRACT(year FROM start_time),
            EXTRACT(dayofweek FROM start_time) AS weekday
    FROM songplays;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
