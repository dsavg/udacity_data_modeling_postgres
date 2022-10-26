"""
File to read and process data and load them in tables.

The file reads and processes data from the `song_data`
and `log_data` folders and load them into tables.
"""

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import (songplay_table_insert, user_table_insert,
                         song_table_insert, artist_table_insert,
                         time_table_insert, song_select)


def process_song_file(cur, filepath):
    """
    Process song files from a file path.

    The function inserts data to the `songs`
    and `artists` tables.

    :param cur: cursor object
    :param filepath: str file path
    """
    # open song file
    dataframe = pd.read_json(filepath, lines=True)
    # insert song record
    song_data = dataframe[['song_id', 'title', 'artist_id',
                           'year', 'duration']]
    # dedupe data and turn dataframe to list of lists
    song_data = song_data.drop_duplicates().values.tolist()
    for song_data_row in song_data:
        cur.execute(song_table_insert, song_data_row)
    # insert artist record
    artist_data = dataframe[['artist_id', 'artist_name',
                             'artist_location', 'artist_latitude',
                             'artist_longitude']]
    # dedupe data and turn dataframe to list of lists
    artist_data = artist_data.drop_duplicates().values.tolist()
    for artist_data_row in artist_data:
        cur.execute(artist_table_insert, artist_data_row)


def process_log_file(cur, filepath):
    """
    Process log files from a file path.

    The function inserts data to the `time`, `users`
    and `songplays` tables.

    :param cur: cursor object
    :param filepath: str file path
    """
    # open log file
    dataframe = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    dataframe = dataframe[dataframe['page'] == 'NextSong']

    # convert timestamp column to datetime
    df_t = pd.to_datetime(dataframe['ts'], unit='ms').to_frame()
    # insert time data records
    df_t['hour'] = df_t.ts.dt.hour
    df_t['day'] = df_t.ts.dt.day
    df_t['week'] = df_t.ts.dt.week
    df_t['month'] = df_t.ts.dt.month
    df_t['year'] = df_t.ts.dt.year
    df_t['weekday'] = df_t.ts.dt.weekday
    time_df = df_t.copy()
    time_df = time_df.drop_duplicates()

    for _, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = dataframe[['userId', 'firstName',
                         'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates()

    # insert user records
    for _, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for _, row in dataframe.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            pd.to_datetime(row.ts, unit='ms'),
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Process dara from multiple JSON files.

    :param cur: cursor object
    :param conn: connection object
    :param filepath: str file path
    :param func: process_song_file or process_log_file
    """
    # get all files matching extension from directory
    all_files = []
    for root, _, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for file in files:
            all_files.append(os.path.abspath(file))

    # get total number of files found
    num_files = len(all_files)
    print(f'{num_files} files found in {filepath}')

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f'{i}/{num_files} files processed.')


def main():
    """Create all tables from files."""
    conn = psycopg2.connect("""host=127.0.0.1 dbname=sparkifydb
                               user=student password=student""")
    cur = conn.cursor()
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    conn.close()


if __name__ == "__main__":
    main()
