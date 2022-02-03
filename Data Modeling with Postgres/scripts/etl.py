import os
import glob
import psycopg2
from datetime import datetime
import pandas as pd
from sql_queries import *

song_index = 1

def process_song_file(cur, filepath):
    """
    - Opens song file

    - Parses file and extracts list of values for song table

    - Calls song_table_insert function to insert values

    - Parses file and extracts list of values for artist table

    - Calls song_table_insert to insert values

    """
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0]
    song_data = song_data.tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude" ]].values[0]
    artist_data = artist_data.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Opens log file and selects only 'NextSong' records

    - Using datetime library breaks down the timestamp variable into multiple requested values and save into a dataframe

    - Iterate through dataframe to insert values into time table.

    - Create a new dataframe using only values requested for user table.

    - Iterate through dataframe to insert values into user table.

    - Finally, iterate through dataframe and populate songplay table via calling songplay_table_insert.

    """
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    time_data = list()
    timestamp = df["ts"].values.tolist()
    for value in timestamp:
        t = list()
        dt_object = datetime.fromtimestamp(value/1000.0)
        time_obj = dt_object.time()
        t.append(value)
        t.append(dt_object.hour)
        t.append(dt_object.day)
        t.append(dt_object.isocalendar()[1])
        t.append(dt_object.month)
        t.append(dt_object.year)
        t.append(dt_object.isoweekday())
        time_data.append(t)

    # insert time data records
    column_labels = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    time_df = pd.DataFrame(time_data, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level" ]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, list(row))

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Add all json files in a list

    - Iterate through files and process.

    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Make a connection to the database.

    - Call function for processing song data.

    - Call function for processing log data.

    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()



    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
