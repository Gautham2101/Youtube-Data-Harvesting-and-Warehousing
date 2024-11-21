#!/usr/bin/env python
# coding: utf-8

# In[20]:


import mysql.connector
import googleapiclient.discovery
from datetime import datetime
from googleapiclient.errors import HttpError
from mysql import connector

connection= connector.connect(
    host="localhost",
    user="root",
    password="Siva@210903",
)
cursor= connection.cursor()
cursor

def execute_query(question):
    query_mapping = {
        "What are the names of all the videos and their corresponding channels?":
            """SELECT title AS Video_title, channel_name
               FROM videos
               JOIN channels ON channels.channel_id = videos.channel_id;""",
               
        "Which channels have the most number of videos, and how many videos do they have?":
            """SELECT channel_name, COUNT(video_id) AS video_count
               FROM videos
               JOIN channels ON channels.channel_id = videos.channel_id
               GROUP BY channel_name
               ORDER BY video_count DESC;""",
               
        "What are the top 10 most viewed videos and their respective channels?":
            """SELECT title AS Video_title, channel_name
               FROM videos
               JOIN channels ON channels.channel_id = videos.channel_id
               ORDER BY view_count DESC
               LIMIT 10;""",
               
        "How many comments were made on each video, and what are their corresponding video names?":
            """SELECT title AS Video_title, COUNT(*) AS comment_counts
               FROM videos
               JOIN comments ON videos.video_id = comments.video_id
               GROUP BY title;""",
               
        "Which videos have the highest number of likes, and what are their corresponding channel names?":
            """SELECT title AS Video_title, channel_name
               FROM videos
               JOIN channels ON channels.channel_id = videos.channel_id
               ORDER BY like_count DESC
               LIMIT 1;""",
               
        "What is the total number of likes for each video, and what are their corresponding video names?":
            """SELECT title AS Video_title, SUM(like_count) AS total_likes
               FROM videos
               GROUP BY title;""",
               
        "What is the total number of views for each channel, and what are their corresponding channel names?":
            """SELECT channel_name, SUM(view_count) AS Total_views
               FROM videos
               JOIN channels ON channels.channel_id = videos.channel_id
               GROUP BY channel_name;""",
               
        "What are the names of all the channels that have published videos in the year 2022?":
            """SELECT DISTINCT channels.channel_name
               FROM channels
               JOIN videos ON channels.channel_id = videos.channel_id
               WHERE YEAR(published_date) = 2022;""",
               
        "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            """SELECT channel_name, AVG(duration) AS Average_duration
               FROM videos
               JOIN channels ON videos.channel_id = channels.channel_id
               GROUP BY channel_name;""",
               
        "Which videos have the highest number of comments, and what are their corresponding channel names?":
            """SELECT title AS Video_title, channel_name
               FROM videos
               JOIN channels ON videos.channel_id = channels.channel_id
               ORDER BY comment_count DESC
               LIMIT 1;"""
    }

    query = query_mapping.get(question)
    if query:
        return fetch_data(query)
    else:
        return pd.DataFrame()



# YouTube API connection setup
def api_connect():
    api_Id = "AIzaSyBlIG-XyCQMMvu02ml_-hKjFc1h4ycrvcY"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_Id)
    return youtube

youtube = api_connect()

# Function to create a database connection
def create_db_connection(database="youtube_db"):
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Siva@210903",
        database=database
    )
    return connection

# Function to create database and tables
def create_database_and_tables():
    connection = create_db_connection()
    cursor = connection.cursor()

    cursor.execute("CREATE DATABASE IF NOT EXISTS youtube_db")
    cursor.execute("USE youtube_db")

    create_channels_table = """
    CREATE TABLE IF NOT EXISTS channels (
        channel_id VARCHAR(50) PRIMARY KEY,
        channel_name VARCHAR(255),
        subscribers BIGINT,
        views BIGINT,
        total_videos INT,
        channel_description TEXT,
        playlist_id VARCHAR(50)
    );
    """
    cursor.execute(create_channels_table)

    create_videos_table = """
    CREATE TABLE IF NOT EXISTS videos (
        video_id VARCHAR(50) PRIMARY KEY,
        channel_id VARCHAR(50),
        title VARCHAR(255),
        tags TEXT,
        thumbnail TEXT,
        description TEXT,
        published_date DATETIME,
        duration VARCHAR(50),
        view_count BIGINT,
        like_count BIGINT,
        comment_count BIGINT,
        favorite_count BIGINT,
        definition VARCHAR(50),
        caption_status VARCHAR(50),
        FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
    );
    """
    cursor.execute(create_videos_table)

    create_comments_table = """
    CREATE TABLE IF NOT EXISTS comments (
        comment_id VARCHAR(50) PRIMARY KEY,
        video_id VARCHAR(50),
        comment_text TEXT,
        comment_author VARCHAR(255),
        comment_published DATETIME,
        FOREIGN KEY (video_id) REFERENCES videos(video_id)
    );
    """
    cursor.execute(create_comments_table)

    connection.commit()
    cursor.close()
    connection.close()
    print("Database and tables created successfully.")

# Functions to fetch YouTube data
def get_channel_info(channel_id):
    request = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id)
    response = request.execute()

    # Check if 'items' is in the response
    if "items" in response and response["items"]:
        i = response["items"][0]
        data = dict(
            channel_name=i['snippet']['title'],
            channel_id=i['id'],
            subscribers=i['statistics']['subscriberCount'],
            views=i['statistics']['viewCount'],
            total_videos=i['statistics']['videoCount'],
            channel_description=i['snippet']['description'],
            playlist_id=i['contentDetails']['relatedPlaylists']['uploads']
        )
        return data
    else:
        print("Error: 'items' not found in response or the channel ID is invalid.")
        return None

def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(part="contentDetails", id=channel_id).execute()
    
    if "items" in response and response["items"]:
        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        next_page_token = None
        while True:
            response1 = youtube.playlistItems().list(
                part="snippet",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()
            for i in response1.get('items', []):
                video_ids.append(i['snippet']['resourceId']['videoId'])
            next_page_token = response1.get('nextPageToken')
            if next_page_token is None:
                break
    else:
        print("Error: 'items' not found in response or the channel ID is invalid.")
    return video_ids

def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(part="snippet,contentDetails,statistics", id=video_id)
        response = request.execute()
        for item in response.get("items", []):
            data = dict(
                channel_name=item['snippet']['channelTitle'],
                channel_id=item['snippet']['channelId'],
                video_id=item['id'],
                title=item['snippet']['title'],
                tags=item.get('tags', []),
                thumbnail=item['snippet']['thumbnails']['default']['url'],
                description=item.get('description'),
                published_date=item['snippet']['publishedAt'],
                duration=item['contentDetails']['duration'],
                view_count=item['statistics'].get('viewCount'),
                like_count=item['statistics'].get('likeCount'),
                comment_count=item['statistics'].get('commentCount'),
                favorite_count=item['statistics'].get('favoriteCount'),
                definition=item['contentDetails']['definition'],
                caption_status=item['contentDetails']['caption']
            )
            video_data.append(data)
    return video_data

def get_comment_info(video_ids):
    comments = []
    
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(part="snippet", videoId=video_id, maxResults=50)
            response = request.execute()

            for item in response.get("items", []):
                data = {
                    "comment_id": item['snippet']['topLevelComment']['id'],
                    "video_id": item['snippet']['topLevelComment']['snippet']['videoId'],
                    "comment_text": item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    "comment_author": item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    "comment_published": item['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                comments.append(data)

        except HttpError as e:
            # Check if it's the "comments disabled" error (403)
            if e.resp.status == 403 and "commentsDisabled" in str(e):
                print(f"Comments are disabled for video: {video_id}, skipping this video.")
            else:
                # Re-raise the error for other issues
                raise e
    
    return comments

# Functions to insert data into MySQL tables
def insert_channel_info(data):
    if data:
        connection = create_db_connection("youtube_db")
        cursor = connection.cursor()
        query = """
        INSERT INTO channels (channel_id, channel_name, subscribers, views, total_videos, channel_description, playlist_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            channel_name = VALUES(channel_name),
            subscribers = VALUES(subscribers),
            views = VALUES(views),
            total_videos = VALUES(total_videos),
            channel_description = VALUES(channel_description),
            playlist_id = VALUES(playlist_id)
        """
        cursor.execute(query, (
            data['channel_id'], data['channel_name'], data['subscribers'], data['views'],
            data['total_videos'], data['channel_description'], data['playlist_id']
        ))
        connection.commit()
        cursor.close()
        connection.close()

def convert_iso_to_mysql_datetime(iso_datetime):
    return datetime.strptime(iso_datetime, '%Y-%m-%dT%H:%M:%SZ')

# Insert video data
def insert_video_info(video_data):
    connection = create_db_connection()
    cursor = connection.cursor()

    query = """
    INSERT INTO videos (video_id, channel_id, title, tags, thumbnail, description, published_date,
                        duration, view_count, like_count, comment_count, favorite_count, definition, caption_status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        title = VALUES(title),
        tags = VALUES(tags),
        thumbnail = VALUES(thumbnail),
        description = VALUES(description),
        published_date = VALUES(published_date),
        duration = VALUES(duration),
        view_count = VALUES(view_count),
        like_count = VALUES(like_count),
        comment_count = VALUES(comment_count),
        favorite_count = VALUES(favorite_count),
        definition = VALUES(definition),
        caption_status = VALUES(caption_status)
    """
    for item in video_data:
        # Convert published_date to MySQL-compatible format
        published_date = convert_iso_to_mysql_datetime(item['published_date'])

        # Safely get the thumbnail URL, handling both dictionary and string formats
        thumbnail_url = (
            item['thumbnail']['default']['url'] if isinstance(item['thumbnail'], dict) and item['thumbnail'].get('default')
            else item['thumbnail'] if isinstance(item['thumbnail'], str)
            else None
        )

        cursor.execute(query, (
            item['video_id'], item['channel_id'], item['title'], ','.join(item.get('tags', [])),
            thumbnail_url, item['description'], published_date,
            item['duration'], item['view_count'], item['like_count'], item['comment_count'],
            item['favorite_count'], item['definition'], item['caption_status']
        ))
    connection.commit()
    cursor.close()
    connection.close()




def convert_iso_to_mysql_datetime(iso_datetime):
    # Remove the 'Z' at the end and convert the datetime string to MySQL format
    iso_datetime = iso_datetime.rstrip('Z')
    # Convert to MySQL-compatible datetime (if necessary, also add UTC offset)
    return datetime.strptime(iso_datetime, '%Y-%m-%dT%H:%M:%S')

# Insert comment data with corrected datetime format
def insert_comment_info(comment_data):
    connection = create_db_connection("youtube_db")
    cursor = connection.cursor()

    query = """
    INSERT INTO comments (comment_id, video_id, comment_text, comment_author, comment_published)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        comment_text = VALUES(comment_text),
        comment_author = VALUES(comment_author),
        comment_published = VALUES(comment_published)
    """
    for item in comment_data:
        # Convert the 'comment_published' field to MySQL-compatible format
        comment_published = convert_iso_to_mysql_datetime(item['comment_published'])

        cursor.execute(query, (
            item['comment_id'], item['video_id'], item['comment_text'],
            item['comment_author'], comment_published
        ))
    connection.commit()
    cursor.close()
    connection.close()
# Run the entire process
create_database_and_tables()

# Get channel information and insert into the database
channel_data = get_channel_info("UCZJHWJZmE2B_fPwt5HMjVjQ")  # Replace with the actual channel ID
if channel_data:
    insert_channel_info(channel_data)

    # Get video IDs for the channel and insert video details into the database
    video_ids = get_videos_ids(channel_data['channel_id'])
    video_data = get_video_info(video_ids)
    if video_data:
        insert_video_info(video_data)

    # Get comments for each video and insert into the database
    for video_id in video_ids:
        comment_data = get_comment_info([video_id])
        if comment_data:
            insert_comment_info(comment_data)

print("Data retrieval and insertion process completed.")


# In[10]:


query="use youtube_db"
cursor.execute(query)


# In[11]:


query="show tables"
cursor.execute(query)
for tb in cursor:
    print(tb)


# In[12]:


query="select * from channels"
cursor.execute(query)
for row in cursor:
    print(row)


# In[ ]:


query="select * from comments"
cursor.execute(query)
for row in cursor:
    print(row)


# In[13]:


connection = create_db_connection("youtube_db")
cursor = connection.cursor()

cursor.execute("SHOW TABLES;")
tables = cursor.fetchall()

print("Tables in the database:")
for table in tables:
    print(table[0])

cursor.close()
connection.close()


# In[14]:


# Define a function to fetch and display data from a table
def fetch_and_display_data(table_name):
    connection = create_db_connection("youtube_db")
    cursor = connection.cursor()
    
    cursor.execute(f"SELECT * FROM {table_name};")
    rows = cursor.fetchall()

    print(f"\nData in {table_name} table:")
    for row in rows:
        print(row)

    cursor.close()
    connection.close()
    
fetch_and_display_data("channels")



# In[6]:


get_ipython().system('streamlit run streamlit_app.py --server.port 8501 --server.headless true')


# In[ ]:




