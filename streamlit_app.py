import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error
from googleapiclient.discovery import build

# Set Streamlit Page Configuration
st.set_page_config(page_title="YouTube Data Analyzer", layout="wide")

# Helper Functions
def create_db_connection(database="youtube_db"):
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Siva@210903",
            database=database
        )
        return connection
    except Error as e:
        st.error(f"Error connecting to database: {e}")
        return None

def get_channel_info(channel_id):
    api_key = "AIzaSyBlIG-XyCQMMvu02ml_-hKjFc1h4ycrvcY"
    youtube = build("youtube", "v3", developerKey=api_key)
    request = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id)
    response = request.execute()

    if "items" in response and response["items"]:
        item = response["items"][0]
        return {
            "channel_id": item["id"],
            "channel_name": item["snippet"]["title"],
            "subscribers": item["statistics"]["subscriberCount"],
            "views": item["statistics"]["viewCount"],
            "total_videos": item["statistics"]["videoCount"],
            "description": item["snippet"]["description"],
            "playlist_id": item["contentDetails"]["relatedPlaylists"]["uploads"]
        }
    return None

# Initialize session state for channel ID
if "channel_id" not in st.session_state:
    st.session_state["channel_id"] = None

# Tabs for Navigation
tabs = st.tabs(["🏠 Home", "📊 Fetched Data", "🔎 Query Execution"])

# Tab 1: Home
with tabs[0]:
    st.title("YouTube Data Analyzer")
    st.write("Fetch and analyze YouTube channel data.")

    st.subheader("Fetch YouTube Channel Data")
    channel_id = st.text_input("Enter YouTube Channel ID", placeholder="e.g., UC_x5XG1OV2P6uZZ5FSM9Ttw")

    if st.button("Fetch Data"):
        if channel_id.strip():
            channel_data = get_channel_info(channel_id)
            if channel_data:
                st.session_state["channel_id"] = channel_id  # Save the channel ID in session state
                connection = create_db_connection()
                if connection:
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
                        channel_data["channel_id"], channel_data["channel_name"], channel_data["subscribers"],
                        channel_data["views"], channel_data["total_videos"], channel_data["description"],
                        channel_data["playlist_id"]
                    ))
                    connection.commit()
                    cursor.close()
                    connection.close()
                    st.success(f"Data fetched and stored successfully for channel: **{channel_data['channel_name']}**")
                else:
                    st.error("Database connection failed.")
            else:
                st.error("Invalid Channel ID or data unavailable.")
        else:
            st.warning("Please enter a valid Channel ID.")

# Tab 2: Fetched Data
with tabs[1]:
    st.title("Fetched Data")
    st.write("View data stored in the database for the selected YouTube channel.")

    if not st.session_state["channel_id"]:
        st.warning("Please fetch data for a YouTube Channel ID on the Home tab first.")
    else:
        channel_id = st.session_state["channel_id"]
        table_choice = st.selectbox("Select Table to View", ["channels", "videos", "comments"])

        if st.button("Show Data"):
            connection = create_db_connection()
            if connection:
                cursor = connection.cursor()
                query = {
                    "channels": "SELECT * FROM channels WHERE channel_id = %s",
                    "videos": "SELECT * FROM videos WHERE channel_id = %s",
                    "comments": """
                        SELECT comments.* 
                        FROM comments
                        JOIN videos ON comments.video_id = videos.video_id
                        WHERE videos.channel_id = %s
                    """
                }[table_choice]

                cursor.execute(query, (channel_id,))
                result = cursor.fetchall()
                columns = [col[0] for col in cursor.description]
                cursor.close()
                connection.close()

                if result:
                    st.dataframe(pd.DataFrame(result, columns=columns))
                else:
                    st.warning(f"No data found in the `{table_choice}` table for this channel.")
            else:
                st.error("Database connection failed.")

# Tab 3: Query Execution
with tabs[2]:
    st.title("Query Execution")
    st.write("Run predefined queries on the database.")

    if not st.session_state["channel_id"]:
        st.warning("Please fetch data for a YouTube Channel ID on the Home tab first.")
    else:
        channel_id = st.session_state["channel_id"]  # Get the current channel ID from session state
        
        # Define queries with filtering for the specific channel ID where applicable
        queries = {
            "What are the names of all the videos and their corresponding channels?":
                """SELECT title AS Video_title, channel_name
                   FROM videos
                   JOIN channels ON channels.channel_id = videos.channel_id
                   WHERE channels.channel_id = %s;""",
                   
            "Which channels have the most number of videos, and how many videos do they have?":
                """SELECT channel_name, COUNT(video_id) AS video_count
                   FROM videos
                   JOIN channels ON channels.channel_id = videos.channel_id
                   GROUP BY channel_name
                   ORDER BY video_count DESC;""",
                   
            "What are the top 10 most viewed videos and their respective channels?":
                """SELECT title AS Video_title, channel_name, view_count
                   FROM videos
                   JOIN channels ON channels.channel_id = videos.channel_id
                   WHERE channels.channel_id = %s
                   ORDER BY view_count DESC
                   LIMIT 10;""",
                   
            "How many comments were made on each video, and what are their corresponding video names?":
                """SELECT title AS Video_title, COUNT(*) AS comment_counts
                   FROM videos
                   JOIN comments ON videos.video_id = comments.video_id
                   WHERE videos.channel_id = %s
                   GROUP BY title;""",
                   
            "Which videos have the highest number of likes, and what are their corresponding channel names?":
                """SELECT title AS Video_title, channel_name, like_count
                   FROM videos
                   JOIN channels ON channels.channel_id = videos.channel_id
                   WHERE channels.channel_id = %s
                   ORDER BY like_count DESC
                   LIMIT 1;""",
                   
            "What is the total number of likes for each video, and what are their corresponding video names?":
                """SELECT title AS Video_title, SUM(like_count) AS total_likes
                   FROM videos
                   WHERE channel_id = %s
                   GROUP BY title;""",
                   
            "What is the total number of views for each channel, and what are their corresponding channel names?":
                """SELECT channel_name, SUM(view_count) AS Total_views
                   FROM videos
                   JOIN channels ON channels.channel_id = videos.channel_id
                   WHERE channels.channel_id = %s
                   GROUP BY channel_name;""",
                   
            "What are the names of all the channels that have published videos in the year 2022?":
                """SELECT DISTINCT channels.channel_name
                   FROM channels
                   JOIN videos ON channels.channel_id = videos.channel_id
                   WHERE YEAR(published_date) = 2022 AND channels.channel_id = %s;""",
                   
            "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
                """SELECT channel_name, AVG(duration) AS Average_duration
                   FROM videos
                   JOIN channels ON videos.channel_id = channels.channel_id
                   WHERE channels.channel_id = %s
                   GROUP BY channel_name;""",
                   
            "Which videos have the highest number of comments, and what are their corresponding channel names?":
                """SELECT title AS Video_title, channel_name, comment_count
                   FROM videos
                   JOIN channels ON videos.channel_id = channels.channel_id
                   WHERE channels.channel_id = %s
                   ORDER BY comment_count DESC
                   LIMIT 1;"""
        }

        # Dropdown for selecting queries
        query_choice = st.selectbox("Select Query to Execute", list(queries.keys()))

        if st.button("Run Query"):
            connection = create_db_connection()
            if connection:
                cursor = connection.cursor()
                query = queries[query_choice]
                
                # Execute query with filtering for the specific channel_id where applicable
                if "%s" in query:
                    cursor.execute(query, (channel_id,))
                else:
                    cursor.execute(query)
                    
                result = cursor.fetchall()
                columns = [col[0] for col in cursor.description]
                cursor.close()
                connection.close()

                # Display query results
                if result:
                    st.dataframe(pd.DataFrame(result, columns=columns))
                else:
                    st.warning("No results found for this query.")
            else:
                st.error("Database connection failed.")




