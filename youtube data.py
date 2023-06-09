#!/usr/bin/env python
# coding: utf-8

# In[2]:


pip install google-api-python-client 


# In[3]:


from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json
import pandas as pd
import re


# In[4]:


api_service_name='youtube'
api_version='v3'
api_key='AIzaSyD-7fhxzS080zzm7gPsMIni1PUa1qgOGdE'  # Replace own API key
youtube = build('youtube', 'v3', developerKey=api_key)


# In[5]:


channel_ids="UCk3JZr7eS3pg5AGEvBdEvFg" #village cooking channel


# In[6]:


def get_channel_status(youtube, channel_ids):
    all_data=[]
   
    request=youtube.channels().list(
            part='snippet,contentDetails,statistics',
            id=channel_ids)
    
    response=request.execute()
    
    for i in range(len(response['items'])):
        data={
            'channel_name' : response['items'][i]['snippet']['title'],
            'channel_id':response['items'][i]['id'],
            'channel_video_count' :int( response['items'][i]['statistics']['videoCount']),
            'channel_subscriber_count' :int( response['items'][i]['statistics']['subscriberCount']),
            'channel_view_count' : int(response['items'][i]['statistics']['viewCount']),
            'channel_description' : response['items'][i]['snippet']['description'],
            'channel_playlist_id' : response['items'][i]['contentDetails']['relatedPlaylists']['uploads']
            }
        all_data.append(data)
    
    return all_data


# In[7]:


channel_details=get_channel_status(youtube, channel_ids)


# In[8]:


channel_data=pd.DataFrame(channel_details)


# In[9]:


channel_data


# In[12]:


channel_data['channel_playlist_id'][0]


# In[13]:


def get_video_ids(youtube, channel_playlist_id):
    request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=channel_playlist_id,
        maxResults=50
    )
    response = request.execute()

    video_data = []

    for i in range(len(response['items'])):
        
        video_data.append(response['items'][i]['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    more_pages = True

    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=channel_playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()

            for i in range(len(response['items'])):
                video_data.append(response['items'][i]['contentDetails']['videoId'])

            next_page_token = response.get('nextPageToken')

    return video_data


# In[14]:


video_ids=get_video_ids(youtube, channel_data['channel_playlist_id'][0])


# In[15]:


playlist_details=pd.DataFrame(video_ids)


# In[16]:


playlist_details


# In[19]:


def convert_duration(duration):
           regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
           match = re.match(regex, duration)
           if not match:
               return '00:00:00'
           hours, minutes, seconds = match.groups()
           hours = int(hours[:-1]) if hours else 0
           minutes = int(minutes[:-1]) if minutes else 0
           seconds = int(seconds[:-1]) if seconds else 0
           total_seconds = hours * 3600 + minutes * 60 + seconds
           return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60), int(total_seconds % 60))


# In[113]:


def get_video_details(youtube, video_ids):
    video_status = []

    for video_id in video_ids:
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        )
        response = request.execute()

        video_data = response['items'][0]

        try:
            video_data['comment_threads'] = get_video_comments(youtube, video_id, max_comments=2)
        except:
            video_data['comment_threads'] = None

        duration = video_data.get('contentDetails', {}).get('duration', 'Not Available')
        if duration != 'Not Available':
            duration = convert_duration(duration)
        video_data['contentDetails']['duration'] = duration

        video = {
            'Video_Id': video_data['id'],  # Change 'id' to 'Video_Id'
            'title': video_data['snippet']['title'],
            'description': video_data['snippet']['description'],
            'tags': str(video_data['snippet'].get(None)),
            'published_at': video_data['snippet']['publishedAt'],
            'view_count': int(video_data['statistics']['viewCount']),
            'like_count': int(video_data['statistics'].get('likeCount', 0)),
            'dislike_count': video_data['statistics'].get('dislikeCount', 0),
            'favorite_count': video_data['statistics'].get('favoriteCount', 0),
            'comment_count': int(video_data['statistics'].get('commentCount', 0)),
            'duration': video_data.get('contentDetails', {}).get('duration', 'Not Available'),
            'thumbnail': video_data['snippet']['thumbnails']['high']['url'],
            'caption_status': video_data.get('contentDetails', {}).get('caption', 'Not Available'),
}


        video_status.append(video)

    return video_status


# In[114]:


video_statistics=get_video_details(youtube, video_ids)


# In[115]:


video_data=pd.DataFrame(video_statistics)


# In[116]:


video_data['published_at'] = pd.to_datetime(video_data['published_at'])
video_data['published_at'] = video_data['published_at'].dt.strftime('%Y-%m-%d')
video_data


# In[117]:


def get_comments_in_video(youtube, video_ids, max_comments=50):
    comments = []

    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=max_comments
            )
            response = request.execute()

            for comment in response['items'][:max_comments]:
                comment_information = {
                    "Video_Id": video_id,
                    "Comment_Id": comment['snippet']['topLevelComment']['id'],
                    "Comment_Text": comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                    "Comment_Author": comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    "Comment_PublishedAt": comment['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                comments.append(comment_information)

        except HttpError as e:
            if e.resp.status == 403 and 'commentsDisabled' in str(e):
                print(f"Comments are disabled for video ID: {video_id}")
            else:
                print(f"An error occurred for video ID: {video_id}, error: {str(e)}")

    return comments


# In[118]:


comments_info=get_comments_in_video(youtube, video_ids)


# In[119]:


comments_data=pd.DataFrame(comments_info)


# In[120]:


comments_data['Comment_PublishedAt'] = pd.to_datetime(comments_data['Comment_PublishedAt'])
comments_data['Comment_PublishedAt'] = comments_data['Comment_PublishedAt'].dt.strftime('%Y-%m-%d')
comments_data


# In[121]:


a=channel_data.to_dict(orient="records")


# In[122]:


b=video_data.to_dict(orient="records")


# In[123]:


cm=comments_data.to_dict(orient="records")


# In[124]:


xyz={"channel_details":a,"video_details":b,"comment_details":cm}


# In[125]:


xyz


# In[126]:


import pymongo


# In[127]:


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["youtube2"]
mycol = mydb["data2"]



mycol.insert_one(xyz)


# In[128]:


get_ipython().system('pip install mysql-connector-python')


# In[129]:


import mysql.connector
import pandas as pd


# In[130]:


mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Jamesbond@007",
    autocommit=True
)

mycursor = mydb.cursor(buffered=True)
mycursor.execute("CREATE DATABASE msd")
mycursor.execute("USE msd")
mycursor.execute("CREATE TABLE summa (channel_name VARCHAR(255), channel_id VARCHAR(255), channel_video_count INT, channel_subscriber_count INT, channel_view_count BIGINT, channel_description VARCHAR(1000), channel_playlist_id VARCHAR(255), PRIMARY KEY(channel_id))")

channel_data_tuples = [tuple(row) for row in channel_data.itertuples(index=False)]
insert_query = "INSERT INTO summa (channel_name, channel_id, channel_video_count, channel_subscriber_count, channel_view_count, channel_description, channel_playlist_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
mycursor.executemany(insert_query, channel_data_tuples)
mydb.commit()
mycursor.close()
mydb.close()


# In[131]:


mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Jamesbond@007",
    autocommit=True
)
mycursor = mydb.cursor(buffered=True)

# Selecting the database to use
mycursor.execute("USE msd")

# Creating the table with primary key and foreign key
mycursor.execute("CREATE TABLE playlist_details ("
                 "playlist_id INT AUTO_INCREMENT PRIMARY KEY,"
                 "channel_id VARCHAR(255),"
                 "channel_playlist_id VARCHAR(255),"
                 "FOREIGN KEY (channel_id) REFERENCES summa(channel_id)"
                 ")")

# Assuming you have the necessary data stored in variables
playlist_id = 1  # Assuming you have a playlist ID
channel_id = "UCk3JZr7eS3pg5AGEvBdEvFg"
channel_playlist_id = "UUk3JZr7eS3pg5AGEvBdEvFg"

# Checking if the channel_id exists in the summa table
check_query = "SELECT channel_id FROM summa WHERE channel_id = %s"
check_value = (channel_id,)

mycursor.execute(check_query, check_value)
result = mycursor.fetchone()

if result:
    # The channel_id exists, so we can proceed with the insert operation
    insert_query = "INSERT INTO playlist_details (playlist_id, channel_id, channel_playlist_id) VALUES (%s, %s, %s)"
    values = (playlist_id, channel_id, channel_playlist_id)

    mycursor.execute(insert_query, values)
    mydb.commit()
else:
    print(f"Channel with ID {channel_id} does not exist in the summa table.")

mycursor.close()
mydb.close()


# In[132]:


mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Jamesbond@007",
    autocommit=True
)
mycursor = mydb.cursor(buffered=True)

# Selecting the database to use
mycursor.execute("USE msd")

# Creating the 'video_details' table with primary key and foreign key
mycursor.execute("CREATE TABLE video_details ("
                 "Video_Id VARCHAR(255) PRIMARY KEY,"
                 "title VARCHAR(255),"
                 "description VARCHAR(1000),"
                 "tags TEXT,"
                 "published_at DATETIME,"
                 "view_count INT,"
                 "like_count INT,"
                 "dislike_count INT,"
                 "favorite_count INT,"
                 "comment_count INT,"
                 "duration VARCHAR(50),"
                 "thumbnail VARCHAR(255),"
                 "caption_status VARCHAR(50),"
                 "playlist_id INT,"
                 "FOREIGN KEY (playlist_id) REFERENCES playlist_details(playlist_id)"
                 ")")
video_data_tuples = [tuple(row) for row in video_data.itertuples(index=False)]
insert_query = "INSERT INTO video_details (Video_Id, title, description, tags, published_at, view_count, like_count, " \
               "dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status, playlist_id) " \
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
# Assuming you have the necessary data stored in variables
playlist_id = 1  # Assuming you have a playlist ID

# Assigning the playlist ID to all video data tuples
video_data_tuples_with_playlist = [(Video_Id, title, description, tags, published_at, view_count, like_count,
                                    dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status,
                                    playlist_id) for (Video_Id, title, description, tags, published_at, view_count,
                                                      like_count, dislike_count, favorite_count, comment_count,
                                                      duration, thumbnail, caption_status) in video_data_tuples]
mycursor.executemany(insert_query, video_data_tuples_with_playlist)
mydb.commit()


# In[133]:


import mysql.connector

# Connect to MySQL database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Jamesbond@007",
    autocommit=True
)
mycursor = mydb.cursor(buffered=True)

# Select the database to use
mycursor.execute("USE try_1")

# Create the 'comment_details' table if it doesn't exist
mycursor.execute("CREATE TABLE IF NOT EXISTS comment_details ("
                 "Comment_Id VARCHAR(255) PRIMARY KEY,"
                 "Video_Id VARCHAR(255),"
                 "Comment_Text VARCHAR(1000),"
                 "Comment_Author VARCHAR(255),"
                 "Comment_PublishedAt DATETIME,"
                 "FOREIGN KEY (Video_Id) REFERENCES video_details(Video_Id)"
                 ")")

# Assuming you have the necessary data stored in the `comments_info` list
comments_info = [
    {"Comment_Id": "comment_id_1", "Video_Id": "video_id_1", "Comment_Text": "Comment 1", "Comment_Author": "Author 1", "Comment_PublishedAt": "2023-01-01 12:00:00"},
    {"Comment_Id": "comment_id_2", "Video_Id": "video_id_2", "Comment_Text": "Comment 2", "Comment_Author": "Author 2", "Comment_PublishedAt": "2023-01-02 12:00:00"},
    {"Comment_Id": "comment_id_3", "Video_Id": "video_id_1", "Comment_Text": "Comment 3", "Comment_Author": "Author 3", "Comment_PublishedAt": "2023-01-03 12:00:00"},
]

# Insert comment details into the 'comment_details' table
insert_comment_query = "INSERT INTO comment_details (Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_PublishedAt) " \
                       "VALUES (%s, %s, %s, %s, %s)"

for comment in comments_info:
    comment_tuple = (comment["Comment_Id"], comment["Video_Id"], comment["Comment_Text"],
                     comment["Comment_Author"], comment["Comment_PublishedAt"])
    mycursor.execute(insert_comment_query, comment_tuple)

mydb.commit()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




