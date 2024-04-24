# #packages and libraries:
from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
import mysql.connector
import pandas as pd
import streamlit as st

#API connect:

api_key = "AIzaSyAZ3ZNDTNSd1c6JTLWt3ZWWqngnWX0e7cg"
api_service_name = "youtube"
api_version = "v3"
youtube = build( api_service_name, api_version, developerKey = api_key )

# to get the channel details:

def get_channel_information(channel_id):
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id = channel_id
    )
    response = request.execute()

    for i in response['items']:
        data = dict(
        channel_id = i["id"],
        channel_name = i["snippet"]["title"],
        channel_desc = i["snippet"]["description"],
        piblish_date = i["snippet"]["publishedAt"],
        channel_playlist = i["contentDetails"]["relatedPlaylists"]["uploads"],
        view_count = i["statistics"]["viewCount"],
        subscriber_count = i["statistics"]["subscriberCount"],
        video_count = i["statistics"]["videoCount"]
        )
        return data
    
    #to get the video ids:
def get_video_id(channel_id):
    Video_Ids = []
    response = youtube.channels().list(id = channel_id,
                                            part = 'contentDetails').execute()
    Playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
            response1 = youtube.playlistItems().list(
                                                part = 'snippet',
                                                playlistId = Playlist_id,
                                                maxResults = 50,
                                                pageToken=next_page_token).execute()
            for i in range(len(response1['items'])):                                
                Video_Ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])     
            next_page_token = response1.get('nextPageToken')

            if next_page_token is None:
                break          
    return Video_Ids


# for video information:
def get_video_details(Video_Ids):
    video_data = []
    for video_id in Video_Ids:
            request = youtube.videos().list(
            part = 'snippet,contentDetails,statistics',
            id = video_id
        )
            response = request.execute()

            for item in response["items"]:
                    data = dict(video_id = item['id'],
                            Publishedat = item['snippet']['publishedAt'],
                            channel_id = item['snippet']['channelId'],
                            video_title = item['snippet']['title'],
                            description = item['snippet']['description'],
                            channel_name = item['snippet']['channelTitle'],
                            video_duration = item['contentDetails']['duration'],
                            captions = item['contentDetails'].get("caption"),
                            views = item['statistics']['viewCount'],
                            likes = item['statistics'].get("likeCount"),
                            defination = item['contentDetails']['definition'],
                            comments = item['statistics'].get('commentCount'))
                    video_data.append(data)
    return video_data


# for comment information:
def get_comment_info(Video_Ids):
    Comment_data = []
    try:
        for video_id in Video_Ids:
            request = youtube.commentThreads().list(
                part = 'snippet',
                videoId = video_id,
                maxResults = 50
            )
            response = request.execute()

            for item in response['items']:
                data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                            Video_id = item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_published = item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                        

    except:
        pass       
    return Comment_data 


# get playlist_details

def get_playlist_details(channel_id):
        next_page_token = None
        All_data = []
        while True:
                request = youtube.playlists().list(
                        part = 'snippet,contentDetails',
                        channelId = channel_id,
                        maxResults = 50,
                        pageToken = next_page_token
                )
                response = request.execute()


                for item in response['items']:
                        data = dict(Playlist_id = item['id'],
                                        Title = item['snippet']['title'],
                                        Channel_Id = item['snippet']['channelId'],
                                        Channel_Name = item['snippet']['channelTitle'],
                                        publishedAt = item['snippet']['publishedAt'],
                                        Video_Count = item['contentDetails']['itemCount'])
                        All_data.append(data)


                next_page_token = response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data


# MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["youtube_data"]

def channel_details(channel_id):
    ch_details = get_channel_information(channel_id)
    vi_ids = get_video_id(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_details = get_video_details(vi_ids)
    cmt_details = get_comment_info(vi_ids)

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information": ch_details, "playlist_information" : pl_details,
                      "video_details" : vi_details, "comment_information" : cmt_details})
    
    return "upload successfully"


# mySql connection:

#channel table

def channel_table():
    # Connect to MongoDB
    client = MongoClient('localhost', 27017)
    db = client['youtube_data']
    coll1 = db['channel_details']

    # Connect to MySQL
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Sagar72427',
        port=3306,
        database='youtube_database'
    )
    cursor = mydb.cursor()

    # Drop existing table
    drop_query = "DROP TABLE IF EXISTS channels"
    cursor.execute(drop_query)
    mydb.commit()

    # Create new table
    create_query = '''
    CREATE TABLE IF NOT EXISTS channels (
        channel_id VARCHAR(100) PRIMARY KEY,
        channel_name VARCHAR(100),
        channel_desc text,
        piblish_date TIMESTAMP,
        channel_playlist VARCHAR(100),
        view_count INT,
        subscriber_count INT,
        video_count INT
    )'''
    cursor.execute(create_query)
    mydb.commit()

    # Retrieve data from MongoDB and insert into MySQL
    ch_list = []
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data['channel_information'])
    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        formatted_publish_date = row['piblish_date'].replace('T', ' ').replace('Z', '')
        insert_query = '''
        INSERT INTO channels (
            channel_id,
            channel_name,
            channel_desc,
            piblish_date,
            channel_playlist,
            view_count,
            subscriber_count,
            video_count
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''

        values = (
            row['channel_id'],
            row['channel_name'],
            row['channel_desc'],
            formatted_publish_date,
            row['channel_playlist'],
            row['view_count'],
            row['subscriber_count'],
            row['video_count']
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print(f"Error inserting data: {e}")


# playlist table:
# MongoDB connection:
    client = MongoClient('localhost', 27017)
    db = client['youtube_data']
    coll1 = db['channel_details']


def playlist_table():
    mydb = mysql.connector.connect(host='localhost',
                                user='root',
                                password='Sagar72427',
                                port = 3306,
                                database='youtube_database')
    cursor = mydb.cursor()
    drop_query = "drop table if exists playlist"
    cursor.execute(drop_query)
    mydb.commit()
    
    create_query = '''create table if not exists playlists(Playlist_id varchar(100) primary key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                         publishedAt timestamp,
                                                        Video_Count int
                                                        )'''


    cursor.execute(create_query)
    mydb.commit()

   # Fetch playlist data from MongoDB
    pl_list = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for playlist_info in pl_data['playlist_information']:
            pl_list.append(playlist_info)
    df1 = pd.DataFrame(pl_list)

    for index,row in df1.iterrows():
        formatted_published_at = row['publishedAt'].replace('T', ' ').replace('Z', '')

        insert_query = '''insert into playlists(Playlist_id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                publishedAt,
                                                Video_Count
                                                )


                                                values(%s,%s,%s,%s,%s,%s)
                                                ON DUPLICATE KEY UPDATE
                                                Title = VALUES(Title),
                                                Channel_Id = VALUES(Channel_Id),
                                                Channel_Name = VALUES(Channel_Name),
                                                publishedAt = VALUES(publishedAt),
                                                Video_Count = VALUES(Video_Count)'''
        
        values = (row['Playlist_id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                formatted_published_at,
                row['Video_Count']
                )
        
     
        cursor.execute(insert_query,values)
        mydb.commit()

    

# videos_table
# videos_table
# Connect to MongoDB

client = MongoClient('localhost', 27017)
db = client['youtube_data']
coll1 = db['channel_details']


def video_table():
        mydb = mysql.connector.connect(host='localhost',
                                        user='root',
                                        password='Sagar72427',
                                        port = 3306,
                                        database='youtube_database')
        cursor = mydb.cursor()
        drop_query = '''drop table if exists videos'''
        cursor.execute(drop_query)
        mydb.commit()

        create_query = '''create table if not exists videos(video_id varchar(30) primary key,
                                                                Publishedat timestamp,
                                                                channel_id varchar(100),
                                                                video_title varchar(150),
                                                                description text,
                                                                channel_name varchar(100),
                                                                video_duration VARCHAR(100) ,
                                                                captions varchar(50),
                                                                views int,
                                                                likes int,
                                                                defination varchar(10), 
                                                                comments int)'''


        cursor.execute(create_query)
        mydb.commit()


        vi_list = []
        db = client['youtube_data']
        coll1 = db['channel_details']
        for vi_data in coll1.find({},{"_id":0,"video_details":1}):
                for i in range(len(vi_data['video_details'])):
                        vi_list.append(vi_data['video_details'][i])
        df2 = pd.DataFrame(vi_list)





        for index,row in df2.iterrows():
                formatted_published_at = row['Publishedat'].replace('T', ' ').replace('Z', '')
                insert_query = '''insert into videos(video_id ,
                                                Publishedat ,
                                                channel_id,
                                                video_title, 
                                                description ,
                                                channel_name ,
                                                video_duration ,
                                                captions,
                                                views ,
                                                likes ,
                                                defination , 
                                                comments )
                                                values( %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s)'''



                values = (row['video_id'],
                        formatted_published_at,
                        row['channel_id'],
                        row['video_title'],
                        row['description'],
                        row['channel_name'],
                        row['video_duration'],
                        row['captions'],
                        row['views'],
                        row['likes'],
                        row['defination'],
                        row['comments']
                        )


                cursor.execute(insert_query,values)
                mydb.commit()


#comments_table:
def comments_table():
    mydb = mysql.connector.connect(host='localhost',
                                            user='root',
                                            password='Sagar72427',
                                            port = 3306,
                                            database='youtube_database')
    cursor = mydb.cursor()
    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists comments(Comment_Id varchar(100) primary key, 
                                                            Video_id varchar(100), 
                                                            Comment_text text, 
                                                            Comment_Author varchar(100), 
                                                            comment_published timestamp)'''
    cursor.execute(create_query)
    mydb.commit()

    comments = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for comment_data in coll1.find({}, {"_id": 0, "comment_information":1}):
        for cmnt in range(len(comment_data["comment_information"])):
            comments.append(comment_data["comment_information"][cmnt])
    df3 = pd.DataFrame(comments)

    for index, row in df3.iterrows():
        formatted_published_at = row['comment_published'].replace('T', ' ').replace('Z', '')
        insert_query = '''insert into comments(Comment_Id, 
                                                Video_id, 
                                                Comment_text, 
                                                Comment_Author, 
                                                comment_published)
                                                values(%s,%s,%s,%s,%s)'''
        
        values = (row['Comment_Id'],
                row['Video_id'],
                row['Comment_text'],
                row['Comment_Author'],
                formatted_published_at)
        
        cursor.execute(insert_query, values)
        mydb.commit()


def tables():
    channel_table()
    playlist_table()
    video_table()
    comments_table()

    return "tables created succesfully"

#stream show tables code:

def show_channel_details():
    ch_list = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data['channel_information'])
    df = st.dataframe(ch_list)

    return df

def show_playlist_table():
    pl_list = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1 = st.dataframe(pl_list)

    return df1

def show_video_table():
        vi_list = []
        db = client['youtube_data']
        coll1 = db['channel_details']
        for vi_data in coll1.find({},{"_id":0,"video_details":1}):
                for i in range(len(vi_data['video_details'])):
                        vi_list.append(vi_data['video_details'][i])
        df2 = st.dataframe(vi_list)

        return df2

def show_comments_table():
    co_list = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for co_data in coll1.find({},{"_id":0,"comment_information":1}):
            for i in range(len(co_data['comment_information'])):
                    co_list.append(co_data['comment_information'][i])
    df3 = st.dataframe(co_list)

    return df3


# streamlit code
st.title(":red[Yotube]:black[DataHarvesting]")
channel_Id = st.text_input("Enter the ChannelId")


if st.button('collect and store Data'):
    ch_list = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data['channel_information']["channel_id"])


    if channel_Id in ch_list:
        st.success("Channel Details of given channel ID already exists")

    else:
        insert = channel_details(channel_Id)
        st.success(insert)


    if st.button('Migrate to SQL'):
        Table = tables()
        st.success('Table')
    
show_table = st.radio('SELECT THE TABLE FOR THE VIEW',('CHANNELS','PLAYLISTS','VIDEOS','COMMENTS'))

if show_table =="CHANNELS":
    show_channel_details()
elif show_table == 'PLAYLISTS':
    show_playlist_table()
elif show_table == 'VIDEOS':
    show_video_table()
elif show_table == 'COMMENTS':
    show_comments_table()


# SQL_connection for SQL Q&A:
mydb = mysql.connector.connect(host='localhost',
                                            user='root',
                                            password='Sagar72427',
                                            port = 3306,
                                            database='youtube_database')
cursor = mydb.cursor()

question = st.selectbox('select your Question', ('1. All the videos and the channel',
                                                 '2. Channels with most numbers of videos and how many videos',
                                                 '3. Top 10 videos of channel',
                                                 '4. comments on each video',
                                                 '5. Highest liked video and their corresponding channel name',
                                                 '6. Likes Of Each Video And Thier corresponing Video Names',
                                                 '7. Total Number of views for each  channel and channel names',
                                                 '8. Channel Names that published Videos in year 2022',
                                                 '9. average duration of video and their channel name',
                                                 '10. viedos with highest comments and its channel name'
                                                    ))

if question =='1. All the videos and the channel':
     query1 = '''select video_title as videoName, channel_name as ChannelName from videos'''
     cursor.execute(query1)
     t1 = cursor.fetchall()
     mydb.commit()
     df1 = pd.DataFrame(t1, columns= ['video Title','Channel Name'])
     st.write(df1)    

elif question == '2. Channels with most numbers of videos and how many videos':
     query2 = '''select video_count as Number_of_videos, channel_name as Channel_Name from channels order by video_count desc'''
     cursor.execute(query2)
     t2 = cursor.fetchall()
     mydb.commit()
     df2 = pd.DataFrame(t2, columns= ["Number of Videos", "Channel Name"])
     st.write(df2)

elif question == '3. Top 10 videos of channel':
     query3 = '''select video_title as Video_title ,channel_name as Channel_name, views as Views from videos order by views desc'''
     cursor.execute(query3)
     t3 = cursor.fetchall()
     mydb.commit()
     df3 = pd.DataFrame(t3, columns=["Video Name", "Channel Name", "Views"])
     st.write(df3)

elif question == '4. comments on each video':
     query4 = '''select comments as Comment_Count, video_title as Video_Name from videos'''
     cursor.execute(query4)
     t4 = cursor.fetchall()
     mydb.commit()
     df4 = pd.DataFrame(t4, columns= ["Comment Counts", "Video Name"])
     st.write(df4)

elif question == '5. Highest liked video and their corresponding channel name':
     query5 = '''select video_title as Video_name, likes as Likes_Count, channel_name as Channel_Name from videos order by likes desc'''
     cursor.execute(query5)
     t5 = cursor.fetchall()
     mydb.commit()
     df5 = pd.DataFrame(t5, columns=["Video Name", "Number Of Likes", "Channel Name"])
     st.write(df5)

elif question== "6. Likes Of Each Video And Thier corresponing Video Names":
     query6 = '''select likes as Total_Likes, video_title as Video_name from Videos'''
     cursor.execute(query6)
     t6 = cursor.fetchall()
     mydb.commit()
     df6 = pd.DataFrame(t6, columns=["Like Count", "Video Name"])
     st.write(df6)

elif question == '7. Total Number of views for each  channel and channel names':
     query7 = "select channel_name as Channel_name, view_count as Total_Views from channels"
     cursor.execute(query7)
     t7 = cursor.fetchall()
     mydb.commit()
     df7 = pd.DataFrame(t7, columns=["Channel Name", "Total Views"])
     st.write(df7)

elif question == '8. Channel Names that published Videos in year 2022':
    query8 = "select channel_name as Channel_name from videos where extract(year from Publishedat) = 2022"
    cursor.execute(query8)
    t8 = cursor.fetchall()
    mydb.commit()
    df8 = pd.DataFrame(t8, columns=["channel_name"])
    st.write(df8)

elif question ==  '9. average duration of video and their channel name':
    query9 = '''select avg(video_duration) as avg_duration, channel_name as channel_name from videos group by channel_name'''
    cursor.execute(query9)
    t9= cursor.fetchall()
    mydb.commit()
    df9 = pd.DataFrame(t9, columns=["Average Duration", "Channel name"])
    st.write(df9)

elif question=='10. viedos with highest comments and its channel name':
     query10 = '''select video_title as video_name, comments as comments, channel_name from videos order by comments desc'''
     cursor.execute(query10)
     t10 = cursor.fetchall()
     mydb.commit()
     df10 = pd.DataFrame(t10, columns = ["video name", "comment count", "channel name"])
     st.write(df10)