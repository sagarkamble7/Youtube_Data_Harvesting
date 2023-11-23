from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


#API Key connection
def Api_connect():
    api_id = "AIzaSyAZ3ZNDTNSd1c6JTLWt3ZWWqngnWX0e7cg"
    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name,api_version,developerKey=api_id)

    return youtube

youtube = Api_connect()
    

# for channels information
def get_channel_info(channel_id):

    request = youtube.channels().list(
                        part = "snippet,ContentDetails,statistics",
                        id = channel_id)
    response = request.execute()

    for i in response['items']:
        data = dict(Channel_Name = i["snippet"]["title"],
                    Channel_Id = i["id"],
                    subscribers = i["statistics"]["subscriberCount"],
                    Views = i["statistics"]["viewCount"],
                    Total_Videos = i["statistics"]["videoCount"],
                    Channel_description = i["snippet"]["description"],
                    Playlist_id = i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data   


# for video ids
def get_videos_ids(channel_id):
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



# for video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request = youtube.videos().list(
            part = 'snippet,ContentDetails,statistics',
            id = video_id
        )
        response = request.execute()

        for item in response['items']:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_id = item['snippet']['channelId'],
                        video_id = item['id'],
                        title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet'].get('description'),
                        Published_date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        views = item['statistics'].get('viewCount'),
                        Likes = item['statistics'].get('likeCount'),
                        comment_counts = item.get('commentCount'),
                        fav_count = item['statistics']['favoriteCount'],
                        defination = item['contentDetails']['definition'],
                        captions_status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data
                


# get comment information
def get_comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
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


#upload  to mongoDB

client = pymongo.MongoClient("mongodb+srv://sagar72427:sagar72427@cluster0.pmsz4he.mongodb.net/?retryWrites=true&w=majority")
db = client['youtube_data']


def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = get_videos_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    comm_details = get_comment_info(vi_ids)
    
    coll1 = db['channel_details']
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                        "video_information":vi_details,"comment_information":comm_details})

    return 'upload completed successfully'


# table creation for channels, playlists, comments

# channel table

def channels_table(): 
    mydb = psycopg2.connect(host = 'localhost',
                            user = 'postgres',
                            password = '$@gaR72427',
                            database = 'youtube_data1',
                            port = '5432')
    cursor = mydb.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            subscribers int,
                                                            Views int,
                                                            Total_Videos int,
                                                            Channel_description text,
                                                            Playlist_id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print('channel tables already created')



    ch_list = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data['channel_information'])
    df = pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query = '''insert into channels(Channel_Name,
                                                Channel_Id,
                                                subscribers,
                                                Views,
                                                Total_Videos,
                                                Channel_description,
                                                Playlist_id)

                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                row['Channel_Id'],
                row['subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_description'],
                row['Playlist_id'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print('channels values are already inserted')


#playlist table

def playlist_table():
    mydb = psycopg2.connect(host = 'localhost',
                            user = 'postgres',
                            password = '$@gaR72427',
                            database = 'youtube_data1',
                            port = '5432')
    cursor = mydb.cursor()

    drop_query = '''drop table if exists playlists'''
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

    pl_list = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1 = pd.DataFrame(pl_list)


    for index,row in df1.iterrows():
        insert_query = '''insert into playlists(Playlist_id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                publishedAt,
                                                Video_Count
                                                )


                                                values(%s,%s,%s,%s,%s,%s)'''
        
        values = (row['Playlist_id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['publishedAt'],
                row['Video_Count']
                )
        
     
        cursor.execute(insert_query,values)
        mydb.commit()


#video table

def videos_table():

        mydb = psycopg2.connect(host = 'localhost',
                                user = 'postgres',
                                password = '$@gaR72427',
                                database = 'youtube_data1',
                                port = '5432')
        cursor = mydb.cursor()



        drop_query = '''drop table if exists videos'''
        cursor.execute(drop_query)
        mydb.commit()


        create_query = '''create table if not exists videos(Channel_Name varchar(100),
                                                        Channel_id varchar(100),
                                                        video_id varchar(30) primary key,
                                                        title varchar(150),
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        Published_date timestamp,
                                                        Duration interval,
                                                        views bigint,
                                                        Likes bigint,
                                                        comment_counts int,
                                                        fav_count int,
                                                        defination varchar(10),
                                                        captions_status varchar(50)
                                                        )'''


        cursor.execute(create_query)
        mydb.commit()


        vi_list = []
        db = client['youtube_data']
        coll1 = db['channel_details']
        for vi_data in coll1.find({},{"_id":0,"video_information":1}):
                for i in range(len(vi_data['video_information'])):
                        vi_list.append(vi_data['video_information'][i])
        df2 = pd.DataFrame(vi_list)





        for index,row in df2.iterrows():
                insert_query = '''insert into videos(Channel_Name,
                                                        Channel_id,
                                                        video_id,
                                                        title,
                                                        Tags,
                                                        Thumbnail,
                                                        Description,
                                                        Published_date,
                                                        Duration,
                                                        views,
                                                        Likes,
                                                        comment_counts,
                                                        fav_count,
                                                        defination,
                                                        captions_status
                                                        )
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        
        
                values = (row['Channel_Name'],
                        row['Channel_id'],
                        row['video_id'],
                        row['title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                        row['Published_date'],
                        row['Duration'],
                        row['views'],
                        row['Likes'],
                        row['comment_counts'],
                        row['fav_count'],
                        row['defination'],
                        row['captions_status'],
                        )
                
        
                cursor.execute(insert_query,values)
                mydb.commit()



#comments table

def comments_table():
        mydb = psycopg2.connect(host = 'localhost',
                                user = 'postgres',
                                password = '$@gaR72427',
                                database = 'youtube_data1',
                                port = '5432')
        cursor = mydb.cursor()

        drop_query = '''drop table if exists comments'''
        cursor.execute(drop_query)
        mydb.commit()


        create_query = '''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                                Video_id varchar(50), 
                                                                Comment_text text,
                                                                Comment_Author varchar(150),
                                                                comment_published timestamp
                                                        )'''


        cursor.execute(create_query)
        mydb.commit()

        co_list = []
        db = client['youtube_data']
        coll1 = db['channel_details']
        for co_data in coll1.find({},{"_id":0,"comment_information":1}):
                for i in range(len(co_data['comment_information'])):
                        co_list.append(co_data['comment_information'][i])
        df3 = pd.DataFrame(co_list)


        for index,row in df3.iterrows():
                insert_query = '''insert into comments(Comment_Id,
                                                                Video_id, 
                                                                Comment_text,
                                                                Comment_Author,
                                                                comment_published
                                                        )
                                                                values(%s,%s,%s,%s,%s)'''
                
                values = (row['Comment_Id'],
                        row['Video_id'],
                        row['Comment_text'],
                        row['Comment_Author'],
                        row['comment_published']
                        )
                
                
                cursor.execute(insert_query,values)
                mydb.commit()



def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return 'tables created successfully'


def show_channel_table():
    ch_list = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data['channel_information'])
    df = st.dataframe(ch_list)

    return df


def show_playlists_table():
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
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
            for i in range(len(vi_data['video_information'])):
                    vi_list.append(vi_data['video_information'][i])
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



#streamlit
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL TAKE AWAY")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL")

channel_id = st.text_input("Enter The CHannel Id")

if st.button('collect and store Data'):
    ch_ids = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])


    if channel_id in ch_ids:
        st.success("Channel Details of given channel ID already exists")

    else:
        insert = channel_details(channel_id)
        st.success(insert)


if st.button('Migrate to SQL'):
    Table = tables()
    st.success('Table')

show_table = st.radio('SELECT THE TABLE FOR THE VIEW',('CHANNELS','PLAYLISTS','VIDEOS','COMMENTS'))

if show_table =="CHANNELS":
    show_channel_table()


elif show_table == 'PLAYLISTS':
    show_playlists_table()


elif show_table == 'VIDEOS':
    show_video_table()



elif show_table == 'COMMENTS':
    show_comments_table()


#SQL connection:
mydb = psycopg2.connect(host = 'localhost',
                        user = 'postgres',
                        password = '$@gaR72427',
                        database = 'youtube_data1',
                        port = '5432')
cursor = mydb.cursor()


question=st.selectbox('Select your Question',('1. All the videos and the channel',
                                              '2. Channels with most number of videos',
                                              '3. 10 most viewd videos',
                                              '4. comments in each videos',
                                              '5. videos with highest likes',
                                              '6. likes of all videos',
                                              '7. views of each channel',
                                              '8. videos published in the year of 2022',
                                              '9. Average duration of videos in each channel',
                                              '10. videos with highest number of comment'))





