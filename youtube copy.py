import googleapiclient.discovery
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

# API KEY CONNECTION

api_service_name = "youtube"
api_version = "v3"
Api_key = "key should be given"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=Api_key)



# GET CHANNEL INFORMATION
def get_channel_details(channel_id):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id = channel_id
        )
    response = request.execute()
    for item in response["items"]:

        data=dict(Channel_name= item['snippet']['title'],
            Channel_id=item['id'],
            Published_at=item['snippet']['publishedAt'],
            Subscribers=item['statistics']['subscriberCount'],
            Views=item['statistics']['viewCount'],
            Total_videos= item['statistics']['videoCount'],
            Channel_description= item['snippet']['description'],
            Playlist_id=item['contentDetails']['relatedPlaylists']['uploads'])
    return data     



#GET VIDEO_IDS
def get_video_ids(channel_id):
    video_ids=[]

    response = youtube.channels().list(id=channel_id,part="contentDetails").execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None

    while True:       
        response_play1= youtube.playlistItems().list(part="snippet",
                                                            playlistId =Playlist_Id,
                                                            maxResults=50,
                                                            pageToken=next_page_token).execute()
        for i in range(len(response_play1['items'])):
            video_ids.append(response_play1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response_play1.get('nextPageToken')
        if next_page_token is None:
            break       
    return video_ids


# TO GET VIDEO INFORMATION
def get_video_info(Video_Ids):   
    video_data =[]   #to append all details of video
    
    for video_id in Video_Ids: # to get all videos id details
        request = youtube.videos().list(part="snippet,contentDetails,statistics",id=video_id)
        response = request.execute()
        for i in range(len(response['items'])): #nested loop to get details of eachids in video_details

            data = {"Channel_name" : response['items'][i]['snippet']['channelTitle'],
                    "Channel_id" : response['items'][i]['snippet']['channelId'],
                    "Video_id " :response['items'][i]['id'],
                    "Title": response['items'][i]['snippet']['title'],
                    "Tags" :response['items'][i]['snippet'].get('tags'),
                    "Thumbnail" : response['items'][i]['snippet']['thumbnails']['default']['url'],
                    "Description" :response['items'][i]['snippet']['description'],
                    "Published_at" : response['items'][i]['snippet']['publishedAt'],
                    "Duration" :response['items'][i]['contentDetails']['duration'],
                    "Definition" :response['items'][i]['contentDetails']['definition'],
                    "caption_status" : response['items'][i]['contentDetails']['caption'],
                    "Views" :response['items'][i]['statistics'].get('viewCount'), 
                    "Likes" : response['items'][i][ 'statistics']['likeCount'],
                    "Favourites" : response['items'][i][ 'statistics']['favoriteCount'],
                    "Comment" : response['items'][i]['statistics'].get('commentCount')
                }
            video_data.append(data)
    return video_data


# GET COMMENTS INFORMATION
def get_comment_info(Video_Ids):
        comment_data = []
        try:
                for video_id in Video_Ids:
                        request = youtube.commentThreads().list(part = 'snippet',
                                                                        videoId =video_id,
                                                                        maxResults=100
                                )
                        response = request.execute()

                        for i in range(len(response['items'])):
                                data = {"Comment_id" : response['items'][i]['snippet']['topLevelComment']['id'],
                                        "video_id" : response['items'][i]['snippet']['topLevelComment']['snippet']['videoId'],
                                        "comments_text" : response['items'][i]['snippet']['topLevelComment']['snippet'][ 'textDisplay'],
                                        "comment_author" :response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                        "Comment_time" :response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']
                                        }
                                comment_data.append(data)
        except:
                pass

        return comment_data


# GETTING PLAYLIST INFORMATION
def get_playlist_info(channel_id):
    play_data =[]
    next_page_token = None
    while True:
        request = youtube.playlists().list(part="snippet,contentDetails",
                                        channelId=channel_id,
                                        maxResults = 50,
                                        pageToken=next_page_token
                                        )

        response = request.execute()

        for i in range(len(response['items'])):
            data = {"playlist_id" : response['items'][i]['id'],
                    "titles" : response['items'][i]['snippet']['title'],
                    "channel_id" : response['items'][i]['snippet']['channelId'],
                    "channel_name" : response['items'][i]['snippet']['channelTitle'],
                    "channel_published" : response['items'][i]['snippet']['publishedAt'],
                    "video_count" : response['items'][i]['contentDetails']['itemCount']
                }
            play_data.append(data)
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
    return play_data


def channel_details(channel_id):
    ch_details =get_channel_details(channel_id)
    pl_details =get_playlist_info(channel_id)
    vi_ids = get_video_ids(channel_id)
    vi_details= get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    collection = db['channel_details']
    collection.insert_one({ "Channel Information" : ch_details,"Playlist_information" : pl_details,
                           "Video_information" : vi_details,"Comment_information" : com_details})
    return "Data Uploaded Successfully"


# CONNECTING TO MONGO DB
client = pymongo.MongoClient("mongodb://mongodb username and pwd given")
db = client["youtube_data"]
coll= db["channel_details"]


# CHANNEL TABLE
def get_channels_table(single_channel_name):
    mydb = psycopg2.connect(host ="localhost",
                                            user ="postgres",
                                            password = "guvi2024",
                                            database = "youtube",
                                            port ="5432"
                                        )    

    cursor = mydb.cursor()

                    
    create_query ='''create table if not exists channels(Channel_name varchar(100),
                                                                Channel_id varchar(100) primary key,
                                                                Published_at timestamp,
                                                                Subscribers int,
                                                                Views int,
                                                                Total_videos int,
                                                                Channel_description text)'''
    cursor.execute(create_query )
    mydb.commit()

    #GET CHANNELS DETAILS IN  MONGODB

    single_detail=[]
    db = client["youtube_data"]
    channel_coll= db["channel_details"] 
    for item in channel_coll.find({"Channel Information.Channel_name":single_channel_name},{"_id":0}):
        single_detail.append(item['Channel Information'])
    df_single_channel_details=pd.DataFrame(single_detail)


# Loop through each row in the DataFrame and insert or update values into the PostgreSQL table
    for index, row in df_single_channel_details.iterrows():
            insert_query = '''INSERT INTO channels (Channel_name,
                                                        Channel_id, 
                                                        Published_at, 
                                                        Subscribers, 
                                                        Views, 
                                                        Total_videos, 
                                                        Channel_description) 
                                                        
                                                        VALUES (%s, %s, %s, %s, %s, %s, %s) 
                                                        ON CONFLICT (Channel_id) DO NOTHING'''  # Skip inserting if Channel_id already exists
                
            # Extracting values from the DataFrame row
            values = (row['Channel_name'], 
                        row['Channel_id'], 
                        row['Published_at'], 
                        row['Subscribers'], 
                        row['Views'], 
                        row['Total_videos'], 
                        row['Channel_description'])
            try:        
                cursor.execute(insert_query, values)
                mydb.commit()
            except:
                news = f"Your Provided Channel Name {single_channel_name} is already exists"
                return news



def get_playlist_table(single_channel_name):
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="guvi2024",
                            database="youtube",
                            port="5432"
                            )

    cursor = mydb.cursor()

    create_query = '''CREATE TABLE IF NOT EXISTS playlists(
                            playlist_id VARCHAR(100) PRIMARY KEY,
                            titles VARCHAR(100),
                            channel_id VARCHAR(100),
                            channel_name VARCHAR(100),
                            channel_published TIMESTAMP,
                            video_count INT
                        )'''
    
    cursor.execute(create_query)
    mydb.commit()

    single_playlist_details=[]
    db = client["youtube_data"]
    ch_data = db["channel_details"] 
    for item in ch_data.find({"Channel Information.Channel_name":single_channel_name},{"_id":0}):
        single_playlist_details.append(item['Playlist_information'])
    df_single_playlist_details=pd.DataFrame(single_playlist_details[0])

    for index, row in df_single_playlist_details.iterrows():
        
        playlist_id = row['playlist_id']
        titles = row['titles']
        channel_id = row['channel_id']
        channel_name = row['channel_name']
        channel_published = row['channel_published']
        video_count = row['video_count']

        insert_query = '''INSERT INTO playlists (playlist_id, titles, channel_id, channel_name, channel_published, video_count)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (playlist_id) DO NOTHING'''

        record_to_insert = (playlist_id, titles, channel_id, channel_name, channel_published, video_count)
        
        cursor.execute(insert_query, record_to_insert)
        mydb.commit()



def get_videos_table(single_channel_name):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="guvi2024",
        database="youtube",
        port="5432"
    )

    cursor = mydb.cursor()

    create_query = '''CREATE TABLE IF NOT EXISTS videos(
                        Channel_name varchar(100),
                        Channel_id varchar(100),
                        Video_id varchar(100) PRIMARY KEY,
                        Title varchar(100),
                        Tags text,
                        Thumbnail varchar(200),
                        Description text,
                        Published_at timestamp,
                        Duration interval,
                        Definition text,
                        caption_status varchar(100),
                        Views int,
                        Likes int,
                        Favourites int,
                        Comment int
                    )'''

    cursor.execute(create_query)
    mydb.commit()

    single_video_details = []
    db = client["youtube_data"]
    ch_data = db["channel_details"]
    for item in ch_data.find({"Channel Information.Channel_name":single_channel_name}, {"_id": 0}):
        single_video_details.append(item['Video_information'])

    df_single_video_details = pd.DataFrame(single_video_details[0])

    for index, row in df_single_video_details.iterrows():
        Channel_name = row['Channel_name']
        Channel_id = row['Channel_id']
        Video_id = row['Video_id ']
        Title = row['Title']
        Tags = row['Tags']
        Thumbnail = row['Thumbnail']
        Description = row['Description']
        Published_at = row['Published_at']
        Duration = row['Duration']
        Definition = row['Definition']
        caption_status = row['caption_status']
        Views = row['Views']
        Likes = row['Likes']
        Favourites = row['Favourites']

        # Convert empty or non-integer 'Comment' values to None
        try:
            Comment = int(row['Comment'])
        except (ValueError, TypeError):
            Comment = None

        insert_query = '''INSERT INTO videos (Channel_name, 
                                            Channel_id,
                                            Video_id, Title, 
                                            Tags, 
                                            Thumbnail, 
                                            Description,
                                            Published_at, 
                                            Duration, 
                                            Definition, 
                                            caption_status, 
                                            Views, 
                                            Likes, 
                                            Favourites, 
                                            Comment)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                            ON CONFLICT (Video_id) DO NOTHING'''

        record_to_insert = (Channel_name, Channel_id, Video_id, Title, Tags, Thumbnail, Description, Published_at, Duration, Definition, caption_status, Views, Likes, Favourites, Comment)

        cursor.execute(insert_query, record_to_insert)
        mydb.commit()
        

#COMMENTS TABLE
def get_comments_table(single_channel_name):
    mydb = psycopg2.connect(host ="localhost",
                                    user ="postgres",
                                    password = "guvi2024",
                                    database = "youtube",
                                    port = "5432"
                                )    

    cursor = mydb.cursor()

            
    create_query = '''create table if not exists comments(Comment_id varchar(100) primary key,
                                                            video_id varchar(100),
                                                            comments_text text,
                                                            comment_author varchar(100),
                                                            Comment_time timestamp)'''
    cursor.execute(create_query )
    mydb.commit()

    single_comment_details = []
    db = client["youtube_data"]
    ch_data = db["channel_details"]
    for item in ch_data.find({"Channel Information.Channel_name":single_channel_name}, {"_id": 0}):
        single_comment_details.append(item['Comment_information'])

    df_single_comment_details = pd.DataFrame(single_comment_details[0])


    for index, row in df_single_comment_details.iterrows():
            Comment_id= row['Comment_id']
            video_id = row['video_id']
            comments_text = row['comments_text']
            comment_author = row['comment_author']
            Comment_time = row['Comment_time']
            
            insert_query = '''INSERT INTO comments (Comment_id,video_id,comments_text, comment_author, Comment_time)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (Comment_id) DO NOTHING'''
            
            record_to_insert = (Comment_id,video_id,comments_text, comment_author,Comment_time)
            cursor.execute(insert_query, record_to_insert)
            mydb.commit()



#All tables
def tables(single_channel):
    news = get_channels_table(single_channel)
    if news:
        return news
    else:
        get_playlist_table(single_channel)
        get_videos_table(single_channel)
        get_comments_table(single_channel)
    
        return("Tables created Successfully")


#converted to dataframe
def show_channels_table():
    channels =[] 
    db = client["youtube_data"]
    channel_coll= db["channel_details"] 
    for item in channel_coll.find({},{"_id":0,"Channel Information" :1}):
        channels.append(item['Channel Information'])

    df = st.dataframe(channels) 

    return df


def show_playlists_table():
    play_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({}, {"_id": 0, "Playlist_information": 1}):
        for i in range(len(pl_data['Playlist_information'])):
            play_list.append(pl_data['Playlist_information'][i])
    df1= st.dataframe(play_list)

    return df1


def show_videos_table():
    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"Video_information":1}):
        for i in range(len(vi_data["Video_information"])):
            vi_list.append((vi_data["Video_information"][i]))
    df2 = st.dataframe(vi_list)

    return df2


def show_comments_table():
    com_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"Comment_information":1}):
        for i in range(len(com_data['Comment_information'])):
            com_list.append((com_data['Comment_information'][i]))

    df4= st.dataframe(com_list)

    return df4


#STREAMLIT PART

# Displaying an image in the sidebar with a width of 300 pixels and a caption

st.sidebar.image("C:\\Users\\user\\OneDrive\\Desktop\\330685-P9SESK-66.jpg")

st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

channel_id=st.text_input("Enter channel ID")

if st.button("Store Data in MongoDB"):
    ch_ids=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"Channel Information":1}):
        ch_ids.append(ch_data['Channel Information']['Channel_id'])

        if channel_id in ch_ids:
            st.success("Channel Details of the given channel id exists")

        else:
            insert =channel_details(channel_id)
            st.success(insert)

all_channels=[]
db = client["youtube_data"]
channel_coll= db["channel_details"] 
for ch_data in channel_coll.find({},{"_id":0,"Channel Information":1}):
    all_channels.append(ch_data['Channel Information']['Channel_name'])


unique_channel = st.selectbox("Select Channels",all_channels)

if st.button("Migrate to SQL"):
    Table=tables(unique_channel)
    st.success(Table)

show_table = st.selectbox("Select Table to View", ["CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"])

if show_table==":green[CHANNELS]":
    show_channels_table()

elif show_table==":red[PLAYLISTS]":
    show_playlists_table()

elif show_table==":blue[VIDEOS]":
    show_videos_table()

elif show_table==":orange[COMMENTS]":
    show_comments_table()


#SQL connection

mydb = psycopg2.connect(host ="localhost",
                                user ="postgres",
                                password = "guvi2024",
                                database = "youtube",
                                port = "5432"
                            )    

cursor = mydb.cursor()

question=st.selectbox("Select Your Question",("1.All the videos and their channnels name",
                                              "2.Channels have the most number of videos",
                                              "3.Top 10 most viewed videos and their channels",
                                              "4.Comments in each video and their Video names",
                                              "5.Videos with highest likes",
                                              "6.Number of likes in video",
                                              "7.Views of each Channel",
                                              "8.Videos published in the year of 2021",
                                              "9.Average duration of all videos",
                                              "10.Videos with highest number of comments"
                                              ))

if question=="1.All the videos and their channnels name":

    query1 = '''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)


elif question=="2.Channels have the most number of videos":
    query2 = '''select channel_name as channelname,total_videos as no_videos from channels 
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df1=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df1)


elif question=="3.Top 10 most viewed videos and their channels":
    query3= '''select views as views,channel_name as channelname,title as videotitle from videos
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df2=pd.DataFrame(t3,columns=["views","channel_name","videotitle"])
    st.write(df2)


elif question=="4.Comments in each video and their Video names":
    query4= '''select comment as no_comments,title as videotitle from videos 
            where comment is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["No of comments","channel_name"])
    st.write(df4)

elif question=="5.Videos with highest likes":
    query5= '''select title as videotitles,channel_name as channelname,
            likes as like_count from videos 
            where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit(),
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["VideoTitles","Channel name","like_count"])
    st.write(df5)

elif question=="6.Number of likes in video":
    query6= '''select  likes as like_count,title as videotitles
                from videos'''
    cursor.execute(query6)
    mydb.commit(),
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["like_count","VideoTitles"])
    st.write(df6)

elif question=="7.Views of each Channel":
    query7= '''select views as Views,channel_name as Channelname 
                from channels'''
    cursor.execute(query7)
    mydb.commit(),
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["views","channel name"])
    st.write(df7)


elif question=="8.Videos published in the year of 2022":
    query8= '''select title as video_title,published_at as videorelease,
            channel_name as channelname from videos
            where extract(year from published_at)=2022  '''
    cursor.execute(query8)
    mydb.commit(),
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["video_title"," videorelease","channel_name"])
    st.write(df8)

elif question=="9.Average duration of all videos":
    query9= '''select channel_name as channelname,AVG(duration) as averageduration
                from videos group by channel_name '''
    cursor.execute(query9)
    mydb.commit(),
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    t9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        t9.append(dict(channeltitle=channel_title,avgduration= average_duration_str))
    df0=pd.DataFrame(t9)
    st.write(df0)

elif question=="10.Videos with highest number of comments":
    query10= '''select title as titlename,channel_name as channelnamre,comment as comments
                from videos where comment is not null order by comment desc '''
    cursor.execute(query10)
    mydb.commit(),
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["videotitle","channelname","commennts"])
    st.write(df10)
