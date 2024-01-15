import pymongo
import streamlit as st
import googleapiclient.discovery
import mysql.connector
import pandas as pd
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["charu"]

dbs = mysql.connector.connect(
      host="localhost",
      user="root@localhost",
      password="Charu@9601",
      database='youtube',
      port='3306'
    )
cursor=dbs.cursor()



api_service_name = "youtube"
api_version = "v3"
developerKey = "AIzaSyCzcgyNl6VjtgO_yt2U7XMAFwRlBgSOnFI"
youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey="AIzaSyCzcgyNl6VjtgO_yt2U7XMAFwRlBgSOnFI")

def channel_details(charu):   
        request = youtube.channels().list(
                part="snippet,contentDetails,statistics,status",
                id=charu
            ).execute()
        
        for i in request["items"]:
            data=dict(
                    Channel_id=i['id'],
                    Channel_name=i['snippet']['title'],
                    subscribers=i['statistics']['subscriberCount'],
                    Channel_views=i['statistics']['viewCount'],
                    Channel_description=i['snippet']['description'],
                    video_count=i['statistics']['videoCount'],
                    Channel_status=i['status']['privacyStatus']
                    
                    )
            return data

video_list=[]
def videos_id(channel_id):
    try:     
        request = youtube.channels().list(
                        part="contentDetails",
                        id=channel_id
                    ).execute()
        playlist_id=request['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        next_page_token=None
        while True:
            request = youtube.playlistItems().list(
                        part="snippet,contentDetails",
                        maxResults=50,
                        playlistId=playlist_id,
                        pageToken=next_page_token
                    ).execute()
            for i in range(len(request['items'])):
                video=request['items'][i]['contentDetails']['videoId']
                video_list.append(video)
            next_page_token=request.get('nextPageToken')
        
            if next_page_token is None:
                break
    except:
        pass
        
    return video_list      
            
v_d=[] 
def get_video_details(v):

    try: 
        for Id in v:
                request = youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id=Id
                    )
                response=request.execute()
                for i in response['items']:
                    video_details=dict(
                                    video_id=i['id'],
                                    video_name=i['snippet']['title'],
                                    channel_name=i["snippet"]['channelId'],
                                    channel_idname=i["snippet"]['title'],
                                    video_description=i['snippet']['description'],
                                    published_date=i['snippet']['publishedAt'],
                                    view_count=i['statistics']['viewCount'],
                                    like_count=i['statistics']['likeCount'],
                                    dislike_count=i['statistics'].get('dislikeCount'),
                                    fav_count=i['statistics']['favoriteCount'],
                                    comment_count=i['statistics']['commentCount'],
                                    duration=i['contentDetails']['duration'],
                                    #Thumbnail=i["snippet"]["thumbnails"]["default"].get(["url"]),
                                    caption_status=i['contentDetails']['caption']
                                    )
                    v_d.append(video_details)
    except:
        pass          
    return v_d            

def Comment_details(video_id):

        comment_list=[]  
        try:      
                for i in video_id:
                                request = youtube.commentThreads().list(
                                part="snippet,replies",
                                videoId=i )
                                response=request.execute()
                                for item in response['items'] :   
                                                
                                        comment_data=dict(
                                                                Comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                                                Channel_id= item['snippet']['topLevelComment']['snippet']['channelId'],
                                                                Comment_id=item['snippet']['topLevelComment']['id'],
                                                                video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                                                                publish_date=item['snippet']['topLevelComment']['snippet']['publishedAt'],
                                                                )
                                                
                                        comment_list.append(comment_data)
                                
        except:
                pass               
                        
        return comment_list
              
                
def tranformation(C):
    channel_d=channel_details(C)
    video_l=videos_id(C)
    video_d=get_video_details(video_list)
    commet_d=Comment_details(video_list)

    mycol=mydb['Youtube_data']
    mycol.insert_one({"channel_de":channel_d,"video_li":video_l,'video_de':video_d,"commet_de":commet_d})

    return "done"



videode_list=[]
db=myclient['charu']
coll1=db['Youtube_data']
for videe_l in coll1.find({},{'_id':0,'video_de':1}):
    for j in range(len(videe_l['video_de'])):
        videode_list.append(videe_l['video_de'][j])
        
df1=pd.DataFrame(videode_list)  

comment_list=[]
db=myclient['charu']
coll2=db['Youtube_data']
for comment_l in coll2.find({},{'_id':0,'commet_de':1}):
    for j in range(len(comment_l['commet_de'])):
        comment_list.append(comment_l['commet_de'][j])
df2=pd.DataFrame(comment_list)  

def channel_table():
    drop='''drop table if exists channels'''
    cursor.execute(drop)
    dbs.commit()

    try:
        a='''create table if not exists channels(Channel_name varchar(100),
                                                        Channel_id  varchar(100) primary key,
                                                        subscribers bigint,
                                                        Channel_views bigint,
                                                        Channel_description text,
                                                        video_count  bigint,
                                                        Channel_status varchar(50))'''
        cursor.execute(a)
        dbs.commit()
    except:
        print("done")    



    channel_list=[]
    db=myclient["charu"]
    collection=db['Youtube_data']
    for i in collection.find({},{"_id":0,"channel_de":1}):
        channel_list.append(i['channel_de'])
    df=pd.DataFrame(channel_list)  


    for index,row in df.iterrows():
        insert_table='''insert into channels(Channel_id,
                                            Channel_name,
                                            subscribers,
                                            Channel_views,
                                            Channel_description,
                                            video_count,
                                            Channel_status)

                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_id'],
                row['Channel_name'],
                row['subscribers'],
                row['Channel_views'],
                row['Channel_description'],
                row['video_count'],
                row['Channel_status'])
        try:
                cursor.execute(insert_table,values)
                dbs.commit()
        except:
                print("done")
        
            


def tvideo():
  drop='''drop table if exists videos'''
  cursor.execute(drop)
  dbs.commit()
  b='''create table if not exists videos(video_id varchar(255) primary key,
                                            video_name varchar(255),
                                            channel_name varchar(250),
                                            video_description  text,
                                            published_date timestamp,
                                            view_count bigint,
                                            like_count bigint,
                                            dislike_count bigint,
                                            fav_count bigint,
                                            comment_count bigint,
                                            duration varchar(50),
                                            caption_status varchar(50)
                                             )'''
                                            
  cursor.execute(b)
  dbs.commit()
  from datetime import datetime
  for index,row in df1.iterrows():
      insert_video_table='''insert into videos(video_id, 
                                            video_name,
                                            channel_name, 
                                            video_description,
                                            published_date,
                                            view_count, 
                                            like_count,
                                            dislike_count,
                                            fav_count, 
                                            comment_count,
                                            duration,
                                            caption_status
                                            )
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
      published_date_str = row['published_date']
      parsed_published_date = datetime.strptime(published_date_str, '%Y-%m-%dT%H:%M:%SZ')
      formatted_published_date = parsed_published_date.strftime('%Y-%m-%d %H:%M:%S')

      values=(row['video_id'],
              row['video_name'],
              row['channel_name'],
              row['video_description'],
              formatted_published_date,
              row['view_count'],
              row['like_count'],
              row['dislike_count'],
              row['fav_count'],
              row['comment_count'],
              row['duration'],
              row['caption_status']
              )

      cursor.execute(insert_video_table,values)
      dbs.commit()

def comments_table():
    drop='''drop table if exists comments'''
    cursor.execute(drop)
    dbs.commit()

    c='''create table if not exists comments(Comment_text text,
                                          Channel_id varchar(50),
                                          Comment_id varchar (100),
                                          video_id varchar(50),
                                          publish_date timestamp,
                                          primary key( Channel_id,Comment_id) )'''

    cursor.execute(c)
    dbs.commit()

    try:
        from datetime import datetime
        for index,row in df2.iterrows():
            insert_comment_table='''insert into comments(Comment_text,
                                            Channel_id,
                                            Comment_id,
                                            video_id,
                                            publish_date)
                                            
                                            value(%s,%s,%s,%s,%s)'''
            
            publish_date_str = row['publish_date']
            parsed_publish_date = datetime.strptime(publish_date_str, '%Y-%m-%dT%H:%M:%SZ')
            formatted_publish_date = parsed_publish_date.strftime('%Y-%m-%d %H:%M:%S')
            
            values=(row['Comment_text'],
                row['Channel_id'],
                row['Comment_id'],
                row['video_id'],
                formatted_publish_date
                )   

            cursor.execute(insert_comment_table,values)
            dbs.commit()
    except():
        print('d')    

def stable():
    channel_table()
    tvideo()
    comments_table()

    return "Done"

youtube_logo_url = "https://upload.wikimedia.org/wikipedia/commons/e/ef/Youtube_logo.png "
st.image(youtube_logo_url,width=50)
st.markdown('## **:red[YouTube Data Harvesting and Warehousing using SQL,MongoDB and Streamlit]**')
Channel_id=st.text_input("Enter the channel ID")


def channel_sttable():
    channel_list=[]
    mydb=myclient["charu"]
    collection=mydb['Youtube_data']
    for i in collection.find({},{"_id":0,"channel_de":1}):
        channel_list.append(i['channel_de'])
    df=st.dataframe(channel_list) 

    return df  


def videos_sttable():
    videode_list=[]
    db=myclient['charu']
    coll1=db['Youtube_data']
    for videe_l in coll1.find({},{'_id':0,'video_de':1}):
        for j in range(len(videe_l['video_de'])):
            videode_list.append(videe_l['video_de'][j])
            
    df1=st.dataframe(videode_list)    

    return df1  

def comment_sttable():
    comment_list=[]
    db=myclient['charu']
    coll2=db['Youtube_data']
    for comment_l in coll2.find({},{'_id':0,'commet_de':1}):
        for j in range(len(comment_l['commet_de'])):
            comment_list.append(comment_l['commet_de'][j])
    df2=st.dataframe(comment_list)

    return df2

with st.sidebar:
    st.markdown("# :red[Tables]")
    
    Channeltable= st.button("**:green[Channel Details]**")
    videotable=st.button("**:green[Video Details]**")
    commenttable=st.button("**:green[Comment Details]**")
if Channeltable:
    channel_sttable()
if videotable:
    videos_sttable()
if commenttable:
    comment_sttable()
          

if st.button("Collect and store data"):
    ch_list=[]
    mydb=myclient["charu"]
    coll1=mydb["Youtube_data"]
    for ch_data in coll1.find({},{"_id":0,"channel_de":1}):
      ch_list.append(ch_data['channel_de']['Channel_id'])
    if  Channel_id in ch_list:
       st.success("Already Exists") 
    else:
       insert=channel_details(Channel_id)   
       st.success(insert)

if st.button("Migrate to SQL"):
     table=stable()
     st.success(table) 

Questions=st.selectbox("Select your questions",("1.Videos name and corresponding channels",
                                                "2.Highest number of vidoes and their corresponding channels",
                                                "3.Top 10 most viewed videos and their respective channels",
                                                "4.Number of comments in each videos and their respective videos name",
                                                "5.Highest number of like video and their channels name",
                                                "6.likes and dislike of each videos and their corresponding videos name",
                                                "7.Total number of views for each channels and channel name",
                                                "8.Channel name which are published videos in the year 2022",
                                                "9.Average duration of all videos in each channel and corresponding videos name",
                                                "10.Highest number of comments in all videos and their corresponding channel name"))

if Questions=="1.Videos name and corresponding channels":
    Query1='''
    SELECT videos.video_name AS Videos_Name, channels.Channel_name AS Channel_Name
    FROM videos
    JOIN channels ON videos.channel_name = channels.Channel_id
    '''
    cursor.execute(Query1)
    table=cursor.fetchall()
    dbs.commit()
    df=pd.DataFrame(table,columns=["Videos_Name","Channel_Name"])
    st.write(df)

elif Questions=="2.Highest number of vidoes and their corresponding channels":   
    Query2='''select Channel_name as Channel_Name,video_count as Video_Count from channels'''
    cursor.execute(Query2)
    table=cursor.fetchall()
    dbs.commit()
    df1=pd.DataFrame(table,columns=["Channel_Name","Video_Count"])
    b=df1.sort_values(by='Video_Count', ascending=False)
    st.write(b)

elif Questions== "3.Top 10 most viewed videos and their respective channels":
    Query3='''select videos.video_name as Videos_Name,channels.Channel_name As channelname,videos.view_count as View_count
           from videos
           join channels on videos.channel_name = channels.Channel_id'''
    cursor.execute(Query3)
    table=cursor.fetchall()
    dbs.commit()
    df2=pd.DataFrame(table,columns=["Videos_Name","Channels_Name","View_Count"])
    a=df2.nlargest(10,"View_Count") 
    st.write(a)

elif Questions=="4.Number of comments in each videos and their respective videos name":
    Query4='''select video_name as Videos_Name,comment_count as View_Count from videos'''
    cursor.execute(Query4)
    table=cursor.fetchall()
    dbs.commit()
    df3=pd.DataFrame(table,columns=["Videos_Name","comment_count"])
    st.write(df3)  

elif Questions=="5.Highest number of like video and their channels name":
    Query5='''
    SELECT
        videos.video_name AS `Videos Name`,
        videos.like_count AS Like_Count,
        channels.Channel_name AS Channel_Name
    FROM
        videos
    JOIN
        channels ON videos.channel_name = channels.Channel_id
    WHERE
        videos.like_count IS NOT NULL
    ORDER BY
        videos.like_count DESC
'''
    cursor.execute(Query5)
    table=cursor.fetchall()
    dbs.commit()
    df4=pd.DataFrame(table,columns=["Videos_Name","Like_Count","Channel_Name"])
    st.write(df4)

elif Questions=="6.likes and dislike of each videos and their corresponding videos name":
    Query6='''select video_name as Videos_Name,like_count as likecount,dislike_count as dikslikecount from videos'''
    cursor.execute(Query6)
    table=cursor.fetchall()
    dbs.commit()
    df5=pd.DataFrame(table,columns=["Video_Name","likeco","Dislikecount"])
    st.write(df5)

elif Questions=="7.Total number of views for each channels and channel name":
    Query7='''select Channel_name as channel_Name,Channel_views as channel_view from channels'''
    cursor.execute(Query7)
    table=cursor.fetchall()
    dbs.commit()
    df6=pd.DataFrame(table,columns=["Channel_Name","Channel_views"])
    st.write(df6)  

elif Questions=="8.Channel name which are published videos in the year 2022":
    Query8 = '''SELECT channel_name AS Channel_Name, published_date AS Published_date FROM videos WHERE EXTRACT(YEAR FROM published_date) = 2022'''
    cursor.execute(Query8)
    table = cursor.fetchall()
    dbs.commit()
    df7 = pd.DataFrame(table, columns=["Channel_Name", "Published_date"])
    st.write(df7)

elif Questions=="9.Average duration of all videos in each channel and corresponding videos name":
    Query9 = '''SELECT
        channels.Channel_name AS `Channel_Name`,
        AVG(
            TIME_TO_SEC(
                SUBSTRING_INDEX(SUBSTRING_INDEX(videos.duration, 'T', -1), 'S', 1)
            )
        ) AS `Average_Duration`
    FROM
        channels
    JOIN
        videos ON channels.Channel_id = videos.channel_name
    GROUP BY
        channels.Channel_name'''

    cursor.execute(Query9)
    table = cursor.fetchall()
    dbs.commit()
    df8 = pd.DataFrame(table, columns=["Channel_Name", "Average_Duration"])
    st.write(df8) 

elif Questions=="10.Highest number of comments in all videos and their corresponding channel name":
    Query10='''select channels.Channel_name as Channel_Name,videos.comment_count as Comment_count
    from channels
    join videos  on channels.Channel_id=videos.channel_name'''
    cursor.execute(Query10)
    table=cursor.fetchall()
    dbs.commit()
    df9=pd.DataFrame(table,columns=["Channel_Name","Comment_Count"])
    c=df9.sort_values(by='Comment_Count', ascending=False)
    st.write(c)

