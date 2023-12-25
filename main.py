
#For the GUI part
import streamlit as st

#for Youtube data api
import googleapiclient.discovery
from googleapiclient.errors import HttpError
#import plotly.express as px


#For using mongodb and mysql for inserting and extracting data
import pandas as pd
import pymongo
from sqlalchemy import create_engine,inspect


#variable for storing all the details about the channel which will be finally stored in MongoDB
ChannelData = {}


# Create the reqd parameters for extracting You Tube Data

api = 'AIzaSyBXoqe7WNgmE3cEDJCiXyBrM_5_j3KxAdY'

api_service_name = "youtube"
api_version = "v3"

# create an API client, will be used by different functions, hence creating as a global parameter

youtube = googleapiclient.discovery.build(api_service_name, api_version,developerKey=api)

#create the engine for connecting to MySQL
user = 'root'
password = '123456789'
host = 'localhost'
port = 3306
database = 'youtubedata'


global engine

engine = create_engine(url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
        user, password, host, port, database), echo=False)

#function to extract the number of hours, minutes and seconds from a field which has data in the form of "PT1H20M20S"
def parse_duration(duration):
    duration_str = ""
    hours = 0
    minutes = 0
    seconds = 0

    # Remove 'PT' prefix from duration
    duration = duration[2:]

    # Check if hours, minutes, and/or seconds are present in the duration string
    if "H" in duration:
        hours_index = duration.index("H")
        hours = int(duration[:hours_index])
        duration = duration[hours_index + 1:]
    if "M" in duration:
        minutes_index = duration.index("M")
        minutes = int(duration[:minutes_index])
        duration = duration[minutes_index + 1:]
    if "S" in duration:
        seconds_index = duration.index("S")
        seconds = int(duration[:seconds_index])

    # Format the duration string
    if hours >= 0:
        duration_str += f"{hours}h "
    if minutes >= 0:
        duration_str += f"{minutes}m "
    if seconds >= 0:
        duration_str += f"{seconds}s"

    return duration_str.strip()
#function to extract the channel details of the channel id specified by the User in the Streamlit app 
#Store the relevant data in ChannelData
def ExtractChannelData(ch_id):

    global ChannelData 
    try:
        # get the channel list based on channel id
        Chanrequest = youtube.channels().list(
                        part="snippet,contentDetails,statistics",
                        id= ch_id
                    )
        Chanresponse = Chanrequest.execute()
        
        if 'items' in Chanresponse:
            channel_data = Chanresponse['items'][0]
            
            snippet = channel_data['snippet']
            
            statistics = channel_data['statistics']
            
            content_details = channel_data.get('contentDetails', {})
            
            related_playlists = content_details.get('relatedPlaylists', {})
            
        
            # Extract relevant data
            ChannelData = {
                'Channel_Details': {
                    'Channel_Name': snippet.get('title', ''),
                    'Channel_Id': ch_id,
                    'Video_Count': int(statistics.get('videoCount', 0)), # added later
                    'Subscription_Count': int(statistics.get('subscriberCount', 0)),
                    'Channel_Views': int(statistics.get('viewCount', 0)),
                    'Channel_Description': snippet.get('description', ''),
                    'Playlist_Id': related_playlists.get('uploads', '')
                }
            }

            return 1
        else:
            st.write(f"Invalid channel id: {ch_id}")
            st.error("Enter the correct 11-digit **channel_id**")
            return None
    except HttpError as e:
        if e.resp.status == 403 and 'exceeded' in str(e):
            st.write("Youtube api request exceeded limit ")
        if e.resp.status == 400 and 'transient' in str(e):
            st.write("Youtube api transient error ")

#Using the playlist id extracted in the Channel Details in function ExtractChannelData, get the video ids corresponding to that channel.
#return the list of video ids
def ExtractPlaylistData(p_id):

    try:
        
        PlayListItemRequest = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=p_id,
            maxResults=10) # getting only 10 videos for now
        PlayListItemResponse = PlayListItemRequest.execute()
        
        video_ids = []
        
        for i in range(len(PlayListItemResponse['items'])):
            video_ids.append(PlayListItemResponse['items'][i]['contentDetails']['videoId'])
        
        # next_page_token = PlayListItemResponse.get('nextPageToken')
        
        
        # while next_page_token is not None:
        #         PlayListItemRequest = youtube.playlistItems().list(
        #             part='contentDetails',
        #             playlistId= p_id, #ChannelData['Channel_Details']['Playlist_Id'],
        #             maxResults=50,
        #             pageToken=next_page_token)
        #         PlayListItemResponse = PlayListItemRequest.execute()
        
        #         for i in range(len(PlayListItemResponse['items'])):
        #             video_ids.append(PlayListItemResponse['items'][i]['contentDetails']['videoId'])
        
        #         next_page_token = PlayListItemResponse.get('nextPageToken')

        return video_ids

    except HttpError as e:
        if e.resp.status == 403 and 'exceeded' in str(e):
            st.write("Youtube api request exceeded limit ")
        if e.resp.status == 400 and 'transient' in str(e):
            st.write("Youtube api transient error ")
    
    

#based on video ids, get video details, and comments of each video.
#return 1 if successful
def ExtractVideoData(video_ids):
    separator = ','
    video_details = []

    try:
        for i in range(0, len(video_ids), 25):
            VideoRequest = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=separator.join(video_ids[i:i+25])
            )
            VideoResponse = VideoRequest.execute()
            video_details.extend(VideoResponse['items']) 
        
        #Storing video data
        for i in range(0,len(video_details)):
                video_id = video_details[i]['id']
                video_data = {
                    'Video_Id': video_id,
                    'Video_Name': video_details[i]['snippet'].get('title', ''),
                    'Video_Description': video_details[i]['snippet'].get('description', ''),
                    'Tags': video_details[i]['snippet'].get('tags', []),
                    'PublishedAt': pd.to_datetime(video_details[i]['snippet'].get('publishedAt', '')),
                    'View_Count': int(video_details[i]['statistics'].get('viewCount', 0)),
                    'Like_Count': int(video_details[i]['statistics'].get('likeCount', 0)),
                    'Dislike_Count': int(video_details[i]['statistics'].get('dislikeCount', 0)),
                    'Favorite_Count': int(video_details[i]['statistics'].get('favoriteCount', 0)),
                    'Comment_Count': int(video_details[i]['statistics'].get('commentCount', 0)),
                    'Duration': parse_duration(video_details[i]['contentDetails'].get('duration', '')),
                    'Thumbnail': video_details[i]['snippet'].get('thumbnails', {}).get('default', {}).get('url', ''),
                    'Caption_Status': video_details[i]['snippet'].get('localized', {}).get('localized', 'Not Available')
                    ,'Comments': ExtractCommentsData(video_id) 
                }
                ChannelData[video_id] = video_data
        return 1
    except HttpError as e:
        if e.resp.status == 403 and 'exceeded' in str(e):
            st.write("Youtube api request exceeded limit ")
        if e.resp.status == 400 and 'transient' in str(e):
            st.write("Youtube api transient error ")
    
#get comments of specific video id
#return comment details, list of dictionary of comment data
def ExtractCommentsData(video_id):
    CommentDetails =[]

    try:
        CommentRequest = youtube.commentThreads().list(
            part="snippet",#",replies", - replies not required, cudn't see any use in the project, but its there in the api code
            videoId=video_id,
            maxResults=25
        )
        CommentResponse = CommentRequest.execute()
        
        for i in range(len(CommentResponse['items'])):
            CommentData={
                    'Video_Id': video_id,
                    'Comment_Id': CommentResponse['items'][i]['snippet']['topLevelComment']['id'],
                    'Comment_Text': CommentResponse['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal'],
                    'Comment_Author': CommentResponse['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_PublishedAt': CommentResponse['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']
                    }
            CommentDetails.append(CommentData)
        
        # next_page_token = CommentResponse.get('nextPageToken')
        # #print(next_page_token)
        # while next_page_token is not None:
        #     CommentRequest = youtube.commentThreads().list(
        #         part="snippet",#",replies", - replies not required, cudn't see any use in the project, but its there in the api code
        #         videoId=video_id,
        #         maxResults=25
        #     )
        #     CommentResponse = CommentRequest.execute()
        #     next_page_token = CommentResponse.get('nextPageToken')
            
        #     for i in range(len(CommentResponse['items'])):
        #         CommentData={
        #                     'Video_Id': video_id,
        #                     'Comment_Id': CommentResponse['items'][i]['snippet']['topLevelComment']['id'],
        #                     'Comment_Text': CommentResponse['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal'],
        #                     'Comment_Author': CommentResponse['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
        #                     'Comment_PublishedAt': CommentResponse['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']
        #                     }
        #         CommentDetails.append(CommentData)
    
    except HttpError as e:
        if e.resp.status == 403 and 'disabled comments' in str(e):
            CommentData = {
                'Video_Id': video_id,
                'Comment_Id': 'comments_disabled',
                'Comment_Text': 'comments_disabled',
                'Comment_Author': 'comments_disabled',
                'Comment_PublishedAt': 'comments_disabled'
            }
            CommentDetails.append(CommentData)
            print("Comments are disabled for video: "+ video_id)
        if e.resp.status == 403 and 'exceeded' in str(e):
            CommentData = {
                'Video_Id': video_id,
                'Comment_Id': 'Youtube api request exceeded limit',
                'Comment_Text': 'Youtube api request exceeded limit',
                'Comment_Author': 'Youtube api request exceeded limit',
                'Comment_PublishedAt': 'Youtube api request exceeded limit'
            }
            CommentDetails.append(CommentData)
            print("Youtube api request exceeded limit for video: "+ video_id)
        if e.resp.status == 400 and 'transient' in str(e):
            CommentData = {
                'Video_Id': video_id,
                'Comment_Id': 'Youtube api transient error',
                'Comment_Text': 'Youtube api transient error',
                'Comment_Author': 'Youtube api transient error',
                'Comment_PublishedAt': 'Youtube api transient error'
            }
            CommentDetails.append(CommentData)
            print("Youtube api transient error for video: "+ video_id)
        if e.resp.status == 404 and 'parameter' in str(e):
            CommentData = {
                'Video_Id': video_id,
                'Comment_Id': 'Video Not Found',
                'Comment_Text': 'Video Not Found',
                'Comment_Author': 'Video Not Found',
                'Comment_PublishedAt': 'Video Not Found'
            }
            CommentDetails.append(CommentData)
            print("Video Not Found for video: "+ video_id)
            
        
    return CommentDetails
######################################################################################################################################################
#call function insertChannelDetail to insert the data of a specific channel to MongoDB
#print the success message on the streamlit app.
def UploadDataMongo():    
    retX = insertChannelDetail()
    if retX:
        return 1
        #st.session_state['Upload_Data_Mongo'] = False

        
#create the required parameters for database and collection to store youtube data in mongodb: Using pymongo
def createCollection():
    #Creating the connection,DB and collection for MongoDB
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb= myclient["YouTubeData"]
    global mycol
    mycol= mydb["ChannelDetails"]
    return
#actual insertion of channel data into MongoDB in the collection created
#return the _id of the document inserted.
def insertChannelDetail():
    #check if channel details already exist in MongoDB, if so print msg and skip entering details, else enter details.
    Names = mycol.find({},{'Channel_Details.Channel_Name':1,'_id':0})
    for x in Names:
        if (x['Channel_Details']['Channel_Name'] == ChannelData['Channel_Details']['Channel_Name']):
            st.write("Channel details already exist in MongoDB")
            return 0
    
    retVal= mycol.insert_one(ChannelData) # will insert as another document if records already exist in the collection
    return retVal

######################################################################################################################################################
#insertion of data read from MongoDB into MySQL as 4 different tables - channel,playlist,videos,comment
#return 0, if details for that channel name already exist, return 1 if inserted successfully
def UploadDataSQL(ch_name):
    
    
    # Create an Inspector and bind it to the engine
    inspector = inspect(engine)
    table_name= 'channel'
    # Get a list of all table names in the database
    table_names = inspector.get_table_names()
    # Check if the table exists
    if table_name in table_names:
        #print(f'Table {table_name} exists.')
        query = "SELECT EXISTS (SELECT * FROM channel WHERE Channel_Name = '"+ ch_name + "') AS ch_exist"
        x = pd.read_sql(query, engine)
        if (x['ch_exist'][0]):
            st.write("Channel Details of channel:", ch_name, " already exist in MySQL. Please choose a different channel to Upload!")
            return 0
    
    

    
    
    ChannelInfo = mycol.find_one({'Channel_Details.Channel_Name':ch_name}) #based on the channel name selected in the streamlit app, get the details from MongoDB

    if ChannelInfo is not None:
        # Create required tables in MySQL
        #Table will be created when Dataframe is stored into the DB
        
        #channel table
        channel_details_fromMongo = {
                    "Channel_Name": ChannelInfo['Channel_Details']['Channel_Name'],
                    "Channel_Id": ChannelInfo['Channel_Details']['Channel_Id'],
                    "Video_Count": ChannelInfo['Channel_Details']['Video_Count'], 
                    "Subscription_Count": ChannelInfo['Channel_Details']['Subscription_Count'],
                    "Channel_Views": ChannelInfo['Channel_Details']['Channel_Views'],
                    "Channel_Description": ChannelInfo['Channel_Details']['Channel_Description'],
                    "Playlist_Id": ChannelInfo['Channel_Details']['Playlist_Id']
                    }
        Channel_dataframe = pd.DataFrame.from_dict(channel_details_fromMongo, orient='index').T
        
        Channel_dataframe.to_sql(name='channel', con=engine,if_exists='append')
        
        #playlist table
        playlist_details_fromMongo = {"Channel_Id": ChannelInfo['Channel_Details']['Channel_Id'],
                                "Playlist_Id": ChannelInfo['Channel_Details']['Playlist_Id']
                                }
        playlist_dataframe = pd.DataFrame.from_dict(playlist_details_fromMongo, orient='index').T
        
        playlist_dataframe.to_sql(name='playlist', con=engine,if_exists='append')

        #videos table
        video_details_fromMongo_list = []
        comment_details_fromMongo_list= []
        for video_id, video_data in ChannelInfo.items():
            print(video_id)
            if video_id != 'Channel_Details' and video_id != '_id':
                #print("inside if"+ video_id)
                video_details_fromMongo = {
                'Playlist_Id':ChannelInfo['Channel_Details']['Playlist_Id'],
                'Video_Id': video_id,
                'Video_Name': video_data['Video_Name'],
                'Video_Description': video_data['Video_Description'],
                'Published_date': video_data['PublishedAt'],
                'View_Count': video_data['View_Count'],
                'Like_Count': video_data['Like_Count'],
                'Dislike_Count': video_data['Dislike_Count'],
                'Favorite_Count': video_data['Favorite_Count'],
                'Comment_Count': video_data['Comment_Count'],
                'Duration': video_data['Duration'],
                'Thumbnail': video_data['Thumbnail'],
                'Caption_Status': video_data['Caption_Status']
                }
                video_details_fromMongo_list.append(video_details_fromMongo)
                # Traverse the comment array per video id .
                for i in range(0,len(video_data['Comments'])):
                    comment_details_fromMongo= { 
                        'Comment_Id': video_data['Comments'][i]['Comment_Id'],
                        'Video_Id': video_id,                
                        'Comment_Text': video_data['Comments'][i]['Comment_Text'],
                        'Comment_Author': video_data['Comments'][i]['Comment_Author'],
                        'Comment_PublishedAt': video_data['Comments'][i]['Comment_PublishedAt']
                        }
                    comment_details_fromMongo_list.append(comment_details_fromMongo)
            
        
        video_dataframe = pd.DataFrame(video_details_fromMongo_list)
        video_dataframe.to_sql(name='videos', con=engine,if_exists='append')
        #comments table
        comment_dataframe = pd.DataFrame(comment_details_fromMongo_list)
        comment_dataframe.to_sql(name='comments', con=engine,if_exists='append')
        return 1



# function to call relevant functions in sequence for youtube data extraction
#return 0 if unsuccessful at any step: channel,playlist,video / return 1 if successful
def ExtractYouTubeData(ch_id):

    retChannelData = ExtractChannelData(ch_id)
    if retChannelData:
        retvideo_ids = ExtractPlaylistData(ChannelData['Channel_Details']['Playlist_Id'])
        if retvideo_ids:
            success = ExtractVideoData(retvideo_ids)
            if success:
                return 1
                #st.session_state['Extract_Data'] = False             
            else:
                return 0
        else:
            return 0
    else:
        return 0
##################################################################################################################################################   

  
#Create the first screen of the streamlit application, to display few radio buttons on the sidebar and for user inputs on the right.
header = st.container()

sidebar1 = st.sidebar

CHIDbyuser = ""

with header:
    st.title("Welcome to YouTube Data Harvesting Project")

# will call the main you tube data extract function which in turn will call all the required other functions for extraction.
# print msg on screen after successful extraction
def click_button_ExtData(ChannelID):
    
    #st.write("Code is analyzing your text.")
    retVal = ExtractYouTubeData(ChannelID)
    if retVal == 0:
        return 0
    else:
        st.write("YouTube data extracted successfully for channel id:", ChannelID)
    #st.text("Channel id entered is:" + CHIDbyuser )

#will call the main mongo db transfer function
#print msg on screen after successful completion.
def click_button_UploadData():
    #st.write("Code is analyzing your text.")
    retVal = UploadDataMongo() 
    if retVal:
        st.write("Inserted Channel Details successfully into MongoDB!")
        
#will call the main sql data storing function
#print msg on screen after successful completion.
def click_button_MoveData(ch_name):      
    ret = UploadDataSQL(ch_name)
    if ret != 0:
        st.write("Data uploaded to MySQL successfully!")
        st.session_state['Upload_Data_MySQL'] = False
        
#returns the list of channel names available in MongoDB
#will be used to populate the dropdown for channel selection to transfer data to MySQL.
def Channel_Names_Select():
    Names = mycol.find({},{'Channel_Details.Channel_Name':1,'_id':0})
    temp_list=[]
    for x in Names:
        temp_list.append(x['Channel_Details']['Channel_Name'])
    return temp_list


########################################################################################################################################
#View screen where the multiple queries are visible to be selected and executed. 
#This function will actually execute the different queries and return a dataframe.
def Query_Result(option):
    if (option == 0):
        query = ("""SELECT v.video_name, c.channel_name
            FROM videos v
            JOIN playlist p
            ON v.playlist_id = p.playlist_id
            JOIN channel c
            ON p.channel_id = c.channel_id """)
        df = pd.read_sql(query, engine)
        # df = pd.DataFrame(resultSet,columns=["Video Name","Channel"])
        # fig = px.bar(df, x='Video Name', y='Chan')
        # st.plotly_chart(fig)
        return df
    elif (option == 1):
        query = ("""SELECT channel_name,video_count FROM channel ORDER BY video_count DESC LIMIT 5""")
        df = pd.read_sql(query, engine)
        #df = pd.DataFrame(resultSet,columns=["Channel","Number of Videos"])
        return df
    elif (option == 2):
        query = ("""SELECT v.video_name,c.channel_name,v.view_count
            FROM videos v
            JOIN playlist p
            ON v.playlist_id = p.Playlist_Id
            JOIN channel c
            ON p.channel_id = c.channel_id
            ORDER BY  v.view_count DESC
            LIMIT 10 """)
        df = pd.read_sql(query, engine)
        return df
    elif (option == 3):
        query = ("""SELECT v.video_name,v.comment_count
            FROM videos v""")
        df = pd.read_sql(query, engine)
        return df
    elif (option == 4):
        query = ("""SELECT
                v.video_name,
                c.channel_name,
                v.like_count
            FROM
                videos v
            JOIN
                playlist p ON v.playlist_id = p.playlist_id
            JOIN
                channel c ON p.channel_id = c.channel_id
            JOIN (
                SELECT
                    playlist_id,
                    MAX(like_count) AS max_like_count
                FROM
                    videos
                GROUP BY
                    playlist_id
            ) max_likes ON v.playlist_id = max_likes.playlist_id AND v.like_count = max_likes.max_like_count""")
        df = pd.read_sql(query, engine)
        return df
    elif (option == 5):
        query = ("""SELECT video_name,like_count,dislike_count
            FROM videos
            ORDER BY like_count DESC """)
        df = pd.read_sql(query, engine)
        return df
    elif (option == 6):
        query = ("""SELECT channel_name,channel_views 
            FROM channel
            ORDER BY channel_views DESC""")
        df = pd.read_sql(query, engine)
        return df
    elif (option == 7):
        query = ("""SELECT v.video_name,c.channel_name,v.published_date
            FROM videos v
            JOIN playlist p
            ON v.playlist_id = p.playlist_id
            JOIN channel c
            ON p.channel_id = c.channel_id
            WHERE v.published_date>'2022-01-01'
            AND v.published_date<'2023-01-01'""")
        df = pd.read_sql(query, engine)
        return df
    elif (option == 8):
        query = ("""WITH TimeDurationCTE AS (
                SELECT v.playlist_id,p.channel_id,c.channel_name,
                    TIME_TO_SEC(
                        ADDTIME(
                            ADDTIME(
                                MAKETIME(SUBSTRING_INDEX(v.duration, 'h', 1), 0, 0),
                                MAKETIME(0, SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, ' ', -2), 'm', 1), 0)
                                ),
                                MAKETIME(0, 0, SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, ' ', -1), 's', 1))
                                )
                        )AS duration_seconds
                FROM videos v
                JOIN playlist p ON v.playlist_id = p.playlist_id
                JOIN channel c ON p.channel_id = c.channel_id
                    )
                SELECT channel_name,AVG(duration_seconds) AS avg_duration_seconds
                FROM TimeDurationCTE
                GROUP BY playlist_id, channel_id,channel_name""")
        df = pd.read_sql(query, engine)
        return df
    elif (option == 9):
        query = ("""SELECT   v.video_name,c.channel_name,v.comment_count
            FROM videos v
            JOIN playlist p ON v.playlist_id = p.playlist_id
            JOIN channel c ON p.channel_id = c.channel_id
            JOIN (
                    SELECT
                        playlist_id,
                        MAX(comment_count) AS max_comment_count
                    FROM
                        videos
                    GROUP BY
                        playlist_id
                ) max_comm ON v.playlist_id = max_comm.playlist_id AND v.comment_count = max_comm.max_comment_count""")
        df = pd.read_sql(query, engine)
        return df
 

global query_list
query_list= [
                "1. What are the names of all the videos and their corresponding channels?",
                "2. Which channels have the most number of videos, and how many videos do they have?",
                "3. What are the top 10 most viewed videos and their respective channels?",
                "4. How many comments were made on each video, and what are their corresponding video names?",
                "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                "8. What are the names of all the channels that have published videos in the year 2022?",
                "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
            ]


# set the session variables for the toggle buttons
if 'Extract_Data' not in st.session_state:
    st.session_state['Extract_Data'] = False
if 'Upload_Data_Mongo' not in st.session_state:
    st.session_state['Upload_Data_Mongo'] = False
if 'Upload_Data_MySQL' not in st.session_state:
    st.session_state['Upload_Data_MySQL'] = False

# create the sidebar with different options to extract and view. Then based on the selection here, display corresponding fields.
with sidebar1:
    selection = sidebar1.radio("What's your choice of task?",[":house: Home",":open_file_folder:(Extract,Transform & Load)",":bar_chart:(Query & View)"])
    # ExtractButton = sidebar1.radio(,)
    # ViewButton = sidebar1.radio(,)

if selection == ":house: Home": 
    st.subheader("To create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.")
    st.text("The application has the following features:")
    st.markdown("- Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.")
    st.markdown("- Option to store the data in a MongoDB database as a data lake.")
    st.markdown("- Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.")
    st.markdown("- Option to select a channel name and migrate its data from the data lake to a SQL database as tables.")
    st.markdown("- Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.")
    

if selection == ":open_file_folder:(Extract,Transform & Load)":
    CHIDbyuser = st.text_input("Enter a valid YouTube Channel ID","")
    createCollection() # mongodb collection needs to be ready

    #if CHIDbyuser:
    extDataButton = st.toggle("Extract Data of this YouTube ID",value=st.session_state['Extract_Data'])
    if extDataButton:
            retValue = click_button_ExtData(CHIDbyuser) # extract youtube data.
            if retValue == 0:
                st.write("Enter a valid YouTube Channel ID to proceed further.")
            else:
                 UploadDataButton = st.toggle("Upload Data to MongoDB",value=st.session_state['Upload_Data_Mongo'])
                 if UploadDataButton:
                     #st.write("transferring to mongo")
                     click_button_UploadData() # upload data to mongodb
                
    
    choice_channel = st.selectbox('Select the channel to upload to MySQL', options = Channel_Names_Select())

    #displaying the selected option
    st.write('You have selected:', choice_channel)        

    MoveToSQLButton = st.toggle("Transfer Data from MongoDB to MySQL",value=st.session_state['Upload_Data_MySQL'])
    if MoveToSQLButton:
        click_button_MoveData(choice_channel) #upload data to mysql

if selection == ":bar_chart:(Query & View)":
    choice_query = st.selectbox('Select the query to view the result', options = query_list)
    st.write('You have selected:', choice_query)        
    ind = query_list.index(choice_query)
    #st.write("Selected Index : ", ind)
    ViewResult = st.button("View Query Result")
    if ViewResult:
        dataf = Query_Result(ind) # execute the specific query based on the selected index.
        st.dataframe(dataf) #display the resultset
