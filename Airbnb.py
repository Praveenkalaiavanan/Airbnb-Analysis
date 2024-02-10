#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymongo
import pandas as pd


# In[2]:


import os


# In[3]:


from pymongo import MongoClient


# In[4]:


connection=pymongo.MongoClient('mongodb+srv://praveenkalaivanan:Praveen8311@cluster0.g5vhm2e.mongodb.net/?retryWrites=true&w=majority')


# In[5]:


db=connection["sample_airbnb"]


# In[6]:


coll=db["listingsAndReviews"]


# In[7]:


#Extract data 
import pymongo
connection=pymongo.MongoClient("mongodb+srv://praveenkalaivanan:Praveen8311@cluster0.g5vhm2e.mongodb.net/?retryWrites=true&w=majority")
db=connection["sample_airbnb"]
coll=db["listingsAndReviews"]
box = []
for i in coll.find():
    data = dict(Id = i['_id'],
                Name = i.get('name'),
                Description = i['description'],
                Property_type = i['property_type'],
                Room_type = i['room_type'],
                Bed_type = i['bed_type'],
                Min_nights = int(i['minimum_nights']),
                Max_nights = int(i['maximum_nights']),
                Cancellation_policy = i['cancellation_policy'],
                Accomodates = i['accommodates'],
                Total_bedrooms = i.get('bedrooms'),
                Total_beds =  i.get('beds'),
                Availability_365 = i['availability']['availability_365'],
                Price = i['price'],
                calculated_host_listing_count = i['host']['host_listings_count'],
                neighbourhood = i['host']['host_neighbourhood'],
                No_of_reviews = i['number_of_reviews'],
                Review_scores = i['review_scores'].get('review_scores_rating'),
                Amenities = ', '.join(i['amenities']),
                Host_id = i['host']['host_id'],
                Host_name = i['host']['host_name'],
                Street = i['address']['street'],
                Country = i['address']['country'],
                Country_code = i['address']['country_code'],
                Location_type = i['address']['location']['type'],
                Longitude = i['address']['location']['coordinates'][0],
                Latitude = i['address']['location']['coordinates'][1])
    box.append(data)


# In[8]:


box


# In[9]:


#Extract comment 
comment = []
for i in coll.find():
    for j in range (len(i['reviews'])):
        d=[]
        try:
            comments=i['reviews'][j]['comments']
            reviewer_name=i['reviews'][j]['reviewer_name']
        except:
            comments=None
            reviewer_name=None
        d.append( i['reviews'][j]['date'].strftime('%d/%m/%Y . %H:%M'))
        dt = d[0].split(' . ')
        date = dt[0]
        time = dt[1]
        data1 = dict(Id = i['reviews'][j]['listing_id'],
                    reviewer_id= i['reviews'][j]['reviewer_id'],
                    reviewer_name=reviewer_name,
                    comments=comments,
                    Date=date,
                    Time=time)
        comment.append(data1)


# In[10]:


comment


# In[34]:


#converting data frame
import pandas as pd
df1 = pd.DataFrame(box)
file_path = 'C:/Users/TAMIL TS/project/.venv/Airbnb/box.csv'
df1.to_csv(file_path, index=False)
df2 = pd.DataFrame(comment)
file_path = 'C:/Users/TAMIL TS/project/.venv/Airbnb/comment.csv'
df2.to_csv(file_path, index=False)


# In[35]:


#EDA process
DF=pd.read_csv("C:/Users/TAMIL TS/project/.venv/Airbnb/box.csv")
df2=pd.read_csv("C:/Users/TAMIL TS/project/.venv/Airbnb/comment.csv")
import datetime
DF["Total_bedrooms"]=DF["Total_bedrooms"].fillna(DF["Total_bedrooms"].median())
DF["Description"]=DF["Description"].bfill()
DF["Name"]=DF["Name"].ffill()
DF['neighbourhood']=DF['neighbourhood'].fillna(DF['neighbourhood'].mode())
DF['neighbourhood']=DF['neighbourhood'].ffill()
DF['Review_scores']=DF['Review_scores'].fillna(DF['Review_scores'].mean())
DF['Amenities']=DF['Amenities'].bfill()
DF["Total_beds"]=DF["Total_beds"].fillna(DF["Total_beds"].median())
df2['comments']=df2['comments'].ffill()
df2['reviewer_name']=df2['reviewer_name'].bfill()
file_path = 'C:/Users/TAMIL TS/project/.venv/Airbnb/boxEDA.csv'
DF.to_csv(file_path, index=False)
file_path = 'C:/Users/TAMIL TS/project/.venv/Airbnb/commentEDA.csv'
df2.to_csv(file_path, index=False)


# In[36]:


#sql connection and inserting dataset into tables
import pandas as pd
DF=pd.read_csv("C:/Users/TAMIL TS/project/.venv/Airbnb/boxEDA.csv")
df=pd.read_csv("C:/Users/TAMIL TS/project/.venv/Airbnb/commentEDA.csv")
import pymysql
myconnection=pymysql.connect(host="127.0.0.1",user="root",passwd="Praveen@1234")
cur=myconnection.cursor()
cur.execute("create database if not exists Airbnb")
myconnection=pymysql.connect(host="127.0.0.1",user="root",passwd="Praveen@1234",database="Airbnb")
cur=myconnection.cursor()
cur.execute('''create table if not exists Main_table (Id int primary key, Name text, Description text, Property_type text, Room_type text, Bed_type text,
       Min_nights int, Max_nights int , Cancellation_policy text, Accomodates int,
       Total_bedrooms float, Total_beds float, Availability_365 int, Price float,
       calculated_host_listing_count int, neighbourhood text, No_of_reviews int,
       Review_scores float, Amenities text, Host_id int, Host_name text, Street text,
       Country text, Country_code text, Location_type text, Longitude float, Latitude float)''') 
sql='''insert ignore into Main_table(Id,Name,Description,Property_type,Room_type,Bed_type,Min_nights,Max_nights,Cancellation_policy,Accomodates,Total_bedrooms,Total_beds,
Availability_365,Price,calculated_host_listing_count,neighbourhood,No_of_reviews,Review_scores,Amenities,Host_id,Host_name,Street,Country,Country_code,
Location_type,Longitude,Latitude)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
for i in range(0,len(DF)):
    cur.execute(sql,tuple(DF.iloc[i]))
    myconnection.commit()
import datetime
df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
cur.execute('''create table if not exists reviews_table (Id int primary key , reviewer_id int, reviewer_name text,
comments text, Date date , Time text)''')
sqlr='''insert ignore into reviews_table (Id , reviewer_id, reviewer_name, comments, Date, Time)values(%s,%s,%s,%s,%s,%s)'''
for i in range(0,len(df)):
    cur.execute(sqlr,tuple(df.iloc[i]))
    myconnection.commit()


#sql connection
myconnection=pymysql.connect(host="127.0.0.1",user="root",passwd="Praveen@1234")
myconnection=pymysql.connect(host="127.0.0.1",user="root",passwd="Praveen@1234",database="airbnb")
cur=myconnection.cursor()

# Set page configuration
st.set_page_config(page_title="Airbnb Analysis")

#streamlit 
SELECT=option_menu(
    menu_title=None,
    options=["Home","Explore Data","Steps Done In The Project"],
    icons=["house","bar-chart","pen"],
    default_index=2,
    orientation="horizontal",
    styles={"container":{"padding":"0!important","background-color":"white","size":"cover","width":"100%"},
            "icon":{"color":"black","font-size":"20px"},
            "nav-link":{"font-size":"20px","test-align":"center","margin":"-2px","--hover-color":"#6F36AD"},
            "nav-link-selected":{"background-color":"#6F36ADj"}})


if SELECT=="Home":
    image1 = Image.open(r"C:\Users\TAMIL TS\project\.venv\Biz.jpg")
    st.sidebar.image(image1, use_column_width=True)   
    st.write('''Airbnb is an American San Francisco-based company operating an online marketplace for short- and long-term homestays 
             and experiences. The company acts as a broker and charges a commission from each booking. The company was founded 
             in 2008 by Brian Chesky, Nathan Blecharczyk, and Joe Gebbia. Airbnb is a shortened version of its original name,
               AirBedandBreakfast.com.Airbnb is the most well-known company for short-term housing rentals''')
    st.write(" ### Key features and aspects of Airbnb include:")
    st.write('''* Hosts and Guests: Airbnb provides a platform for hosts to list their properties and for guests to find and book 
             accommodations. Hosts can rent out their entire homes, private rooms, or shared spaces for short-term stays. ''')
    st.write('''*  Diverse Accommodations: Airbnb offers a wide range of accommodation options, including apartments,
              houses, villas, cabins, treehouses, and even unique or unconventional spaces''')
    st.write('''* Global Reach: Airbnb operates in countries around the world, making it a global marketplace for both 
             hosts and guests.''')
    st.write('''* Reviews and Ratings: Both hosts and guests can leave reviews and ratings after a stay,
              contributing to a reputation system that helps ensure trust and transparency within the community. ''')
    st.write('''* Hosts are responsible for setting house rules, pricing, and managing their listings. ''')
    st.write('''* Online Booking and Payments: Airbnb facilitates the booking process through its online platform.
              Guests can search for available properties, communicate with hosts, and make reservations. ''')
    

if SELECT=="Steps Done In The Project":
    st.sidebar.write(" ## Technologies Used:")
    st.sidebar.write("* Python")
    st.sidebar.write("* MongoDB atlas")
    st.sidebar.write("* Exploratory Data Analysis")
    st.sidebar.write("* MySQL")
    st.sidebar.write("* PowerBI")
    st.sidebar.write("* Streamlit")
    st.write("1.Establish a MongoDB connection, retrieve the Airbnb dataset")
    st.divider()
    st.write("2.Converting extracted data into DataFrame and then to csv file")
    st.divider()
    st.write("3.Clean and prepare the dataset, addressing missing values, duplicates, and data type conversions for accurate analysis")
    st.divider()
    st.write("4.Converting cleaned data to CSV file")
    st.divider()
    st.write("5.Using the extracted data for visualisation using powerBI and Streamlit")

if SELECT=="Explore Data":
    st.warning('''For the past projects i have made visualisation using streamlit. So for my skill development i have used PowerBI for visualization in this project.
             since i am using free version of powerBI i am unable to Publish it .Hence i decided to access it from my system''')
    




