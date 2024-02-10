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


# In[ ]:





# In[39]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




