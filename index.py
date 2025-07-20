
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
uri = "mongodb+srv://yousefmarey12:yousefabdelrahman@cluster0.pmpseq0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    pf =  pd.read_csv('movies.csv')
    obj = pf.to_dict('records')
    db=  client.get_database('Cluster0')
    movies= db.get_collection('movies')
    movies.insert_many(obj)
    movies.insert_one({
        "movieId": 9999,
        "title": "AI Revolution (2025)",
        "genres": "Sci-Fi|Thriller"
    })
    movies.insert_one({
        "movieId": 10000,
        "title": " The Last Code",
        "genres": "Action|Drama"
    })
    movies.insert_many([
       {
        "movieId": 10001,
         "title": "Data Dreams",
     },
    {
        "movieId": 10002,
         "title": "Documentary",
     },
    ])
    movies.update_one(
    {"title": "AI Revolution (2025)"},
    {"$set": {"genres": "Sci-Fi|Action"}}
)
    stuff= movies.update_many(
    {"genres": {"$regex": "Drama"}},
    {"$set": {"genre": "Drama|Updated"}}
    )
    strc =str(stuff.matched_count)
    print(" ackn"  + strc )
    movies.delete_one({"title": "Python Power"})
    movies.delete_many({"genres": "Documentary"})
    for doc in movies.find({"title": {"$regex": "Toy", "$options": "i"}}):
        print(doc)

    for doc in movies.find({"genres": {"$regex": "Comedy"}}):
        print(doc)    
    for doc in movies.find({"movieId": {"$gt": 1000}}):
        print(doc)    
    for doc in movies.find({}, {"_id": 0, "title": 1, "genres": 1}):
        print(doc)
    for doc in movies.find({"movieId": {"$lt": 10}}, {"_id": 0, "movieId": 1, "title": 1}):
        print(doc)  
    for doc in movies.find({
    "genres": {"$regex": "Drama"},
    "movieId": {"$gt": 10000}
}):
        print(doc) 
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)