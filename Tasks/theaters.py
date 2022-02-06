from pymongo import MongoClient
import json

# Set up Connection

myclient = MongoClient("mongodb://localhost:27017/")
mydb = myclient["mflix"]
theaters = mydb["theaters"]

# 1. Top 10 cities with the maximum number of theatres

def top_10_cities_with_max_num_of_theaters():
    result = theaters.aggregate([
        {"$group": {"_id": {"city": "$location.address.city"}, "totalTheaters": {"$sum": 1}}},
        {"$sort": {"totalTheaters": -1}},
        {"$limit": 10},
        {"$project": {"cityName": "$_id.city", "_id": 0, "totalTheaters": 1}}
    ])
    return result

# for i in top_10_cities_with_max_num_of_theaters():
#     print(i)

# 2. top 10 theatres nearby given coordinates

