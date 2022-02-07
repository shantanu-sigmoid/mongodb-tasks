from pymongo import MongoClient
import json

# Set up Connection

myclient = MongoClient("mongodb://localhost:27017/")
mydb = myclient["mflix"]
comments = mydb["comments"]

# 1. Find top 10 users who made the maximum number of comments 

top_10_user_with_max_comments = comments.aggregate([
    {"$group": {"_id": {"email": "$email", "name": "$name"}, "commentsMade": {"$sum": 1}}},
    {"$sort": {"commentsMade": -1}},
    {"$limit": 10}
])

# 2. Find top 10 movies with most comments

top_10_movies_with_most_comments = comments.aggregate([
    {"$group": {"_id": {"movie": "$movie_id"}, "commentsMade": {"$sum": 1}}},
    {"$sort": {"commentsMade": -1}},
    {"$limit": 10}
])

# 3. Given a year find the total number of comments created each month in that year

def total_comments_each_month_given_year(year):
    result = comments.aggregate([
        {"$project": {"_id": 0, "date": {"$toDate": {"$convert": {"input": "$date", "to": "long"}}}}}, 
        {"$group": {
            "_id": {
                "year": {"$year": "$date"}, 
                "month": {"$month": "$date"}
            }, 
            "totalPerson": {"$sum": 1}}
        },
        {"$match": {"_id.year": {"$eq": year}}},
        {"$sort": {"_id.month": 1}}
    ])
    return result

for i in total_comments_each_month_given_year(2012):
    print(i)


