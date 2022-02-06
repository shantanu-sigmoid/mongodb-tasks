import pymongo
import json
from bson import ObjectId

# Establist the connection
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mflix"]
comments = mydb["comments"]

# Load Data
item_list = []
with open("/Users/shantanu/Downloads/sample_mflix/comments.json") as f:
    for json_obj in f:
        if json_obj:
            my_dict = json.loads(json_obj)
            my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])
            my_dict["date"] = my_dict["date"]["$date"]["$numberLong"]
            item_list.append(my_dict)
            
# Insert to Database
comments.insert_many(item_list)