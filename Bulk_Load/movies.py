import pymongo
import json
from bson import ObjectId


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mflix"]
movies = mydb["movies"]
item_list = []
with open("/Users/shantanu/Downloads/sample_mflix/movies.json") as f:
    for json_obj in f:
        if json_obj:
            my_dict = json.loads(json_obj)
            my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])
            try:
                my_dict["runtime"] = int(my_dict["runtime"]["$numberInt"])
            except:
                pass
            try:
                my_dict["num_mflix_comments"] = int(my_dict["num_mflix_comments"]["$numberInt"])
            except:
                pass
            try:
                my_dict["released"] = int(my_dict["released"]["$date"]["$numberLong"])
            except:
                pass
            try:
                my_dict["year"] = int(my_dict["year"]["$numberInt"])
            except:
                pass
            try:
                my_dict["imdb"]["votes"] = int(my_dict["imdb"]["votes"]["$numberInt"])
            except:
                pass
            try:
                my_dict["imdb"]["id"] = int(my_dict["imdb"]["id"]["$numberInt"])
            except:
                pass
            try:
                my_dict["tomatoes"]["viewer"]["rating"] = int(my_dict["tomatoes"]["viewer"]["rating"]["$numberInt"])
            except:
                pass
            try:
                my_dict["tomatoes"]["viewer"]["numReviews"] = int(my_dict["tomatoes"]["viewer"]["numReviews"]["$numberInt"])
            except:
                pass
            try:
                my_dict["tomatoes"]["viewer"]["meter"] = int(my_dict["tomatoes"]["viewer"]["meter"]["$numberInt"])
            except:
                pass
            try:
                my_dict["tomatoes"]["lastUpdated"] = int(my_dict["tomatoes"]["lastUpdated"]["$date"]["$numberLong"])
            except:
                pass
            try:
                my_dict["awards"]["wins"] = int(my_dict["awards"]["wins"]["$numberInt"])
            except:
                pass
            try:
                my_dict["awards"]["nominations"] = int(my_dict["awards"]["nominations"]["$numberInt"])
            except:
                pass
            try:
                my_dict["imdb"]["rating"] = float(my_dict["imdb"]["rating"]["$numberDouble"])
            except:
                pass
            item_list.append(my_dict)
movies.insert_many(item_list)

