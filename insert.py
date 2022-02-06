import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mflix"]


# Insert into comments
def insert_comments(**kwargs):
    comments = mydb["comments"]
    try:
        data = {
          "name": kwargs["name"],
          "email": kwargs["email"],
          "movie_id": {
            "$oid": kwargs["movie_id"]
          },
          "text": kwargs["text"],
          "date": {
            "$date": {
              "$numberLong": kwargs["date"]
            }
          }
        }
        comments.insert_one(data)
        print("Insert Successful")
    except KeyError:
        print("Exception occurred while creating the data: Key not present")
    except Exception:
        print("Error occured")


# Insert into movies
def insert_movies(**kwargs):
    movies = mydb["movies"]
    try:
        data = {
            "plot": kwargs["plot"],
            "genres": kwargs["genres"],
            "runtime": {
                "$numberInt": kwargs["runtime"]
            },
            "cast": kwargs["cast"],
            "num_mflix_comments": {
                "$numberInt": kwargs["num_mflix_comments"]
            },
            "title": kwargs["title"],
            "fullplot": kwargs["fullplot"],
            "countries": kwargs["countries"],
            "released": {
                "$date": {
                  "$numberLong": kwargs["released_date"]
                }
            },
            "directors": kwargs["directors"],
            "rated": kwargs["rated"],
            "awards": {
                "wins": {
                  "$numberInt": kwargs["awards_wins"]
                },
                "nominations": {
                  "$numberInt": kwargs["awards_nominations"]
                },
                "text": kwargs["awards_text"]
            },
            "lastupdated": kwargs["lastupdated"],
            "year": {
                "$numberInt": kwargs["year"]
            },
            "imdb": {
                "rating": {
                  "$numberDouble": kwargs["imdb_rating"]
                },
                "votes": {
                  "$numberInt": kwargs["imdb_votes"]
                },
                "id": {
                  "$numberInt": kwargs["imdb_id"]
                }
            },
            "type": kwargs["type"],
            "tomatoes": {
                "viewer": {
                  "rating": {
                    "$numberInt": kwargs["tomatoes_viewer_rating"]
                  },
                  "numReviews": {
                    "$numberInt": kwargs["tomatoes_viewer_numreviews"]
                  },
                  "meter": {
                    "$numberInt": kwargs["tomatoes_viewer_meter"]
                  }
                },
                "lastUpdated": {
                  "$date": {
                    "$numberLong": kwargs["tomatoes_lastupdated"]
                  }
                }
              }
        }
        movies.insert_one(data)
        print("Insert Successful")
    except KeyError:
        print("Exception occurred while creating the data: Key not present")
    except Exception:
        print("Error occured")


# Insert into theatres
def insert_theatres(tid, address, gtype, cx, cy):
    theatre = mydb["theatre"]
    try:
        data = {
            "theaterId": {
                "$numberInt": tid,
            },
            "location": {
                "address": address,
                "geo": {
                    "type": gtype,
                    "coordinates": [
                        {
                            "$numberDouble": cx
                        },
                        {
                            "$numberDouble": cy
                        }
                    ]
                }
            }
        }
        theatre.insert_one(data)
        print("Insert Successful")
    except KeyError:
        print("Exception occurred while creating the data: Key not present")
    except Exception:
        print("Error occured")


# Insert into users
def insert_users(name, email, password):
    # Accessing users collections from database
    users = mydb["users"]
    # Making Schema to insert
    try:
        data = {
            "name": name,
            "email": email,
            "password": password,
        }
        users.insert_one(data)
        print("Insert Successful")
    except KeyError:
        print("Exception occurred while creating the data: Key not present")
    except Exception:
        print("Error occured")

