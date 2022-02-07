from pymongo import MongoClient
import json

# Set up Connection

myclient = MongoClient("mongodb://localhost:27017/")
mydb = myclient["mflix"]
movies = mydb["movies"]

# 1. Find top `N` movies

##   i. with the highest IMDB rating (ISSUE: $numberInt, $numberDouble, None)

def top_N_movies_with_highest_IMDB_rating(N):
    result = movies.aggregate([
        {"$project": {"_id": 0, "imdb.rating": 1, "title": 1}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": N}
    ])
    return result

# for i in top_N_movies_with_highest_IMDB_rating(10):
#     print(i)

##   ii. with the highest IMDB rating in a given year

def highest_IMDB_given_year(N):
    result = movies.aggregate([
        {"$match": {"year": N}},
        {"$project": {"_id": 0, "title": 1, "imdb.rating": 1}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": 1}
    ])
    return result

# for i in highest_IMDB_given_year(1986):
#     print(i)

##   iii. with highest IMDB rating with number of votes > 1000

def highest_IMDB_votes_greater_1000():
    result = movies.aggregate([
        {"$match": {"imdb.votes": {"$gt": 1000}}},
        {"$project": {"_id": 0, "title": 1, "imdb.rating": 1}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": 1}
    ])
    return result

# for i in highest_IMDB_votes_greater_1000():
#     print(i)

##    iv. with title matching a given pattern sorted by highest tomatoes ratings

def pattern_matching_highest_tomatoes_rating(pattern, N):
    result = movies.aggregate([
        {"$match": {"title": {"$regex": pattern}}},
        {"$project": {"_id": 0, "title": 1, "tomatoes.viewer.rating": 1}},
        {"$sort": {"tomatoes.viewer.rating": -1}},
        {"$limit": N}
    ])
    return result

# for i in pattern_matching_highest_tomatoes_rating("Scene", 10):
#     print(i)

# 2. Find top `N` directors -

##    i. who created the maximum number of movies

def top_N_directors_created_max_num_of_movies(N):
    result = movies.aggregate([
        {"$unwind": "$directors"},
        {"$group": {"_id": {"directors": "$directors"}, "totalFilms": {"$sum": 1}}},
        {"$sort": {"totalFilms": -1}},
        {"$limit": N}
    ])
    return result

# for i in top_N_directors_created_max_num_of_movies(10):
#     print(i)

##    ii. who created the maximum number of movies in a given year

def top_N_directors_created_max_movies_given_year(year, N):
    result = movies.aggregate([
        {"$unwind": "$directors"},
        {"$group": {"_id": {"directors": "$directors", "year": "$year"}, "totalFilms": {"$sum": 1}}},
        {"$sort": {"totalFilms": -1}},
        {"$match": {"_id.year": year}},
        {"$project": {"_id.directors": 1, "totalFilms": 1}},
        {"$limit": N}
    ])
    return result

# for i in top_N_directors_created_max_movies_given_year(2000, 10):
#     print(i)

##    iii. who created the maximum number of movies for a given genre

def top_N_directors_created_max_movies_given_genre(genre, N):
    result = movies.aggregate([
        {"$unwind": "$directors"},
        {"$unwind": "$genres"},
        {"$group": {"_id": {"directors": "$directors", "genres": "$genres"}, "totalFilms": {"$sum": 1}}},
        {"$sort": {"totalFilms": -1}},
        {"$match": {"_id.genres": genre}},
        {"$project": {"totalFilms": 0}},
        {"$limit": N}
    ])
    return result

# for i in top_N_directors_created_max_movies_given_genre("Drama", 10):
#     print(i)

# 3. Find top `N` actors - 

##     i. who starred in the maximum number of movies

def top_N_actors_starred_in_max_num_of_movies(N):
    result = movies.aggregate([
        {"$unwind": "$cast"},
        {"$group": {"_id": {"cast": "$cast"}, "totalFilms": {"$sum": 1}}},
        {"$sort": {"totalFilms": -1}},
        {"$limit": N}
    ])
    return result

# for i in top_N_actors_starred_in_max_num_of_movies(10):
#     print(i)

##     ii. who starred in the maximum number of movies in a given year

def top_N_actors_starred_in_max_num_of_movies_given_year(year, N):
    result = movies.aggregate([
        {"$unwind": "$cast"},
        {"$group": {"_id": {"cast": "$cast", "year": "$year"}, "totalFilms": {"$sum": 1}}},
        {"$sort": {"totalFilms": -1}},
        {"$match": {"_id.year": year}},
        {"$project": {"_id.year": 0}},
        {"$limit": N}
    ])
    return result

# for i in top_N_actors_starred_in_max_num_of_movies_given_year(2000, 10):
#     print(i)

##      iii. who starred in the maximum number of movies for a given genre

def top_N_actors_starred_in_max_num_of_movies_given_genre(genre, N):
    result = movies.aggregate([
        {"$unwind": "$cast"},
        {"$unwind": "$genres"},
        {"$group": {"_id": {"cast": "$cast", "genres": "$genres"}, "totalFilms": {"$sum": 1}}},
        {"$sort": {"totalFilms": -1}},
        {"$match": {"_id.genres": genre}},
        {"$project": {"_id.genres": 0}},
        {"$limit": N}
    ])
    return result

# for i in top_N_actors_starred_in_max_num_of_movies_given_genre("Drama", 10):
#     print(i)

# 4. Find top `N` movies for each genre with the highest IMDB rating

def top_N_movies_for_each_genre_with_highest_IMDB(N):
    result = movies.aggregate([
        {"$unwind": "$genres"},
        {"$group": {"_id": {"genres": "$genres"}, "filmPlusRating": {"$push": {"title": "$title", "rating": "$imdb.rating"}}}},
        {"$unwind": "$filmPlusRating"},
        {"$sort": {"filmPlusRating.rating": -1}},
        {"$group": {"_id": {"genres": "$_id.genres"}, "filmPlusRating": {"$push": "$filmPlusRating"}}},
        {"$project": {"_id": 1, "filmPlusRating": {"$slice": ["$filmPlusRating", N]}}}
    ])
    return result

# for i in top_N_movies_for_each_genre_with_highest_IMDB(10):
#     print(i)