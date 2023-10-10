from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from pyspark.sql import Row
from pyspark.sql.functions import explode
from spark_utils import initialize_spark, train_als_model


app = Flask(__name__)
CORS(app, expose_headers="*", allow_headers="*", origins="*")

all_movies = pd.read_csv('data/movies.csv')
def clean_title(title):
    return re.sub("[^a-zA-Z0-9 ]", "", title)
all_movies['clean_title'] = all_movies['title'].apply(clean_title)

@app.route('/search', methods=['POST'])
def search():
    reqBody = request.get_json()
    inputTitle = reqBody['input']
    # look at groups of two words in consecutive
    vectorizer = TfidfVectorizer(ngram_range=(1,2))
    # turn titles into numbers for similarity comparison
    tfidf = vectorizer.fit_transform(all_movies['clean_title'])
    query_vec = vectorizer.transform([inputTitle])
    # numpy vector of similaroty numbers
    similarity = cosine_similarity(query_vec, tfidf).flatten()
    # find titles of top 5 greatest similarity 
    indices = np.argpartition(similarity, -5)[-5:]
    result = all_movies.iloc[indices][::-1]
    return jsonify(result.to_dict(orient='records'))


spark = initialize_spark()
movies,train, als, model = train_als_model(spark)

@app.route('/recommend', methods=['POST'])
def recommend():
    reqBody = request.get_json()
    # create pseudo-user ratings
    new_user_id = 999999
    rows = [Row(userId=new_user_id, movieId=movie['movieId'], rating=5.0) for movie in reqBody['input']]
    pseudo_ratings = spark.createDataFrame(rows)
    combined_ratings = train.union(pseudo_ratings)
    new_model = als.fit(combined_ratings)
    userRecs = new_model.recommendForUserSubset(pseudo_ratings.select("userId"), 10)
    # filter out the original movies
    userRecs = userRecs.withColumn("exploded", explode("recommendations"))
    userRecs = userRecs.select("userId", "exploded.movieId", "exploded.rating")
    result = userRecs.join(movies, userRecs.movieId == movies.movieId).select(
        userRecs.userId, userRecs.movieId, movies.title, userRecs.rating
        )
    return jsonify(result.toPandas().to_json(orient='records'))

if __name__ == '__main__':
    app.run(debug=True, port=5000)

