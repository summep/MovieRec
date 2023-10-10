from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

def initialize_spark():
    return SparkSession.builder\
           .appName('movierec')\
           .getOrCreate()

def train_als_model(spark):
    ratings = spark.read.csv('data/ratings.csv', inferSchema=True, header=True)
    movies = spark.read.csv('data/movies.csv', inferSchema=True, header=True)
    ratings.join(movies, 'movieId')

    data = ratings.select('userId', 'movieId', 'rating')
    small_data = data.sample(False, 0.5)
    # Splits into two separate DataFrames, train and test
    splits = small_data.randomSplit([0.7, 0.3])
    train = splits[0].withColumnRenamed('rating', 'label')
    test = splits[1].withColumnRenamed('rating', 'trueLabel')

    # Initializes the ALS recommendation model
    # adjut iteration and regularization; coldStartStrategy="drop"
    als = ALS(maxIter=19, regParam=0.01, userCol='userId', itemCol='movieId', ratingCol='label')
    # train ALS model
    model = als.fit(train)
    return movies, train, als, model

# predict testing data
# pred = model.transform(test)
# pred.join(movies, 'movieId').select('userId', 'title', 'prediction', 'trueLabel').show(n=10, truncate=False)

# c = pred.count()
# print('# of original data rows: ', c)
# # drop rows with any missing data
# cleanPred = pred.dropna(how='any', subset=['prediction'])
# clean_c = cleanPred.count()
# print('# of missing data: ', c - clean_c)

# # evaluate accurary of the model
# eval = RegressionEvaluator(metricName='rmse', labelCol='trueLabel', predictionCol='prediction')
# rmse = eval.evaluate(cleanPred)
# print(f"RMSE: {rmse}")

# Recommend 10 movies for each user
# userRecs = model.recommendForAllUsers(10)
# specificUserRecs = userRecs.filter(userRecs.userId == 123)
# specificUserRecs = specificUserRecs.withColumn("exploded", explode("recommendations"))
# specificUserRecs = specificUserRecs.select("userId", "exploded.movieId", "exploded.rating")
# result = specificUserRecs.join(movies, specificUserRecs.movieId == movies.movieId)

# result.select("userId", "title", "rating").show(truncate=False)





# train_rows = train.count()
# test_rows = test.count()
# print('# of training data rows: ', train_rows)
# print('# of testing data rows: ', test_rows)

# df = spark.read.csv('data/ratings.csv', inferSchema=True, header=True)
# # sample a fraction of the data, no replacement during sampling, 1%
# small_df = df.sample(False, 0.01)

# # Splits the small_df into two separate DataFrames, train and test
# # train will contain roughly 70% of the rows, and test will contain the other 30%
# # Seed ensures reproducibility. Using the same seed will produce the same random split every time.
# (train, test) = small_df.randomSplit([0.7, 0.3], seed=42)

# # Initializes the ALS recommendation model
# # adjut iteration and regularization
# als = ALS(maxIter=10, regParam=0.15, userCol='userId', itemCol='movieId', ratingCol='rating', coldStartStrategy="drop")
# model = als.fit(train)
# pred = model.transform(test)
# #pred.show()

# eval = RegressionEvaluator(metricName='rmse', labelCol='rating', predictionCol='prediction')
# rmse = eval.evaluate(pred)
# # RMSE describes the error - 1.552674613686726
# print(f"RMSE: {rmse}")

# user_1 = test.filter(test['userId'] == 1).select(['movieId', 'userId'])
# user_1.show()
# rec = model.transform(user_1)
# rec.orderBy('prediction', ascending=False).show()