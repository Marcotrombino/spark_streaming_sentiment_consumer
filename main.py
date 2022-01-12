# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 main.py
import random as rand
from pyspark.sql import SparkSession, functions as fn
from pyspark.sql.types import FloatType, StringType
from schema import user_metrics_schema, tweet_schema
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

if __name__ == '__main__':

    spark = SparkSession.builder.appName("twitterFinanceStreaming").getOrCreate()

    analyser = spark.sparkContext.broadcast(SentimentIntensityAnalyzer())

    def get_tweet_sentiment(tweet_text: str):
        # return rand.random()
        print(analyser.value)
        result = analyser.value.polarity_scores(tweet_text)
        if result['compound'] >= 0.05:
            return "Positive"
        elif result['compound'] <= - 0.05:
            return "Negative"
        else:
            return "Neutral"


    tweet_sentiment_udf = fn.udf(lambda t: get_tweet_sentiment(t))

    # read raw tweets from Kafka
    raw_tweets = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "tweets") \
        .option("startingOffsets", "latest")\
        .load() \

    # read json schema as "parsed" field
    parsed_tweets = raw_tweets\
        .select(
            fn.from_json(
                fn.col("value").cast("string"),
                schema=tweet_schema
            ).alias("parsed")
        )

    # resulting tweets dataframe
    tweets_df = parsed_tweets\
        .withColumn(                                    # get first user metrics
            "user",
            fn.col("parsed.includes.users")\
            .getItem(0)\
            .getField("public_metrics")
        )\
        .select(
            fn.col("parsed.data.*"),                      # tweets properties
            # fn.col("parsed.data.public_metrics.*"),     # public_metrics properties

            *[  # extract and rename user metrics
                # https://stackoverflow.com/questions/41655158/dynamically-rename-multiple-columns-in-pyspark-dataframe
                fn.col("user." + f_name).alias("user_" + f_name) for f_name in user_metrics_schema.fieldNames()
            ]
        )\
        .drop(fn.col("public_metrics"))

    tweets_df.printSchema()

    # tweet overall sentiment query
    tweets_query = tweets_df\
        .filter(fn.col("user_followers_count") > 500)\
        .withColumn("sentiment", tweet_sentiment_udf("text").cast(StringType()))\
        .select(
            fn.col("text"),
            fn.col("sentiment")
        )\
        #.groupBy(fn.col("sentiment"))\
        #.count()

    # .outputMode("complete")\

    # write sentiment stream to console
    tweets_query\
        .writeStream\
        .outputMode("update") \
        .format("console")\
        .option("truncate", "false")\
        .queryName("financeTweets")\
        .start()\
        .awaitTermination()

    spark.stop()