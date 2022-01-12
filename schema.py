from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType, ArrayType

user_metrics_schema = StructType([
    StructField("followers_count", IntegerType()),
    StructField("following_count", IntegerType()),
    StructField("listed_count", IntegerType()),
    StructField("tweet_count", IntegerType()),
])

user_schema = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("public_metrics", user_metrics_schema),
    StructField("username", StringType())
])

tweet_schema = StructType([
    # data object
    StructField("data", StructType([
        StructField("author_id", StringType()),
        StructField("created_at", StringType()),
        StructField("id", StringType()),
        StructField("public_metrics", StructType([
            StructField("like_count", IntegerType()),
            StructField("quote_count", IntegerType()),
            StructField("reply_count", IntegerType()),
            StructField("retweet_count", IntegerType()),
        ])),
        StructField("text", StringType())
    ])),

    # includes object
    StructField("includes", StructType([
        StructField("users", ArrayType(
            user_schema
        ))
    ])),
])