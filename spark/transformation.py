from pyspark.sql import SparkSession
import argparse
from pyspark.sql import functions as f
from os.path import join


def get_tweets_data(df):
    return df.select(
        f.explode("data").alias("tweets")
    ).select(
        "tweets.author_id",
        "tweets.conversation_id",
        "tweets.created_at",
        "tweets.id",
        "tweets.in_reply_to_user_id",
        "tweets.text",
        "tweets.public_metrics.*"
    )


def get_users_data(df):
    return df.select(
        f.explode("includes.users").alias("users")
    ).select(
        "users.*"
    )


def export_json(df, path_dest):
    df.write.mode("overwrite").json(path_dest)


def twitter_transform(spark, source, path_dest, process_date):
    df = spark.read.json(source)

    tweet_df = get_tweets_data(df)
    user_df = get_users_data(df)

    table_dest = join(path_dest, "{table_name}", f"process_date={process_date}")

    export_json(tweet_df, table_dest.format(table_name="tweet"))
    export_json(user_df, table_dest.format(table_name="user"))


if __name__ == '__main__':
    # for send args to SparkJob
    parser = argparse.ArgumentParser(description="Spark Twitter Transformation")
    parser.add_argument("--source", required=False)
    parser.add_argument("--path-dest", required=False)
    parser.add_argument("--process-date", required=True)
    args = parser.parse_args()


    spark = SparkSession.builder \
        .master("local") \
        .appName("twitter_transformation") \
        .getOrCreate()
    
    source = f'/Users/raphael/Galpao/data_engineer/data_pipeline/datalake/bronze/twitter_alura_online/extract_date={args.process_date}'
    path_dest = '/Users/raphael/Galpao/data_engineer/data_pipeline/datalake/silver'
    
    twitter_transform(spark, source, path_dest, args.process_date)
