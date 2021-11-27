from pyspark.sql import SparkSession
import argparse
from pyspark.sql import functions as f
from os.path import join
from os import listdir


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


def twitter_transform(spark, source, path_dest, extract_date):
    df = spark.read.json(source)

    tweet_df = get_tweets_data(df)
    user_df = get_users_data(df)

    table_dest = join(path_dest, "{table_name}", f"extract_date={extract_date}")

    export_json(tweet_df, table_dest.format(table_name="tweet"))
    export_json(user_df, table_dest.format(table_name="user"))


if __name__ == '__main__':
    # for send args to SparkJob
    parser = argparse.ArgumentParser(description="Spark Twitter Transformation")
    parser.add_argument("--source", required=False)
    parser.add_argument("--path-dest", required=False)
    parser.add_argument("--extract-date", required=False)
    args = parser.parse_args()


    spark = SparkSession.builder \
        .master("local") \
        .appName("twitter_transformation") \
        .getOrCreate()

    path = '/Users/raphael/Galpao/data_engineer/data_pipeline/datalake/{}/twitter_alura_online'
    path_dest = path.format('silver')
    source_path = path.format('bronze')
    
    folders = [folder for folder in listdir(source_path)]
    extract_date = max(folders)

    extract_date = f'extract_date={args.extract_date}' if args.extract_date else extract_date
    source = f"{path.format('bronze')}/{extract_date}"

    twitter_transform(spark, source, path_dest, extract_date)
