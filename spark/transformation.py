from pyspark.sql import SparkSession


if __name__ == '__main__':
    file_path = "/Users/raphael/Galpao/data_engineer/data_pipeline/datalake/twitter_alura_online"

    spark = SparkSession.builder \
        .master("local") \
        .appName("twitter_transformation") \
        .getOrCreate()


    df = spark.read.json(file_path)

    df.printSchema()
    df.show()