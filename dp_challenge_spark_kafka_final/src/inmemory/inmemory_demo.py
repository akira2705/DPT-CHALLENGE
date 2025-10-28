from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

def main():
    spark = SparkSession.builder.appName("InMemoryDemo").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("header", True).csv("data/raw/transactions.csv", inferSchema=True)

    print("Before cache count:", df.count())
    df_cached = df.cache()
    print("After cache count:", df_cached.count())

    df_cached.groupBy("currency").agg(avg(col("amount")).alias("avg_amount")).show(truncate=False)

    rdd = spark.sparkContext.parallelize([("a",1),("b",2),("a",3),("c",4)])
    print("RDD reduceByKey:", rdd.reduceByKey(lambda a,b: a+b).collect())

if __name__ == "__main__":
    main()
