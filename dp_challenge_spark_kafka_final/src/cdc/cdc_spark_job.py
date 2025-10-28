from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, MapType

# Debezium envelope schema (minimal)
schema_after = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("price", DoubleType()),
    StructField("category", StringType()),
    StructField("updated_at", StringType())
])

schema_env = StructType([
    StructField("before", MapType(StringType(), StringType())),
    StructField("after", schema_after),
    StructField("op", StringType()),
    StructField("ts_ms", StringType())
])

def main():
    spark = SparkSession.builder.appName("CDCReader").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9093")
          .option("subscribe", "pgserver1.public.products")
          .option("startingOffsets", "earliest")
          .load()
          .selectExpr("CAST(value AS STRING) as json"))

    parsed = df.select(from_json(col("json"), schema_env).alias("e")).select("e.*")
    rows = parsed.select("op", "after.*")

    # write gold table
    query = (rows.writeStream
             .format("parquet")
             .option("path", "data/processed/products_gold")
             .option("checkpointLocation", "data/processed/checkpoints/products_gold")
             .outputMode("append")
             .start())

    query.awaitTermination()

if __name__ == "__main__":
    main()
