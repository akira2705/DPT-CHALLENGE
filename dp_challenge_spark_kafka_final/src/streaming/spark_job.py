import os
import pickle
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sklearn.linear_model import SGDRegressor

MODEL_PATH = "data/processed/sgd_amount_model.pkl"

schema = StructType([
    StructField("txn_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("device_lat", DoubleType()),
    StructField("device_lon", DoubleType()),
    StructField("category", StringType()),
    StructField("notes", StringType())
])

def get_or_init_model():
    if os.path.exists(MODEL_PATH):
        with open(MODEL_PATH, "rb") as f:
            return pickle.load(f)
    return SGDRegressor(max_iter=5, tol=1e-3)

def save_model(model):
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)

def preprocess(df):
    # Cast timestamp & fill missing
    df = df.withColumn("ts", to_timestamp(col("timestamp")))
    df = df.fillna({"amount": 0.0, "device_lat": 0.0, "device_lon": 0.0, "notes": ""})

    # Currency normalization → INR (toy FX)
    fx_expr = when(col("currency")=="USD", lit(83.0)) \
              .when(col("currency")=="EUR", lit(90.0)) \
              .otherwise(lit(1.0))
    df = df.withColumn("amount_in_inr", col("amount") * fx_expr)

    # Remove exact duplicates
    df = df.dropDuplicates(["txn_id","customer_id","timestamp","amount","currency","category"])

    # Feature engineering
    df = df.withColumn("hour", hour(col("ts")))
    df = df.withColumn("is_high_value", when(col("amount_in_inr") >= 500.0, lit(1)).otherwise(lit(0)))
    df = df.withColumn("amount_norm", col("amount_in_inr") / lit(1000.0))  # simple min-max-ish

    return df

def foreach_batch_incremental(batch_df, batch_id):
    import pandas as pd
    if batch_df.rdd.isEmpty():
        return
    pdf = batch_df.select("amount_in_inr","hour","category","is_high_value").toPandas()

    # One-hot top categories
    top_cats = list(pdf["category"].value_counts().head(5).index)
    for c in top_cats:
        pdf[f"cat_{c}"] = (pdf["category"] == c).astype(int)
    feat_cols = ["hour"] + [f"cat_{c}" for c in top_cats]
    if not feat_cols:
        return

    X = pdf[feat_cols].fillna(0.0).to_numpy(dtype=float)
    y = pdf["amount_in_inr"].to_numpy(dtype=float)

    model = get_or_init_model()
    model.partial_fit(X, y)        # incremental learning
    save_model(model)
    print(f"[Batch {batch_id}] Updated model with {len(y)} rows.")

def main():
    spark = (SparkSession.builder
             .appName("StreamingPreprocessAndML")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    df_raw = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", "localhost:9093")
              .option("subscribe", "transactions")
              .option("startingOffsets", "earliest")
              .load())

    df = df_raw.selectExpr("CAST(value AS STRING) as json") \
               .select(from_json(col("json"), schema).alias("d")).select("d.*")

    df_clean = preprocess(df)

    # Rolling aggregates (1-minute window)
    agg = (df_clean
        .withWatermark("ts", "2 minutes")
        .groupBy(window(col("ts"), "1 minute"), col("category"))
        .agg(count(lit(1)).alias("txn_count"),
             avg("amount_in_inr").alias("avg_amount_in_inr"))
        .selectExpr(
            "CAST(window.start AS STRING) AS window_start",
            "CAST(window.end AS STRING) AS window_end",
            "category",
            "txn_count",
            "ROUND(avg_amount_in_inr,2) AS avg_amount_in_inr"
        ))

    q1 = (agg.writeStream
          .outputMode("update")
          .format("console")
          .option("truncate", "false")
          .start())

    q2 = (df_clean.writeStream
          .foreachBatch(foreach_batch_incremental)
          .outputMode("update")
          .start())

    q1.awaitTermination()
    q2.awaitTermination()

if __name__ == "__main__":
    main()
