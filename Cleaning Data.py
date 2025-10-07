from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, month

spark = SparkSession.builder.appName("TaxiZoneFilter").getOrCreate()

df = spark.read.format("delta").table("stg.Taxi_Jan")

df = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime"))) \
       .withColumn("dropoff_date", to_date(col("tpep_dropoff_datetime")))

january_df = df.filter(month(col("pickup_date")) == 1)

january_df.write.mode("overwrite").format("delta").saveAsTable("stg.Taxi_Jan_Clean")

print("✅ January data filtered and saved successfully into stg.Taxi_Jan_Clean")

