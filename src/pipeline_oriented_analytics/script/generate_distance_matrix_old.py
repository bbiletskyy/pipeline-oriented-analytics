#!/usr/bin/python

import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import s2sphere
from pyspark.sql.types import StringType, FloatType


def main(argv):
    # print("This is the name of the script: ", sys.argv[0])
    # print("Number of arguments: ", len(sys.argv))
    # print("The arguments are: ", str(sys.argv))
    spark = SparkSession.builder \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

    def cell_token(level: int, lat: int, lng: int) -> str:
        return s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(lat, lng)).parent(level).to_token()

    cell_token_udf = f.udf(cell_token, StringType())

    def sphere_distance(token_from: str, token_to: str) -> float:
        r = 6373.0
        cell_from = s2sphere.CellId.from_token(token_from)
        cell_to = s2sphere.CellId.from_token(token_to)
        return cell_from.to_lat_lng().get_distance(cell_to.to_lat_lng()).radians * r

    sphere_distance_udf = f.udf(sphere_distance, FloatType())

    train_df = spark.read.option('header', 'true').csv('data/raw/train.csv') \
        .select('pickup_datetime', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude') \
        .withColumnRenamed('pickup_latitude', 'pickup_lat') \
        .withColumn('pickup_lat', f.col('pickup_lat').cast('double')) \
        .withColumnRenamed('pickup_longitude', 'pickup_lon') \
        .withColumn('pickup_lon', f.col('pickup_lon').cast('double')) \
        .withColumnRenamed('dropoff_latitude', 'dropoff_lat') \
        .withColumn('dropoff_lat', f.col('dropoff_lat').cast('double')) \
        .withColumnRenamed('dropoff_longitude', 'dropoff_lon') \
        .withColumn('dropoff_lon', f.col('dropoff_lon').cast('double')) \
        .withColumn('pickup_cell', cell_token_udf(f.lit(18), f.col('pickup_lat'), f.col('pickup_lon'))) \
        .withColumn('dropoff_cell', cell_token_udf(f.lit(18), f.col('dropoff_lat'), f.col('dropoff_lon')))

    test_df = spark.read.option('header', 'true').csv('data/raw/test.csv') \
        .select('pickup_datetime', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude') \
        .withColumnRenamed('pickup_latitude', 'pickup_lat') \
        .withColumn('pickup_lat', f.col('pickup_lat').cast('double')) \
        .withColumnRenamed('pickup_longitude', 'pickup_lon') \
        .withColumn('pickup_lon', f.col('pickup_lon').cast('double')) \
        .withColumnRenamed('dropoff_latitude', 'dropoff_lat') \
        .withColumn('dropoff_lat', f.col('dropoff_lat').cast('double')) \
        .withColumnRenamed('dropoff_longitude', 'dropoff_lon') \
        .withColumn('dropoff_lon', f.col('dropoff_lon').cast('double')) \
        .withColumn('pickup_cell', cell_token_udf(f.lit(18), f.col('pickup_lat'), f.col('pickup_lon'))) \
        .withColumn('dropoff_cell', cell_token_udf(f.lit(18), f.col('dropoff_lat'), f.col('dropoff_lon')))

    train_df.union(test_df) \
        .select('pickup_cell', 'dropoff_cell') \
        .dropDuplicates() \
        .withColumn('distance', sphere_distance_udf(f.col('pickup_cell'), f.col('dropoff_cell')))\
        .write.parquet('data/processed/distance_matrix', mode='overwrite')

    spark.stop()


if __name__ == "__main__":
   main(sys.argv[1:])
