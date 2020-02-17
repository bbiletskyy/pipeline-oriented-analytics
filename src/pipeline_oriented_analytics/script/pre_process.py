#!/usr/bin/python

import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pipeline_oriented_analytics.pipe import Pipe, IF
from pipeline_oriented_analytics.transformer import *
from typing import List, Dict
from pipeline_oriented_analytics.dataframe import CsvDataFrame, ParquetDataFrame
from pipeline_oriented_analytics import Phase


def main(argv):
    phase = Phase[argv[0]]
    #phase = Phase.train

    spark = SparkSession.builder \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

    variables = ['id', 'passenger_count', 'pickup_datetime', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude',
                 'dropoff_latitude']
    lables = ['trip_duration']
    column_names = {'pickup_longitude': 'pickup_lon', 'pickup_latitude': 'pickup_lat',
                    'dropoff_longitude': 'dropoff_lon', 'dropoff_latitude': 'dropoff_lat',
                    'trip_duration': 'duration_sec'}
    variable_types = {'id': 'string', 'passenger_count': 'integer', 'pickup_datetime': 'timestamp',
                      'pickup_lon': 'double', 'pickup_lat': 'double', 'dropoff_lon': 'double', 'dropoff_lat': 'double'}
    label_types = {'duration': 'int'}

    if phase.is_predict():
        columns = variables
        column_types = variable_types
        data_path = 'data/raw/test.csv'
    else:
        columns = variables + lables
        column_types = {**variable_types, **label_types}
        data_path = 'data/raw/train.csv'

    df = Pipe([
        SelectColumns(columns),
        RenameColumns(column_names),
        NormalizeColumnTypes(column_types),
        CellToken(6, 'pickup_lat', 'pickup_lon', 'pickup_cell_6'),
        CellToken(6, 'dropoff_lat', 'dropoff_lon', 'dropoff_cell_6'),
        CellToken(14, 'pickup_lat', 'pickup_lon', 'pickup_cell_14'),
        CellToken(14, 'dropoff_lat', 'dropoff_lon', 'dropoff_cell_14'),
        Join(['pickup_cell_14', 'dropoff_cell_14'], Join.Method.left,
             ParquetDataFrame('data/processed/distance_matrix', spark)),
        DropColumns(
            inputCols=['pickup_lat', 'pickup_lon', 'dropoff_lon', 'dropoff_lat', 'pickup_cell_14', 'dropoff_cell_14']),
        SaveToParquet(f'data/processed/{phase.name}/inputs'),
    ]).transform(CsvDataFrame(data_path, spark))

    print(f'Saved {df.count()} rows of {phase.name} inputs')
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
