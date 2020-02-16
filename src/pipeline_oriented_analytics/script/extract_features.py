#!/usr/bin/python

import sys
from pyspark.sql import SparkSession
from pipeline_oriented_analytics import Phase
import pyspark.sql.functions as f
from pipeline_oriented_analytics.pipe import Pipe, IF
from pipeline_oriented_analytics.transformer import *
from typing import List, Dict
from pipeline_oriented_analytics.dataframe import CsvDataFrame, ParquetDataFrame
from pipeline_oriented_analytics.pipe import Pipe, IF
from pipeline_oriented_analytics.transformer import *
from pipeline_oriented_analytics.transformer.feature import *
from typing import List, Dict
from pipeline_oriented_analytics.dataframe import CsvDataFrame, ParquetDataFrame
from pipeline_oriented_analytics import Phase



def main(argv):
    phase = Phase[argv[0]]

    spark = SparkSession.builder \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()
    print(f'Extracting features for {phase.name}')

    features_df = Pipe([
        Time('pickup_datetime',
             [Time.Feature.month, Time.Feature.day_of_month, Time.Feature.day_of_week, Time.Feature.hour]),
        AddMinutes(-15, 'pickup_datetime', '15_min_before'),
        RequestCount(15, 'pickup_cell_6', '15_min_before', 'requests_pickup_cell'),
        RequestCount(15, 'dropoff_cell_6', '15_min_before', 'requests_dropoff_cell'),
        IF(phase.is_train(), then=[
            Duration(Duration.Unit.minute, 'duration_sec', 'duration_min'),
            DropColumns(inputCols=['duration_sec'])
        ]),
        DropColumns(inputCols=['pickup_datetime', '15_min_before']),
        SaveToParquet(f'data/processed/{phase.name}/features')
    ]).transform(ParquetDataFrame(f'data/processed/{phase.name}/inputs', spark))

    print(f'Saved {features_df.count()} rows {phase.name} features')
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
