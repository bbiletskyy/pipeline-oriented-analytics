#!/usr/bin/python

import sys
from pyspark.sql import SparkSession
from pipeline_oriented_analytics import Phase
from pipeline_oriented_analytics.dataframe import ParquetDataFrame
from pipeline_oriented_analytics.transformer import SaveToParquet, DropColumns
from pyspark.ml import PipelineModel


def main(argv):
    spark = SparkSession.builder \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

    phase = Phase.predict

    data_to_predict_path = f'data/processed/{phase.name}/features'
    model_path = 'model/trip_duration_min'
    predicted_data_path = 'data/reporting/trip_durations'

    model = PipelineModel.load(model_path)

    predicted_df = PipelineModel([
        model,
        DropColumns(inputCols=['features']),
        SaveToParquet(predicted_data_path)
    ]).transform(ParquetDataFrame(data_to_predict_path, spark))

    predicted_df.show(2)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
