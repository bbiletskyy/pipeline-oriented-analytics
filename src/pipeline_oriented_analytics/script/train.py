#!/usr/bin/python

import sys
from pyspark.sql import SparkSession
from pipeline_oriented_analytics.dataframe import CsvDataFrame, ParquetDataFrame
from pipeline_oriented_analytics import Phase
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import PipelineModel


def main(argv):
    spark = SparkSession.builder \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

    features_df = ParquetDataFrame(f'data/processed/{Phase.train.name}/features', spark)
    test_data_frac = 0.1
    test_features_df, train_features_df = features_df.randomSplit([test_data_frac, 1 - test_data_frac])
    label_col = 'duration_min'
    model = Pipeline(stages=[
        StringIndexer(inputCol='pickup_cell_6', handleInvalid='keep', outputCol='pickup_cell_6_idx'),
        StringIndexer(inputCol='dropoff_cell_6', handleInvalid='keep', outputCol='dropoff_cell_6_idx'),
        VectorAssembler(inputCols=['pickup_cell_6_idx', 'dropoff_cell_6_idx', 'distance', 'month', 'day_of_month',
                                   'day_of_week', 'hour', 'requests_pickup_cell', 'requests_dropoff_cell'],
                        outputCol="features"),
        DecisionTreeRegressor(maxDepth=7, featuresCol='features', labelCol=label_col)
    ]).fit(train_features_df)

    model_path = 'model/trip_duration_min'
    print(f'Saving model to {model_path}')
    model.write().overwrite().save(model_path)
    print(f'Model saved...')

    model = PipelineModel.load(model_path)
    predictions_df = model.transform(test_features_df)
    mae_cv = RegressionEvaluator(labelCol=label_col, metricName='mae').evaluate(predictions_df)
    print(f'Mean absolutre error: {mae_cv}')

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
