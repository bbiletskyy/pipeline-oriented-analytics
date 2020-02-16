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
from pyspark.ml.regression import DecisionTreeRegressor, RandomForestRegressor, GBTRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from timeit import default_timer as timer
from datetime import timedelta


def main(argv):
    spark = SparkSession.builder \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

    features_df = ParquetDataFrame(f'data/processed/{Phase.train.name.lower()}/features', spark)
    test_data_frac = 0.1
    test_features_df, train_features_df = features_df.randomSplit([test_data_frac, 1 - test_data_frac])
    label_col = 'duration_min'

    assembler_estimator = Pipeline(stages=[
        StringIndexer(inputCol='pickup_cell_6', handleInvalid='keep', outputCol='pickup_cell_6_idx'),
        StringIndexer(inputCol='dropoff_cell_6', handleInvalid='keep', outputCol='dropoff_cell_6_idx'),
        VectorAssembler(inputCols=['pickup_cell_6_idx', 'dropoff_cell_6_idx', 'distance', 'month', 'day_of_month',
                                   'day_of_week', 'hour', 'requests_pickup_cell', 'requests_dropoff_cell'],
                        outputCol="features")
    ])

    dt_estimator = DecisionTreeRegressor(maxDepth=5, featuresCol='features', labelCol=label_col, maxBins=32)
    rf_estimator = RandomForestRegressor(maxDepth=5, numTrees=40, featuresCol='features', labelCol=label_col)

    pipeline = Pipeline(stages=[])
    dt_stages = [assembler_estimator, dt_estimator]
    rf_stages = [assembler_estimator, rf_estimator]

    dt_grid = ParamGridBuilder().baseOn({pipeline.stages: dt_stages}) \
        .addGrid(dt_estimator.maxDepth, [2, 5, 7, 9]) \
        .build()

    rf_grid = ParamGridBuilder().baseOn({pipeline.stages: rf_stages}) \
        .addGrid(rf_estimator.maxDepth, [5, 7]) \
        .addGrid(rf_estimator.numTrees, [10, 20]) \
        .build()

    grid = dt_grid + rf_grid

    eval_metric = 'mae'
    folds = 3
    print(f'Preparing {eval_metric} evaluator and {folds}-fold cross-validator...')
    mae_evaluator = RegressionEvaluator(metricName=eval_metric, labelCol=label_col)
    cross_val = CrossValidator(estimatorParamMaps=grid, estimator=pipeline,
                               evaluator=mae_evaluator, numFolds=folds, parallelism=4)

    print(f'Searching for parameters...')
    start = timer()
    cross_val_model = cross_val.fit(train_features_df)
    end = timer()
    print(f'Search complete, duration: {timedelta(seconds=end - start)}')
    print(f'Best model: {cross_val_model.bestModel.stages[1]}')

    predictions_df = cross_val_model.transform(test_features_df)
    mae_cv = RegressionEvaluator(labelCol=label_col, metricName=eval_metric).evaluate(predictions_df)
    print(f'Best model MAE: {mae_cv}')

    print(f'Best model parameters:')
    for item in cross_val_model.bestModel.stages[1].extractParamMap().items():
        print(f'- {item[0]}: {item[1]}')

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
