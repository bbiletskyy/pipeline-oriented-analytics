#!/usr/bin/python

import sys

from pipeline_oriented_analytics.pipe import Pipe
from pipeline_oriented_analytics.transformer import *
from pipeline_oriented_analytics.dataframe import *
from typing import List, Dict


def main(argv):
    spark = SparkSession.builder \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

    column_names = ['pickup_datetime', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude']
    column_new_names = {'pickup_longitude': 'pickup_lon', 'pickup_latitude': 'pickup_lat',
                        'dropoff_longitude': 'dropoff_lon', 'dropoff_latitude': 'dropoff_lat'}
    column_types = {'pickup_lon': 'double', 'pickup_lat': 'double', 'dropoff_lon': 'double', 'dropoff_lat': 'double'}
    level = 14
    pickup_cell = f'pickup_cell_{level}'
    dropoff_cell = f'dropoff_cell_{level}'

    def PREPARE_TRIP_DATA(level: int, column_names: List[str], column_new_names: Dict[str, str],
                          column_types: Dict[str, str]) -> Pipe:
        return Pipe([
            SelectColumns(column_names),
            RenameColumns(column_new_names),
            NormalizeColumnTypes(column_types),
            CellToken(level, 'pickup_lat', 'pickup_lon', pickup_cell),
            CellToken(level, 'dropoff_lat', 'dropoff_lon', dropoff_cell)
        ])

    df = Pipe([
        PREPARE_TRIP_DATA(level, column_names, column_new_names, column_types),
        Union(
            PREPARE_TRIP_DATA(level, column_names, column_new_names, column_types).transform(
                CsvDataFrame('data/raw/test.csv', spark))
        ),
        SelectColumns([pickup_cell, dropoff_cell]),
        DropDuplicates(),
        SphereDistance(pickup_cell, dropoff_cell),
        SaveToParquet('data/processed/distance_matrix')
    ]).transform(CsvDataFrame('data/raw/train.csv', spark))

    print(f'{df.count()} {level}-level distance pairs generated')
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
