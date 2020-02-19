import pytest

from pipeline_oriented_analytics.pipe import IF
from pipeline_oriented_analytics.transformer import SelectColumns


class TestIF(object):
    def test_transform_condition_true(self, spark):
        requests_data = [(1, 'Bob', 24)]
        column_names = ['id', 'name', 'age']
        df = spark.createDataFrame(requests_data, column_names)

        res_df = IF(IF.Predicate.const(True), then=[
            SelectColumns(['name'])
        ], otherwise=[
            SelectColumns(['age'])
        ]).transform(df)

        assert res_df.columns == ['name']

    def test_transform_condition_false(self, spark):
        requests_data = [(1, 'Bob', 24)]
        column_names = ['id', 'name', 'age']
        df = spark.createDataFrame(requests_data, column_names)

        res_df = IF(IF.Predicate.const(False), then=[
            SelectColumns(['name'])
        ], otherwise=[
            SelectColumns(['age'])
        ]).transform(df)

        assert res_df.columns == ['age']

    def test_transform_condition_has_rows(self, spark):
        requests_data = [(1, 'Bob', 24)]
        column_names = ['id', 'name', 'age']
        df = spark.createDataFrame(requests_data, column_names)

        res_df = IF(IF.Predicate.has_column('name'), then=[
            SelectColumns(['name'])
        ], otherwise=[
            SelectColumns(['age'])
        ]).transform(df)

        assert res_df.columns == ['name']
