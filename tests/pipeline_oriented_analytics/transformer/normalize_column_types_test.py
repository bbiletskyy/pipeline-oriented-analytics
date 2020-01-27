import pytest

import pyspark.sql.functions as f
from pipeline_oriented_analytics.transformer import NormalizeColumnTypes


class TestNormalizeColumnTypes(object):
    def test_normalize_column_types(self, spark):
        data = [
            ('Alice', 20.0, '2017-07-09 11:35:00'),
            ('Bob', 0.0, '2017-08-24 19:25:00'),
            ('Nina', 40.0, None)
        ]
        columns = ['driver_id', 'age', 'time']
        df = spark.createDataFrame(data, columns)
        column_types = {'driver_id': 'string', 'age': 'int', 'time': 'timestamp'}

        res_df = NormalizeColumnTypes(column_types).transform(df)
        assert set(column_types.keys()) == set(res_df.schema.names)
        assert df.count() == res_df.count()
        for col, dtype in res_df.dtypes:
            assert dtype == column_types[col]

    def test_normalize_column_types_with_redundant_columns(self, spark):
        data = [('Alice', 20.0, '2017-07-09 11:35:00'), ('Bob', 0.0, '2017-07-09 11:35:00'), ('Nina', 40.0, None)]
        columns = ['driver_id', 'age', 'time']
        df = spark.createDataFrame(data, columns)
        column_types = {'driver_id': 'string', 'age': 'int', 'occupation': 'timestamp'}

        res_df = NormalizeColumnTypes(column_types).transform(df)
        assert df.count() == res_df.count()
        for col, dtype in res_df.dtypes:
            if col in column_types:
                assert dtype == column_types[col]

    def test_wrong_types(self, spark):
        data = [('Alice', 20.0, '2017-07-09 11:35:00'), ('Bob', 0.0, '2017-07-09 11:35:00'), ('Nina', 40.0, None)]
        columns = ['driver_id', 'age', 'time']
        df = spark.createDataFrame(data, columns)
        column_types = {'driver_id': 'string', 'age': 'int', 'time': 'double'}

        res_df = NormalizeColumnTypes(column_types).transform(df)
        assert res_df.where(f.col('time').isNull()).count() == 3

    def test_date_time_formats(self, spark):
        data = [('1', '2017-07-09', '19/07/2017 11:35:00')]
        columns = ['trip_id', 'day', 'time']
        df = spark.createDataFrame(data, columns)
        column_types = {'trip_id': 'int', 'day': 'date(YYYY-MM-dd)', 'time': 'timestamp(dd/MM/YYYY HH:mm:ss)'}

        res_df = NormalizeColumnTypes(column_types).transform(df)
        res_column_types = dict(res_df.dtypes)
        assert res_column_types['trip_id'] == 'int'
        assert res_column_types['day'] == 'date'
        assert res_column_types['time'] == 'timestamp'
