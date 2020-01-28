import pytest

from datetime import datetime

from pipeline_oriented_analytics.transformer import DropDuplicates


class TestDropDuplicates(object):
    def test_drop_duplicates(self, spark):
        data = [(1, datetime(2017, 11, 28, 23, 55, 59, 11), 'London'),
                (2, datetime(2018, 1, 1, 0, 0, 0, 0), 'Paris'),
                (2, datetime(2018, 1, 1, 0, 0, 0, 0), 'Paris')]
        columns = ['id', 'service_time', 'city']
        df = spark.createDataFrame(data, columns)

        res_df = DropDuplicates().transform(df)
        assert res_df.count() == 2

    def test_drop_duplicates_in_columns(self, spark):
        data = [(1, datetime(2017, 11, 28, 23, 55, 59, 11), 'London'),
                (2, datetime(2018, 1, 1, 0, 0, 0, 0), 'Paris'),
                (3, datetime(2018, 1, 1, 0, 0, 0, 0), 'Paris')]
        columns = ['id', 'service_time', 'city']
        df = spark.createDataFrame(data, columns)

        res_df = DropDuplicates(['service_time', 'city']).transform(df)
        assert res_df.count() == 2
