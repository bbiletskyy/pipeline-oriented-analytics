import pytest
import pyspark.sql.functions as F
from datetime import datetime

from pipeline_oriented_analytics.transformer.feature import RequestCount


class TestRequestCount(object):
    def test_transform_1_request(self, spark):
        data = [(1, datetime(2017, 7, 9, 16, 45, 59), 'ba64c31')]
        columns = ['id', 'timestamp', 'cell_id']
        df = spark.createDataFrame(data, columns)
        res_df = RequestCount(20, 'cell_id', 'timestamp', 'request_count').transform(df)
        assert set(res_df.columns) == {'id', 'timestamp', 'cell_id', 'request_count'}
        assert res_df.where(F.col('id') == 1).select('request_count').collect()[0][0] == 1

    def test_transform_2_requests(self, spark):
        data = [
            (1, datetime(2017, 7, 9, 16, 45, 59), 'ba64c31'),
            (2, datetime(2017, 7, 9, 16, 50, 59), 'ba64c31')
        ]
        columns = ['id', 'timestamp', 'cell_id']
        df = spark.createDataFrame(data, columns)
        res_df = RequestCount(20, 'cell_id', 'timestamp', 'request_count').transform(df)
        assert res_df.where(F.col('id') == 1).select('request_count').collect()[0][0] == 2
        assert res_df.where(F.col('id') == 2).select('request_count').collect()[0][0] == 2

    def test_transform_3_requests(self, spark):
        data = [
            (1, datetime(2017, 7, 9, 16, 45, 59), 'ba64c31'),
            (2, datetime(2017, 7, 9, 16, 50, 59), 'ba64c31'),
            (3, datetime(2017, 7, 9, 16, 50, 59), 'ba64c32')
        ]
        columns = ['id', 'timestamp', 'cell_id']
        df = spark.createDataFrame(data, columns)
        res_df = RequestCount(20, 'cell_id', 'timestamp', 'request_count').transform(df)
        assert res_df.where(F.col('id') == 1).select('request_count').collect()[0][0] == 2
        assert res_df.where(F.col('id') == 2).select('request_count').collect()[0][0] == 2
        assert res_df.where(F.col('id') == 3).select('request_count').collect()[0][0] == 1
