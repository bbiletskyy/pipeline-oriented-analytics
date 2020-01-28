import pytest
from pipeline_oriented_analytics.transformer import Union


class TestUnion(object):
    def test_union(self, spark):
        data1 = [(1, 234), (2, 76), (3, 1400)]
        cols = ['trip_is', 'duration']
        df_1 = spark.createDataFrame(data1, cols)
        data2 = [(4, 11), (5, 26)]
        df_2 = spark.createDataFrame(data2, cols)
        res_df = Union(df_1).transform(df_2)

        assert res_df.count() == 5
        assert res_df.columns == cols
