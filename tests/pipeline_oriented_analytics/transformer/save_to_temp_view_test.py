import pytest

from pipeline_oriented_analytics.transformer import SaveToTempView


class TestSaveToTempView(object):
    def test_transform(self, spark):
        data = [('Alice', 20, 'London'), ('Bob', 10, 'Paris'), ('Nina', 40, None)]
        columns = ['name', 'age', 'city']
        df = spark.createDataFrame(data, columns)
        res_df = SaveToTempView('test_view').transform(df)
        assert res_df.collect() == df.collect()
        assert spark.table('test_view').collect() == df.collect()
