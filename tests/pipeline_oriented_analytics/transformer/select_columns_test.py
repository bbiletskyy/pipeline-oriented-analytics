import pytest
from pipeline_oriented_analytics.transformer import SelectColumns


class TestSelectColumns(object):
    def test_rename_columns(self, spark):
        data = [('Alice', 20.0, 'London'), ('Bob', 0.0, 'Paris'), ('Nina', 40.0, None)]
        columns = ['driver', 'age', 'city']
        df = spark.createDataFrame(data, columns)

        column_names = ['driver', 'city']

        res_df = SelectColumns(column_names).transform(df)

        assert df.count() == res_df.count()
        assert set(res_df.columns) == set(column_names)
