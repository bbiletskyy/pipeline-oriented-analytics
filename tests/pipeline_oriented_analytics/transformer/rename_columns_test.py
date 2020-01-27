import pytest
from pipeline_oriented_analytics.transformer import RenameColumns


class TestRenameColumns(object):
    def test_rename_columns(self, spark):
        data = [('Alice', 20.0, 'London'), ('Bob', 0.0, 'Paris'), ('Nina', 40.0, None)]
        columns = ['driver', 'age', 'city']
        df = spark.createDataFrame(data, columns)

        column_names = {'driver': 'driver1', 'missing_column': 'missing_column1', 'city': 'city'}

        res_df = RenameColumns(column_names).transform(df)

        assert df.count() == res_df.count()
        assert 'driver1' in res_df.columns
        assert 'city' in res_df.columns
