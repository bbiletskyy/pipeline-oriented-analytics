import pytest

from pipeline_oriented_analytics.transformer.drop_columns import DropColumns


class TestDropColumns(object):
    def test_drop_columns(self, spark):
        data = [('Alice', 20.0, 'London'), ('Bob', 0.0, 'Paris'), ('Nina', 40.0, None)]
        columns = ['name', 'age', 'city']
        df = spark.createDataFrame(data, columns)

        res_df = DropColumns(inputCols=['name', 'city']).transform(df)

        assert df.count() == res_df.count()
        assert res_df.columns == ['age']

