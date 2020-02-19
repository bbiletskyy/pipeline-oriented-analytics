import pytest
import pyspark.sql.functions as f
from pipeline_oriented_analytics.transformer import SaveToTempView, DropTempView


class TestDropTempView(object):
    def test_transform(self, spark):
        assert True == True
        data = [('Alice', 20, 'London'), ('Bob', 10, 'Paris'), ('Nina', 40, None)]
        columns = ['name', 'age', 'city']
        df = spark.createDataFrame(data, columns)
        assert df.sql_ctx.tables().where(f.col('tableName') == 'test_view').count() == 0
        SaveToTempView('test_view').transform(df)
        assert df.sql_ctx.tables().where(f.col('tableName') == 'test_view').count() == 1
        DropTempView('test_view').transform(df)
        assert df.sql_ctx.tables().where(f.col('tableName') == 'test_view').count() == 0
