import pytest

from pipeline_oriented_analytics.transformer import Count
import pyspark.sql.functions as f


class TestDropColumns(object):
    def test_drop_columns(self, spark):
        data = [('Alice', 20, 'London'), ('Alice', 33, 'Paris'), ('Alice', 20, 'Paris'), ('Nina', 40, None)]
        columns = ['name', 'age', 'city']
        df = spark.createDataFrame(data, columns)
        res_df = Count(['name', 'age'], 'count').transform(df).sort(f.asc('name'), f.asc('age'))
        rows = res_df.collect()
        assert rows[0]['count'] == 2
        assert rows[1]['count'] == 1
        assert rows[2]['count'] == 1
