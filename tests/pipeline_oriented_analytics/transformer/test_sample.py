import pytest
from pyspark.sql import Row
from pipeline_oriented_analytics.transformer import Sample


class TestSample(object):
    def test_transform(self, spark):
        data = list(range(100))
        df = spark.createDataFrame(list(map(lambda x: Row(value=x), data)))
        res_df = Sample(0.5, False, 19).transform(df)
        assert res_df.count() == pytest.approx(50, 0.2)





