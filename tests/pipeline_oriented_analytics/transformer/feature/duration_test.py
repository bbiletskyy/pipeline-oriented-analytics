import pytest
from pyspark.sql.types import IntegerType
from pipeline_oriented_analytics.transformer.feature import Duration


class TestDuration(object):
    def test_transform_minutes(self, spark):
        df = spark.createDataFrame([(66)], IntegerType())
        assert Duration(Duration.Unit.minute, 'value', 'min').transform(df).collect()[0]['min'] == pytest.approx(1, 0.1)

    def test_transform_hours(self, spark):
        df = spark.createDataFrame([(2 * 3600 + 1800)], IntegerType())
        assert Duration(Duration.Unit.hour, 'value').transform(df).collect()[0]['hour'] == pytest.approx(3, 0.1)
