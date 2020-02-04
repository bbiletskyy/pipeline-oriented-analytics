import pytest
from pyspark.sql.types import TimestampType
from datetime import datetime
from pipeline_oriented_analytics.transformer.feature import AddMinutes


class TestAddMinutes(object):
    def test_transform(self, spark):
        df = spark.createDataFrame([(datetime(2017, 7, 9, 0, 9, 23))], TimestampType())
        AddMinutes(-20, 'value', '20_mins_before') \
            .transform(df).select('20_mins_before').collect()[0][0] == datetime(2017, 7, 8, 23, 49, 23)
