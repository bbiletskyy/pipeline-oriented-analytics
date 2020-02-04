import pytest
import pyspark.sql.functions as f
from pyspark.sql.types import TimestampType
from datetime import datetime

from pipeline_oriented_analytics.transformer.feature import SlotOfHour


class TestSlotOfHour(object):
    def test_transform_59(self, spark):
        df = spark.createDataFrame([(datetime(2017, 7, 9, 16, 59, 59))], TimestampType())
        assert SlotOfHour(20, 'value').transform(df).select('slot_of_hour').collect()[0][0] == 2

    def test_transform_00(self, spark):
        df = spark.createDataFrame([(datetime(2017, 7, 9, 16, 00, 00))], TimestampType())
        assert SlotOfHour(20, 'value').transform(df).select('slot_of_hour').collect()[0][0] == 0

    def test_transform_multiple(self, spark):
        data = [
            (1, datetime(2017, 7, 9, 12, 0, 0)),
            (1, datetime(2017, 7, 9, 12, 10, 0)),
            (3, datetime(2017, 7, 9, 12, 20, 0)),
            (4, datetime(2017, 7, 9, 12, 40, 0))
        ]
        columns = ['expected_slot_of_hour', 'timestamp']
        df = spark.createDataFrame(data, columns)
        res_df = SlotOfHour(20, 'timestamp', 'slot_of_hour').transform(df)
        res_df.where(f.col('expected_slot_of_hour') == f.col('slot_of_hour')) == res_df.count()

    def test_transform_invalid_slot_size(self, spark):
        df = spark.createDataFrame([(datetime(2017, 7, 9, 16, 45, 59))], TimestampType())
        with pytest.raises(AssertionError):
            SlotOfHour(45, 'value')
