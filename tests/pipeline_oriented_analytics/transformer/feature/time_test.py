import pytest
from pyspark.sql.types import TimestampType
from datetime import datetime, date
import pyspark.sql.functions as f
from pipeline_oriented_analytics.transformer.feature.time import Time #, TimeFeature


class TestTime(object):
    def test_transform_features(self, spark):
        d = datetime(2017, 7, 9, 16, 45, 59)
        df = spark.createDataFrame([(d)], TimestampType())
        res = Time('value', [Time.Feature.date, Time.Feature.second]).transform(df)
        assert res.collect()[0]['date'] == date(2017, 7, 9)
        assert res.collect()[0]['second'] == 59

    def test_transform_col_features(self, spark):
        d = datetime(2017, 7, 9, 16, 45, 59)
        df = spark.createDataFrame([(d)], TimestampType())
        res = Time('value', column_features={'d': Time.Feature.date, 'sec': Time.Feature.second}).transform(df)
        assert res.collect()[0]['sec'] == 59
        assert res.collect()[0]['d'] == date(2017, 7, 9)

    def test_functions(self, spark):
        d = datetime(2017, 7, 9, 16, 45, 59)
        df = spark.createDataFrame([(d)], TimestampType())
        assert Time('value', [Time.Feature.date]).transform(df).collect()[0]['date'] == date(2017, 7, 9)
        assert Time('value', [Time.Feature.year]).transform(df).collect()[0]['year'] == 2017
        assert Time('value', [Time.Feature.month]).transform(df).collect()[0]['month'] == 7
        assert Time('value', [Time.Feature.day_of_month]).transform(df).collect()[0]['day_of_month'] == 9
        assert Time('value', [Time.Feature.day_of_week]).transform(df).collect()[0]['day_of_week'] == 1
        assert Time('value', [Time.Feature.hour]).transform(df).collect()[0]['hour'] == 16
        assert Time('value', [Time.Feature.minute]).transform(df).collect()[0]['minute'] == 45
        assert Time('value', [Time.Feature.second]).transform(df).collect()[0]['second'] == 59
