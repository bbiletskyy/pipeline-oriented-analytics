import pytest
import pyspark.sql.functions as f
from pipeline_oriented_analytics.dataframe import ParquetDataFrame
from pipeline_oriented_analytics.transformer import SaveToParquet


class TestSaveToParquet(object):
    def test_transform(self, spark, temp_dir):
        requests_data = [(1,'9105c87', '9105c87'), (2, '9105c87', '9105c85'), (3, '9105c85', '9105c87')]
        column_names = ['id', 'pickup_cell', 'dropoff_cell']
        requests_df = spark.createDataFrame(requests_data, column_names)
        path = temp_dir+'/requests'
        res_df = SaveToParquet(path).transform(requests_df)

        assert ParquetDataFrame(path, spark).sort(f.col('id')).collect() == res_df.sort(f.col('id')).collect()
