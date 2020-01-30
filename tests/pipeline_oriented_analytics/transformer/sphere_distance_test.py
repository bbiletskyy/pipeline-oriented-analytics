import pytest
from pipeline_oriented_analytics.transformer import SphereDistance
import pyspark.sql.functions as f


class TestSphereDistance(object):
    def test_transform(self, spark):
        requests_data = [(1,'9105c87', '9105c87'), (2, '9105c87', '9105c85'), (3, '9105c85', '9105c87')]
        column_names = ['id', 'pickup_cell', 'dropoff_cell']
        requests_df = spark.createDataFrame(requests_data, column_names)

        res_df = SphereDistance('pickup_cell', 'dropoff_cell', 'distance').transform(requests_df)
        rows = res_df.sort(f.col('id')).collect()
        assert rows[0].asDict()['distance'] == 0
        assert rows[1].asDict()['distance'] == pytest.approx(2.5, 0.1)
        assert rows[2].asDict()['distance'] == pytest.approx(2.5, 0.1)
