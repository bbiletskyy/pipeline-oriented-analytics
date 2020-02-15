import pytest
from pipeline_oriented_analytics.transformer import CellToken
import pyspark.sql.functions as f
import s2sphere


class TestCellToken(object):
    def test_transform_level_12(self, spark):
        requests_data = [(1, -12.05737, -77.135816), (2, -12.091752, -77.06939), (3, -12.083219, -76.998312)]
        column_names = ['request_id', 'lon', 'lat']
        requests_df = spark.createDataFrame(requests_data, column_names)

        res_df = CellToken(12, 'lat', 'lon', 'cell_id').transform(requests_df)
        assert res_df.count() == requests_df.count()
        assert set(res_df.columns) == set(requests_df.columns).union({'cell_id'})
        assert res_df.where(f.col('request_id') == 1).where(f.col('cell_id') == 'ba64c31').count() == 1
        assert res_df.where(f.col('request_id') == 2).where(f.col('cell_id') == 'ba64dd1').count() == 1
        assert res_df.where(f.col('request_id') == 3).where(f.col('cell_id') == 'ba64de1').count() == 1

    def test_cell_token_udf(self):
        lat, lng = -12.05737, -77.135816
        cell12_id = s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(lat, lng)).parent(12)
        cell11_id = cell12_id.parent(11)

        test_data = list(map(
            lambda cell: (cell.to_lat_lng().lat().degrees, cell.to_lat_lng().lng().degrees),
            cell12_id.parent(11).children(12)
        ))

        for lat, lon in test_data:
            assert CellToken._cell_token(11, lat, lon) == cell11_id.to_token()

    def test_transform_level_11(self, spark):
        lat, lng = -12.05737, -77.135816
        cell12_id = s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(lat, lng)).parent(12)

        cell11_id = cell12_id.parent(11)
        expected_token = cell11_id.to_token()

        test_data = list(map(
            lambda cell: (cell.to_lat_lng().lat().degrees, cell.to_lat_lng().lng().degrees),
            cell12_id.parent(11).children(12)
        ))

        column_names = ['lat', 'lon']
        test_df = spark.createDataFrame(test_data, column_names)

        res_df = CellToken(11, 'lat', 'lon', 'cell_id').transform(test_df)
        assert res_df.where(f.col('cell_id') == expected_token).count() == 4
