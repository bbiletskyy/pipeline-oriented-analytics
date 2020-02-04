import pytest
from pipeline_oriented_analytics.transformer import Join


class TestJoin(object):
    def test_enrich(self, spark):
        trip_duration_data = [(1, 234), (2, 76), (1, 1400)]
        trip_duration_cols = ['trip_id', 'trip_duration']
        trip_duration_df = spark.createDataFrame(trip_duration_data, trip_duration_cols)

        trip_city_data = [(1, 'Madrid'), (2, 'Amsterdam')]
        trip_city_cols = ['trip_id', 'city']
        trip_city_df = spark.createDataFrame(trip_city_data, trip_city_cols)

        res_lst = Join(['trip_id'], Join.Method.inner, trip_city_df).transform(trip_duration_df).sort('trip_id').collect()

        assert len(res_lst) == trip_duration_df.count() == 3
        trip_1 = res_lst[0].asDict()
        assert (trip_1['trip_id'] == 1) & (trip_1['city'] == 'Madrid')
        trip_2 = res_lst[1].asDict()
        assert (trip_2['trip_id'] == 1) & (trip_2['city'] == 'Madrid')
        trip_3 = res_lst[2].asDict()
        assert (trip_3['trip_id'] == 2) & (trip_3['city'] == 'Amsterdam')

    def test_left_anti_join(self, spark):
        trip_duration_data = [(1, 234), (2, 76), (1, 1400)]
        trip_duration_cols = ['trip_id', 'trip_duration']
        trip_duration_df = spark.createDataFrame(trip_duration_data, trip_duration_cols)

        trip_city_data = [(1, 'Madrid'), (2, 'Amsterdam'), (3, 'Alkmaar')]
        trip_city_cols = ['trip_id', 'city']
        trip_city_df = spark.createDataFrame(trip_city_data, trip_city_cols)

        res_lst = Join(['trip_id'], Join.Method.left_anti, trip_duration_df)\
            .transform(trip_city_df).sort('trip_id').collect()
        assert len(res_lst) == 1
        trip_1 = res_lst[0].asDict()
        assert (trip_1['trip_id'] == 3) & (trip_1['city'] == 'Alkmaar')
