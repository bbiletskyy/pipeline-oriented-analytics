from enum import Enum
from typing import List, Dict, Callable

from pyspark.sql import DataFrame
from pyspark.ml import Transformer
import pyspark.sql.functions as f


# class TimeFeature(Enum):
#     date = (f.to_date,)
#     year = (f.year,)
#     month = (f.month,)
#     day_of_month = (f.dayofmonth,)
#     day_of_week = (f.dayofweek,)
#     hour = (f.hour,)
#     minute = (f.minute,)
#     second = (f.second,)
#
#     def __init__(self, function: Callable):
#         self.function = function


class Time(Transformer):
    """Adds new column(s) with time feature(s) given a timestamp column to extract necessary features from.
    """

    class Feature(Enum):
        date = (f.to_date,)
        year = (f.year,)
        month = (f.month,)
        day_of_month = (f.dayofmonth,)
        day_of_week = (f.dayofweek,)
        hour = (f.hour,)
        minute = (f.minute,)
        second = (f.second,)

        def __init__(self, function: Callable):
            self.function = function

    def __init__(self, timestamp_col: str, features: List[Feature] = None, column_features: Dict[str, Feature] = None):
        assert (features is not None) | (column_features is not None)
        super(Time, self).__init__()
        if column_features is None:
            self._column_features = dict(zip(list(map(lambda e: e.name, features)), features))
        else:
            self._column_features = column_features
        self._timestamp_col = timestamp_col

    def _transform(self, dataset: DataFrame) -> DataFrame:
        df = dataset
        for col in self._column_features:
            feature = self._column_features[col]
            df = df.withColumn(col, feature.function(f.col(self._timestamp_col)))
        return df


