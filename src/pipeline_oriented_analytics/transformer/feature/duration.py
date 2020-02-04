from enum import Enum

from pyspark.sql import DataFrame
from pyspark.ml import Transformer
import pyspark.sql.functions as f


class Duration(Transformer):
    """Given a column with duration in seconds converts it to duration in minutes or hours in a new column.
    """

    class Unit(Enum):
        minute = 60
        hour = 3600

    def __init__(self, unit: Unit, duration_seconds_col: str, output_col: str = None):
        super(Duration, self).__init__()
        self._unit = unit
        self._duration_seconds_col = duration_seconds_col
        if output_col is None:
            self._output_col = unit.name
        else:
            self._output_col = output_col

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset.withColumn(self._output_col, f.round(f.col(self._duration_seconds_col) / self._unit.value, 1))
