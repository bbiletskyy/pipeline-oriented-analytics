from pyspark.sql import DataFrame
from pyspark.ml import Transformer
import pyspark.sql.functions as f
from typing import Any


class DropOutliers(Transformer):
    """Drops records where column values lie outside of the [lower, upper] range."""

    def __init__(self, column: str, lower_bound: Any = None, upper_bound: Any = None):
        super(DropOutliers, self).__init__()
        assert (lower_bound is not None) | (upper_bound is not None)
        if (lower_bound is not None) & (upper_bound is not None):
            assert lower_bound <= upper_bound
        self._column = column
        self._lower_bond = lower_bound
        self._upper_bond = upper_bound

    def _transform(self, dataset: DataFrame) -> DataFrame:
        if self._lower_bond is None:
            res_df = dataset.where(f.col(self._column) > self._lower_bond)
        else:
            res_df = dataset

        if self._upper_bond is not None:
            return res_df.where(f.col(self._column) < self._upper_bond)
