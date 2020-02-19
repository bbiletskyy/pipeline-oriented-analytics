import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.ml import Transformer
from typing import List


class Count(Transformer):
    """Groups rows by given columns and adds group sizes in a new column."""

    def __init__(self, group_by_cols: List[str], output_col: str):
        super(Count, self).__init__()
        self._group_by_cols = group_by_cols
        self._output_col = output_col

    def _transform(self, dataset: DataFrame):
        return dataset.groupBy(self._group_by_cols).agg(f.count('*').alias(self._output_col))
