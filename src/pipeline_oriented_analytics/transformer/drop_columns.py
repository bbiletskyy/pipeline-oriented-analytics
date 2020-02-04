from pyspark.sql import DataFrame
from pyspark.ml import Transformer
from typing import List


class DropColumns(Transformer):
    """Drops listed columns."""

    def __init__(self, cols: List[str]):
        super(DropColumns, self).__init__()
        self.cols = cols

    def _transform(self, dataset: DataFrame):
        return dataset.drop(*self.cols)
