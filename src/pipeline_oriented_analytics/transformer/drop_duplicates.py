from pyspark.sql import DataFrame
from pyspark.ml import Transformer
from typing import List


class DropDuplicates(Transformer):
    """Drops duplicate records"""

    def __init__(self, cols: List[str] = None):
        super(DropDuplicates, self).__init__()
        self._cols = cols

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset.dropDuplicates(self._cols)
