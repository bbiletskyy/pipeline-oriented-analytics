from pyspark.sql import DataFrame
from pyspark.ml import Transformer
from typing import List


class SelectColumns(Transformer):
    """Transformer that selects from a DataFrame only the specified columns.
    """

    def __init__(self, columns: List[str]):
        super(SelectColumns, self).__init__()
        self.columns = columns

    def _transform(self, dataset: DataFrame):
        return dataset.select(self.columns)
