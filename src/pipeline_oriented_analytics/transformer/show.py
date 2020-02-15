from pyspark.sql import DataFrame
from pyspark.ml import Transformer


class Show(Transformer):
    """Prints the dataframe."""

    def __init__(self, rows: int, truncate=False):
        super(Show, self).__init__()
        self._rows = rows
        self._truncate = truncate

    def _transform(self, dataset: DataFrame):
        dataset.show(self._rows(), self._truncate)
        return dataset
