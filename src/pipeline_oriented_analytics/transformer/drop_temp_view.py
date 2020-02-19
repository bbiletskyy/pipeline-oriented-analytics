from pyspark.ml import Transformer
from pyspark.sql import DataFrame


class DropTempView(Transformer):
    """Drops a temp view, given the name of the view."""

    def __init__(self, name: str):
        super(DropTempView, self).__init__()
        self._name = name

    def _transform(self, dataset: DataFrame) -> DataFrame:
        dataset.sql_ctx.sparkSession.catalog.dropTempView(self._name)
        return dataset
