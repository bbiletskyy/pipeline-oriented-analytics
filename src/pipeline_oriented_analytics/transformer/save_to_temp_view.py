from pyspark.ml import Transformer
from pyspark.sql import DataFrame


class SaveToTempView(Transformer):
    """Saves the dataframe as a temp view in spark session."""

    def __init__(self, name: str):
        super(SaveToTempView, self).__init__()
        self._name = name

    def _transform(self, dataset: DataFrame) -> DataFrame:
        dataset.createOrReplaceTempView(self._name)
        return dataset
