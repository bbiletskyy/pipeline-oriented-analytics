from pyspark.sql import DataFrame
from pyspark.ml import Transformer
from typing import Dict


class RenameColumns(Transformer):
    """Renames columns in a DataFrame using the provided dictionary of old->new names.
    Skips columns, which names were not found among tanble columns.
    """

    def __init__(self, column_names: Dict[str, str]):
        super(RenameColumns, self).__init__()
        self._column_names = column_names

    def _transform(self, dataset: DataFrame):
        df = dataset
        for old_name, new_name in self._column_names.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df
