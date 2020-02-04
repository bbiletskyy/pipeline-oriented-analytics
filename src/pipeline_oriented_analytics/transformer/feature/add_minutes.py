from pyspark.sql import DataFrame
from pyspark.ml import Transformer
import pyspark.sql.functions as F


class AddMinutes(Transformer):
    """Adds a new column with the new timestamp obtained by adding the specified number of minutes
    to the specified timestamp column.

    """
    def __init__(self, mins: int, timestamp_col: str = 'timestamp', output_col: str = 'output_timestamp'):
        super(AddMinutes, self).__init__()
        self._mins = mins
        self._timestamp_col = timestamp_col
        self._output_col = output_col

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset\
            .withColumn(self._output_col, (F.unix_timestamp(self._timestamp_col) + self._mins * 60).cast('timestamp'))
