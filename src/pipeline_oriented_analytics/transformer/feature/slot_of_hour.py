from pyspark.sql import DataFrame
from pyspark.ml import Transformer
import pyspark.sql.functions as f


class SlotOfHour(Transformer):
    """Adds a new column with the number of the slot in the hour given the slot size in minutes and
    the name of the timestamp column.
    Slot lenght in minutes should be > 0 and <= 30, otherwise AssertionError is raised.
    For example: if slot_size_mins=20 mins, then 12:00 is slot 0, and 12:59 is slot 2.
    """
    def __init__(self, slot_size_mins: int, timestamp_col: str = 'timestamp', output_col: str = 'slot_of_hour'):
        assert (slot_size_mins > 0) & (slot_size_mins <= 30)
        super(SlotOfHour, self).__init__()
        self._slot_size_mins = slot_size_mins
        self._timestamp_col = timestamp_col
        self._output_col = output_col

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset.withColumn(self._output_col, f.floor(f.minute(self._timestamp_col) / self._slot_size_mins))
