from pyspark.ml import Transformer
from pyspark.sql import DataFrame
from pipeline_oriented_analytics.pipe import Pipe
from pipeline_oriented_analytics.transformer import DropColumns
from pipeline_oriented_analytics.transformer.feature import *
import pyspark.sql.functions as f
from pipeline_oriented_analytics.transformer.feature.time import Time


class RequestCount(Transformer):
    """Adds a new column with the count of request during the timeslot defined by the timestamp and
    the time slot length in minutes which occured in the cell defined by cell_id.
    """
    def __init__(self,
                 slot_size_min: int = 20,
                 cell_id_col: str = 'cell_id',
                 timestamp_col: str = 'timestamp',
                 output_col: str = 'request_count'):
        super(RequestCount, self).__init__()
        self._output_col = output_col
        self._pipe = Pipe([
            Time(timestamp_col, column_features={'_date': Time.Feature.date, '_hour': Time.Feature.hour}),
            SlotOfHour(slot_size_min, timestamp_col, '_slot_of_hour')
        ])
        self._temp_cols = ['_date', '_hour', '_slot_of_hour']
        self._grouping_cols = [cell_id_col] + self._temp_cols

    def _transform(self, dataset: DataFrame) -> DataFrame:
        df = self._pipe.transform(dataset)
        counts_df = df.groupBy(self._grouping_cols).agg(f.count('*').alias(self._output_col))
        joined_df = df.join(counts_df, on=self._grouping_cols, how='left')
        return DropColumns(inputCols=self._temp_cols).transform(joined_df)
