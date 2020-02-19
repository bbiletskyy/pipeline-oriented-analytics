from pyspark.ml import Transformer
from pyspark.sql import DataFrame

from pipeline_oriented_analytics.dataframe import TempViewDataFrame
from pipeline_oriented_analytics.pipe import Pipe
from pipeline_oriented_analytics.transformer import DropColumns, SaveToTempView, Count, Join, DropTempView
from pipeline_oriented_analytics.transformer.feature import Time, SlotOfHour


class RequestCount(Transformer):
    """Adds a new column with the count of requests in a cell during the time slot defined by the timestamp and
    the time slot length in minutes."""
    def __init__(self,
                 slot_size_min: int = 20,
                 cell_token_col: str = 'cell_token',
                 timestamp_col: str = 'timestamp',
                 output_col: str = 'request_count'):
        super(RequestCount, self).__init__()
        self._output_col = output_col
        self._timestamp_col = timestamp_col
        self._slot_size_min = slot_size_min
        self._cell_token_col = cell_token_col

    def _transform(self, dataset: DataFrame) -> DataFrame:
        temp_cols = ['_date', '_hour', '_slot_of_hour']
        grouping_cols = [self._cell_token_col] + temp_cols
        view_name = f'{self.uid}_temp_view'
        return Pipe([
            Time(self._timestamp_col, column_features={'_date': Time.Feature.date,
                                                       '_hour': Time.Feature.hour}),
            SlotOfHour(self._slot_size_min, self._timestamp_col, '_slot_of_hour'),
            SaveToTempView(view_name),
            Count(grouping_cols, self._output_col),
            Join(grouping_cols, Join.Method.right, TempViewDataFrame(view_name, dataset.sql_ctx)),
            DropColumns(inputCols=temp_cols),
            DropTempView(view_name)
        ]).transform(dataset)
