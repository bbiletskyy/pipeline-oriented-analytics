from pyspark.sql import DataFrame
from pyspark.ml import Transformer
from typing import List


class Join(Transformer):
    """Enriches transformable dataframe with specified data.
    Upported join methods: ``inner``, ``cross``, ``outer``, ``full``, ``full_outer``, ``left``, ``left_outer``,
    ``right``, ``right_outer``, ``left_semi`` and ``left_anti``.
    """

    def __init__(self, on_cols: List[str], how: str, with_df: DataFrame):
        super(Join, self).__init__()
        self.with_df = with_df
        self.on_cols = on_cols
        self.how = how

    def _transform(self, dataset: DataFrame):
        return self.with_df.join(dataset, on=self.on_cols, how=self.how)
