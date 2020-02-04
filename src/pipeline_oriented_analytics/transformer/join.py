from enum import Enum, auto

from pyspark.sql import DataFrame
from pyspark.ml import Transformer
from typing import List


class Join(Transformer):
    """Joins the dataframe being transformed with the dataframe provided in constructor, given join columns and join type.
    Supported join methods: ``inner``, ``cross``, ``outer``, ``full``, ``full_outer``, ``left``, ``left_outer``,
    ``right``, ``right_outer``, ``left_semi`` and ``left_anti``.
    """
    class Method(Enum):
        inner = auto()
        cross = auto()
        outer = auto()
        full = auto()
        full_outer = auto()
        left = auto()
        left_outer = auto()
        right = auto()
        right_outer = auto()
        left_semi = auto()
        left_anti = auto()

    def __init__(self, on_cols: List[str], how: Method, with_df: DataFrame):
        super(Join, self).__init__()
        self._with_df = with_df
        self._on_cols = on_cols
        self._how = how

    def _transform(self, dataset: DataFrame):
        return dataset.join(self._with_df, on=self._on_cols, how=self._how.name)
