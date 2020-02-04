from pyspark.sql import DataFrame
from pyspark.ml import Transformer, PipelineModel
from typing import List

from pipeline_oriented_analytics.pipe.pipe import Pipe


class IF(Transformer):
    """Conditional pipeline which runs one or another list of transformers based on condition"""

    def __init__(self, condition: bool, then: List[Transformer], otherwise: List[Transformer] = None):
        super(IF, self).__init__()
        self._then = Pipe(then)
        if otherwise is None:
            self._otherwise = Pipe([])
        else:
            self._otherwise = Pipe(otherwise)

        if condition:
            self._pipe = self._then
        else:
            self._pipe = self._otherwise

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return self._pipe.transform(dataset)
