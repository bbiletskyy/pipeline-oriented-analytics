from pyspark import keyword_only
from pyspark.ml.param.shared import HasInputCols
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import DataFrame
from pyspark.ml import Transformer
from typing import List


class DropColumns(Transformer,
                  HasInputCols,
                  DefaultParamsReadable,
                  DefaultParamsWritable
                  ):
    """Transformer that drops specified columns."""

    @keyword_only
    def __init__(self, inputCols: List[str] = None):
        super(DropColumns, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCols: List[str] = None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset: DataFrame):
        return dataset.drop(*self.getInputCols())
