from pyspark.sql import DataFrame
from pyspark.ml import Transformer


class Sample(Transformer):
    """Returns an approximate sample oif data given the replacement mode (True or False),
    the ratio of original data and the seed.
    """

    def __init__(self, ratio: float, with_replacement: bool = False, seed: int = 17):
        super(Sample, self).__init__()
        self._ratio = ratio
        self._with_replacement = with_replacement
        self._seed = seed

    def _transform(self, dataset: DataFrame):
        return dataset.sample(self._with_replacement, self._ratio, self._seed)
