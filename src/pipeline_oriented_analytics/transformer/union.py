from pyspark.sql import DataFrame
from pyspark.ml import Transformer


class Union(Transformer):
    """Appends rows of the dataframe provided in the constructor to the rows of the dataframe being transformed.
    This operation is possible if both tables have equal schemas.
    """

    def __init__(self, with_df: DataFrame):
        super(Union, self).__init__()
        self.with_df = with_df

    def _transform(self, dataset: DataFrame):
        return dataset.union(self.with_df)
