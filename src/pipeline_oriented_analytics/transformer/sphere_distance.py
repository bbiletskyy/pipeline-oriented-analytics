from pyspark.sql import DataFrame
from pyspark.ml import Transformer
import pyspark.sql.functions as f
from pyspark.sql.types import FloatType
import s2sphere


class SphereDistance(Transformer):
    """Add a column with sphere distance in KM between 2 s2sphere cells, given their ids.
    """
    def __init__(self, from_col: str, to_col: str, output_col_name: str = 'distance'):
        super(SphereDistance, self).__init__()
        self._from_col = from_col
        self._to_col = to_col
        self._output_col_name = output_col_name
        self._distance_udf = f.udf(SphereDistance._distance, FloatType())

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset.withColumn(
            self._output_col_name, f.round(self._distance_udf(f.col(self._from_col), f.col(self._to_col)), 2)
        )

    @staticmethod
    def _distance(token_from: str, token_to: str) -> float:
        """
        Calculates distance between two cell centers in KMs based on Earth radius 6373 KM
        :param cell_a: first cell
        :param cell_b: second cell
        :return: distance in KMs
        """
        r = 6373.0
        cell_from = s2sphere.CellId.from_token(token_from)
        cell_to = s2sphere.CellId.from_token(token_to)
        return cell_from.to_lat_lng().get_distance(cell_to.to_lat_lng()).radians * r
