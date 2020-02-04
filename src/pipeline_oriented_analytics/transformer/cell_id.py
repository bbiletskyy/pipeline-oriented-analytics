from pyspark.sql import DataFrame
from pyspark.ml import Transformer
import pyspark.sql.functions as f
from pyspark.sql.types import StringType
import s2sphere


class CellId(Transformer):
    """Adds a new column with S2 cell id, given the s2 cell level, lat and lng column names.
    This transformer uses udf with external library call.
    """
    def __init__(self, level: int, lat_col: str = 'lat', lon_col: str = 'lon', output_col: str = 'cell_id'):
        super(CellId, self).__init__()
        self._level = level
        self._lat_col = lat_col
        self._lon_col = lon_col
        self._cell_id_col = output_col
        self._cell_id_udf = f.udf(CellId._cell_id, StringType())

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset.withColumn(
            self._cell_id_col,
            self._cell_id_udf(f.lit(self._level), f.col(self._lat_col), f.col(self._lon_col))
        )

    @classmethod
    def _cell_id(cls, level: int, lat: int, lon: int) -> str:
        return s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(lat, lon)).parent(level).to_token()
