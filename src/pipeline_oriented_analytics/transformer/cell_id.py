from pyspark.sql import DataFrame
from pyspark.ml import Transformer
import pyspark.sql.functions as f
from pyspark.sql.types import StringType
import s2sphere


class CellId(Transformer):
    """
    Adds a new column with S2 cell id, given the s2 cell level, lat and lng column names.

    NB This transformer is not scaleable (due to the usage of non-scaleable s2sphere library) nor persistable.
    """
    def __init__(self, level: int, lat_col: str = 'lat', lng_col: str = 'lon', cell_id_col_prefix: str = 'cell_id'):
        super(CellId, self).__init__()
        self.level = level
        self.lat_col = lat_col
        self.lng_col = lng_col
        self.cell_id_col = f'{cell_id_col_prefix}_{level}'
        self.cell_id_udf = f.udf(CellId._cell_id, StringType())

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset.withColumn(
            self.cell_id_col,
            self.cell_id_udf(f.lit(self.level), f.col(self.lat_col), f.col(self.lng_col))
        )

    @classmethod
    def _cell_id(cls, level: int, lat: int, lng: int) -> str:
        return s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(lat, lng)).parent(level).to_token()
