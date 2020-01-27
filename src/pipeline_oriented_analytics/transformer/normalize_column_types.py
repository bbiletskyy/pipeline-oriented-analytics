from pyspark.sql import DataFrame
from pyspark.ml import Transformer
import pyspark.sql.functions as f

from typing import Dict


class NormalizeColumnTypes(Transformer):
    """Transformer that changes column types using the provided column_type dictionary.
    Standard Pyspark column types are supported: int, string, float, double, date, timestamp,...
    By default "YYYY-MM-dd HH:mm:ss" date/timestamp format is assumed. Arbitrary format can be used as follows::

        NormalizeColumnTypes({'day': 'date(YYYY-MM-dd)', 'time': 'timestamp(dd/MM/YYYY HH:mm:ss)'})

    Produces null values if the column value can't be cast to the specified type.
    """

    def __init__(self, column_types: Dict[str, str]):
        super(NormalizeColumnTypes, self).__init__()
        self.column_types = column_types

    def _transform(self, dataset: DataFrame):
        def extract_format(s: str) -> str:
            return s[s.find("(") + 1:s.find(")")]

        df = dataset
        for col_name in df.columns:
            if col_name in self.column_types:
                col_type = self.column_types[col_name]
                if col_type.startswith('date('):
                    df = df.withColumn(col_name, f.to_date(f.col(col_name), extract_format(col_type)))
                elif col_type.startswith('timestamp('):
                    df = df.withColumn(col_name, f.to_timestamp(f.col(col_name), extract_format(col_type)))
                else:
                    df = df.withColumn(col_name, df[col_name].cast(col_type))
        return df
