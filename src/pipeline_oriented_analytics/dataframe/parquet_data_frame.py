from pyspark.sql import DataFrame, SparkSession


class ParquetDataFrame(DataFrame):
    """DataFrame related to partquet files."""

    def __init__(self, path: str, spark: SparkSession):
        super(ParquetDataFrame, self).__init__(spark.read.parquet(path)._jdf, spark)
