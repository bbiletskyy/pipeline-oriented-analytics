from pyspark.sql import DataFrame, SparkSession


class CsvDataFrame(DataFrame):
    """DataFrame related to a csv file."""

    def __init__(self, path: str, spark: SparkSession, header: bool = True):
        super(CsvDataFrame, self).__init__(spark.read.option('header', str(header).lower()).csv(path)._jdf, spark)
