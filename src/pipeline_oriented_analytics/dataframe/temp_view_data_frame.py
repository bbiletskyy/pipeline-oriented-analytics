from pyspark.sql import DataFrame, SparkSession


class TempViewDataFrame(DataFrame):
    """Dataframe associated with a temp view registered in the spark sql session."""

    def __init__(self, name: str, spark: SparkSession):
        super(TempViewDataFrame, self).__init__(None, spark)
        self.__name = name

    def __getattribute__(self, item):
        if (item == '_jdf') & (super().__getattribute__(item) is None):
            self.__dict__[item] =  super().__getattribute__('sql_ctx').table(self.__name)._jdf
        return super().__getattribute__(item)
