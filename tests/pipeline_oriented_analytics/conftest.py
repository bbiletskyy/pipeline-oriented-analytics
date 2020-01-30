import pytest
from pyspark.sql import SparkSession
import tempfile
import shutil


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope='session')
def temp_dir():
    temp_dir = tempfile.mkdtemp(prefix='pipeline-oriented-analytics-tests')
    yield temp_dir
    shutil.rmtree(temp_dir)

