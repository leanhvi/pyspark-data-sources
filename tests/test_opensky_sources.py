import pytest
import tempfile
import os
import pyarrow as pa

from pyspark.sql import SparkSession
from pyspark_datasources import *


@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark

    #  "client_id": "leanhvi91-api-client"
    #  "client_secret": "k9wyxsO9d5BJ6GlL503cVwHBrUDeBLrj"

def test_opensky_datasource_stream(spark):
    spark.dataSource.register(OpenSkyDataSource)
    (
        spark.readStream.format("opensky")
        .option("region", "EUROPE")
        .option("client_id", "leanhvi91-api-client")
        .option("client_secret", "k9wyxsO9d5BJ6GlL503cVwHBrUDeBLrj")
        .load()
        .writeStream.format("memory")
        .queryName("opensky_result")
        .trigger(once=True)
        .start()
        .awaitTermination()
    )
    result = spark.sql("SELECT * FROM opensky_result")
    result.show()
    assert len(result.columns) == 18  # Check schema has expected number of fields 
    assert result.count() > 0  # Verify we got some data
