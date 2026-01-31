import pytest
import tempfile
import os
import pyarrow as pa

from pyspark.sql import SparkSession
from pyspark_datasources import *
import requests
import time


def _get_access_token(client_id, client_secret):
    """Get OAuth2 access token using client credentials flow"""
    current_time = time.time()

    token_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    response = requests.post(token_url, data=data, timeout=10)
    response.raise_for_status()
    token_data = response.json()
    access_token = token_data["access_token"]
    
    return access_token

if __name__ == "__main__":
    # spark = (
    #     SparkSession.builder
    #     .appName("MyAppName")
    #     .master("local[*]")
    #     .config("spark.driver.bindAddress", "127.0.0.1")
    #     .getOrCreate()
    # )

    # spark.dataSource.register(OpenSkyDataSource)

    # (
    #     spark.readStream.format("opensky")
    #     .option("region", "EUROPE")
    #     .option("client_id", "leanhvi91-api-client")
    #     .option("client_secret", "k9wyxsO9d5BJ6GlL503cVwHBrUDeBLrj")
    #     .load()
    #     .writeStream.format("memory")
    #     .queryName("opensky_result")
    #     .trigger(once=True)
    #     .start()
    #     .awaitTermination()
    # )
    # result = spark.sql("SELECT * FROM opensky_result")
    # result.show()

    # df = (
    #     spark.read.format("opensky")
    #     .option("region", "EUROPE")
    #     .option("client_id", "leanhvi91-api-client")
    #     .option("client_secret", "k9wyxsO9d5BJ6GlL503cVwHBrUDeBLrj")
    #     .load().limit(10)
    #     )
    
    # df.show()

    tkn = _get_access_token("leanhvi91-api-client", "k9wyxsO9d5BJ6GlL503cVwHBrUDeBLrj")
    print(tkn)


