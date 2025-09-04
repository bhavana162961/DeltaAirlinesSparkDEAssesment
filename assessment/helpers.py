from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Get the spark session

    This function assumes you are using a locally running spark connect server.
    If you choose to use a different spark configuration, feel free to modify
    the function.

    Returns
    -------
    SparkSession
        An active spark session
    """
    # My spark connect server is running on localhost:15002. Incase you are using a different host or port,
    # please modify the below line accordingly.
    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
    print(spark.version)

    return spark
