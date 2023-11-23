from pyspark.sql import SparkSession


class SparkInstance:
    def __init__(self, app_name: str) -> None:
        """
        Spark session
        Args:
            app_name: The name of the session
        """
        self.app_name = app_name

    def get_spark(self) -> SparkSession:
        """
        Create a spark session
        """
        spark_session_builder = (
            SparkSession.builder.appName(self.app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.delta.schema.autoMerge.enabled", "true")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        )

        spark_session = spark_session_builder.getOrCreate()
        spark_session.sparkContext.setLogLevel("WARN")

        return spark_session
