from delta.tables import DeltaTable
from loguru import logger
from phynfra.aws.storage import check_s3_folder
from phynfra.aws.storage import get_boto3_session
from phynfra.commons.utils import get_bucket_name_from_zone_path
from phynfra.commons.utils import get_folder_from_zone_path
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType
from pyspark.sql.utils import AnalysisException


class DeltaTableMethods:
    def read_spark_dataframe(self, spark: SparkSession, s3_path: str, table: str, **kwargs) -> DataFrame:
        """
        Read spark dataframe from s3 bucket

        Args:
            spark: Spark session
            s3_path: AWS s3 path where the table will be save
            kwargs: Optional arguments for dataframe
        """
        logger.info(f"Reading file from {s3_path}{table}")

        try:
            df = spark.read.load(f"{s3_path}{table}", **kwargs)
            return df
        except AnalysisException as e:
            logger.error(f"Error trying to read dataframe from {s3_path}{table}")
            logger.error(e)
            raise e

    def write_delta_table(self, df: DataFrame, s3_path: str, table: str, write_mode: str = "append", **options):
        """
        Write delta table in the specified s3 bucket

        Args:
            df: Spark Dataframe
            s3_path: AWS s3 path where the table will be save
            table: The name of the table
            write_mode: The write mode (append, overwrite)
            options: Optional arguments for dataframe
        """
        try:
            writer = df.write.options(**options).format("delta").mode(write_mode)

            logger.info(f"Writing Delta Table {table} in bucket: {s3_path}")
            writer.save(f"{s3_path}{table}")

        except Exception as e:
            logger.error(f"Error trying to write Delta Table {table} in bucket: {s3_path}")
            logger.error(e)
            raise e

    def write_partitioned_delta_table(
        self,
        df: DataFrame,
        s3_path: str,
        table: str,
        partition_columns: list[str],
        write_mode: str = "append",
    ):
        """
        Write partitioned delta table in the specified s3 bucket

        Args:
            df: Spark Dataframe
            s3_path: AWS s3 path where the table will be save
            table: The name of the table
            partition_columns: List of partition columns
            write_mode: The write mode (append, overwrite)
        """
        try:
            writer = (
                df.write.option("encoding", "UTF-8")
                .option("header", "true")
                .option("mergeSchema", "true")
                .partitionBy(*partition_columns)
                .format("delta")
                .mode(write_mode)
            )

            logger.info(f"Writing Delta Table {table} in bucket: {s3_path} partitioned by {partition_columns}")
            writer.save(f"{s3_path}{table}")

        except Exception as e:
            logger.error(f"Error trying to write Delta Table {table} in bucket: {s3_path}")
            logger.error(e)
            raise e

    def upsert_data(self, spark: SparkSession, df_new: DataFrame, pk_list: list, bucket: str, table: str):
        """
        Update and Insert data in the specified delta table

        Args:
            spark: Spark session
            df_new: New spark dataframe
            pk_list: List of primary keys
            bucket: Destination bucket to save json file
            table: The name of the table
        """
        try:
            table = DeltaTable.forPath(spark, bucket + table)

            merge_string_list = [f"destination.{pk} = updates.{pk}" for pk in pk_list]
            merge_string = " AND ".join(merge_string_list)

            (
                table.alias("destination")
                .merge(df_new.alias("updates"), merge_string)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        except AnalysisException as e:
            logger.debug("Not able to merge because this is the first write made to the Delta Table")
            logger.debug(e)

    def delete_data(self, spark: SparkSession, df_new: DataFrame, pk_list: list, s3_path: str, table: str):
        """
        Delete data in the specified delta table

        Args:
            spark: Spark session
            df_new: New spark dataframe
            pk_list: List of primary keys
            s3_path: AWS s3 path where the table will be delete
            table: The name of the table
        """

        spark_read_options = {
            "format": "delta",
        }
        df_base = self.read_spark_dataframe(spark=spark, s3_path=s3_path, table=table, **spark_read_options)
        forms_id = df_new.select(pk_list[0]).distinct().rdd.map(lambda x: x[0]).collect()
        df_base = df_base.filter((F.col(pk_list[0]).isin(forms_id)) & (F.col("deleted_at").isNotNull()))

        df_deleted = df_base.select(*pk_list).exceptAll(df_new.select(*pk_list))
        df_deleted = df_deleted.withColumn("deleted_at", F.current_timestamp())

        if df_deleted.count() > 0:
            print(df_deleted.show())
            table = DeltaTable.forPath(spark, f"{s3_path}{table}")

            merge_string_list = [f"destination.{pk} = updates.{pk}" for pk in pk_list]
            merge_string = " AND ".join(merge_string_list)
            logger.info(f"merge_string: {merge_string}")

            (
                table.alias("destination")
                .merge(df_deleted.alias("updates"), merge_string)
                .whenMatchedUpdate(set={"deleted_at": "updates.deleted_at"})
                .execute()
            )
            logger.info("Data was updated with the status False")
        else:
            logger.info("No data to delete")

    def create_views(self, spark: SparkSession, list_temp_views: list):
        """

        Args:
            spark:
            list_temp_views:
        """
        spark_read_options = {
            "format": "delta",
        }
        for item in list_temp_views:
            logger.info(item)
            logger.info(f"Criando a temp da tabela {item['name']}")
            logger.info(f"Bucket {item['bucket']}")
            temp_table = self.read_spark_dataframe(
                spark=spark, s3_path=item["bucket"], table=item["table"], **spark_read_options
            )
            temp_table.createOrReplaceTempView(item["name"])

    def update_data(self, spark: SparkSession, df_new: DataFrame, pk_list: list, bucket: str, table: str):
        """

        Args:
            spark:
            df_new:
            pk_list:
            bucket:
            table:
        """
        try:
            table = DeltaTable.forPath(spark, bucket + table)
            merge_string_list = [f"destination.{pk} = updates.{pk}" for pk in pk_list]
            merge_string = " AND ".join(merge_string_list)

            update_columns_dict = {}
            for column in df_new.columns:
                if column not in pk_list:
                    update_columns_dict[column] = f"updates.{column}"

            (
                table.alias("destination")
                .merge(df_new.alias("updates"), merge_string)
                .whenMatchedUpdate(set=update_columns_dict)
                .execute()
            )

        except AnalysisException as e:
            logger.error("Not able to merge Delta Table")
            logger.error(e)
            raise e

    def upsert_data_with_delete_track(
        self, spark: SparkSession, df: DataFrame, primary_keys: list, bucket: str, table: str
    ):
        """

        Args:
            spark:
            df:
            primary_keys:
            bucket:
            table:
        """
        try:
            table = DeltaTable.forPath(spark, bucket + table)
            merge_string_list = [f"destination.{pk} = updates.{pk}" for pk in primary_keys]
            merge_string = " AND ".join(merge_string_list)

            (
                table.alias("destination")
                .merge(df.alias("updates"), merge_string)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .whenNotMatchedBySourceUpdate(
                    condition="destination.deleted_at is null",
                    set={"destination.deleted_at": F.current_timestamp()},
                )
                .execute()
            )

        except AnalysisException as e:
            logger.error("Not able to merge Delta Table")
            logger.error(e)
            raise e

    def deduplicate_dataframe(self, df: DataFrame, primary_keys: list[str], ts_column: str) -> DataFrame:
        """
        Remove data that has the same pk, keeping the most recent record

        Args:
            df:
            primary_keys:
            ts_column:
        """
        try:
            window = Window.partitionBy(primary_keys).orderBy(F.col(ts_column).desc(), F.col("tiebreak"))
            df_unique = (
                df.withColumn("tiebreak", F.monotonically_increasing_id())
                .withColumn("rank", F.rank().over(window))
                .filter(F.col("rank") == 1)
                .drop("rank", "tiebreak")
            )
        except Exception as e:
            logger.error("Error trying to deduplicate dataframe")
            logger.error(e)
            raise e

        return df_unique

    def first_write_to_delta_table(self, zone_path: str, table_name: str):
        """

        Args:
            zone_path:
            table_name:
        """
        s3_client = get_boto3_session().client("s3")

        bucket_name = get_bucket_name_from_zone_path(zone_path)
        folder_name = get_folder_from_zone_path(zone_path)

        return not (check_s3_folder(s3_client, bucket_name, folder_name + f"{table_name}/"))

    def get_df_latest_batch(self, df: DataFrame, date_col: str):
        """

        Args:
            df:
            date_col:
        """
        logger.info("Getting latest batch of dataframe")
        try:
            max_date = df.select(F.max(F.col(date_col))).rdd.map(lambda x: x[0]).collect()[0]
            df_latest_batch = df.where(F.col(date_col) >= max_date)

            return df_latest_batch
        except Exception as e:
            logger.error("Error trying to get latest batch of dataframe")
            logger.error(e)
            raise e

    def flatten(self, df: DataFrame) -> DataFrame:
        """
        Flatten json files dynamically.

        References:
        # https://gist.github.com/nmukerje/e65cde41be85470e4b8dfd9a2d6aed50

        Args:
            df:
        """
        try:
            logger.info("Flattening Spark DataFrame")
            # compute Complex Fields (Lists and Structs) in Schema
            complex_fields = dict(
                [
                    (field.name, field.dataType)
                    for field in df.schema.fields
                    if type(field.dataType) == ArrayType or type(field.dataType) == StructType
                ]
            )
            while len(complex_fields) != 0:
                col_name = list(complex_fields.keys())[0]

                # if StructType then convert all sub element to columns.
                # i.e. flatten structs
                if type(complex_fields[col_name]) == StructType:
                    expanded = [
                        F.col(col_name + "." + k).alias(col_name + "_" + k)
                        for k in [n.name for n in complex_fields[col_name]]
                    ]

                    df = df.select("*", *expanded).drop(col_name)

                # if ArrayType then add the Array Elements as Rows using the explode function
                # i.e. explode Arrays
                elif type(complex_fields[col_name]) == ArrayType:
                    df = df.withColumn(col_name, F.explode_outer(col_name))

                # recompute remaining Complex Fields in Schema
                complex_fields = dict(
                    [
                        (field.name, field.dataType)
                        for field in df.schema.fields
                        if type(field.dataType) == ArrayType or type(field.dataType) == StructType
                    ]
                )

            return df
        except Exception as e:
            logger.error("Error trying to flatten Spark DataFrame")
            logger.error(e)
            raise e

    def get_date_partition_columns(self, df: DataFrame, base_column: str):
        """

        Args:
            df:
            base_column:
        """
        new_df = df.withColumn("year", F.date_format(F.col(base_column), "yyyy")).withColumn(
            "month", F.date_format(F.col(base_column), "MM")
        )

        return new_df, ["year", "month"]

    def get_spark_equivalent_type(self, arrow_type: str):
        """

        Args:
            arrow_type:
        """
        arrow_spark_types_dict = {
            "timestamp[pyarrow]": TimestampType(),
            "int64[pyarrow]": LongType(),
            "int32[pyarrow]": IntegerType(),
            "float64[pyarrow]": DoubleType(),
            "float32[pyarrow]": FloatType(),
        }

        return arrow_spark_types_dict.get(arrow_type, StringType())

    def replace_caracter_in_df_columns(self, df: DataFrame, old_caracter: str, new_caracter: str) -> DataFrame:
        """

        Args:
            df:
            old_caracter:
            new_caracter:
        """
        new_columns = [col.replace(old_caracter, new_caracter) for col in df.columns]

        return df.toDF(*new_columns)
