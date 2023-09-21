from delta.tables import DeltaTable
from loguru import logger
from pandas import DataFrame as PandasDataFrame
from phynfra.aws.storage import AWSBucket
from phynfra.commons.utils import convert_pandas_df_to_list
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
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType
from pyspark.sql.utils import AnalysisException


class DeltaTableMethods:
    def read_spark_dataframe(self, spark: SparkSession, bucket: str, table: str, **kwargs) -> DataFrame:
        """
        Read spark dataframe from s3 bucket

        Args:
            spark: Spark session
            bucket: AWS s3 path where the table will be save
            kwargs: Optional arguments for dataframe
        """
        logger.info(f"Reading file from {bucket}{table}")

        try:
            df = spark.read.load(f"{bucket}{table}", **kwargs)
            return df
        except AnalysisException as e:
            logger.error(f"Error trying to read dataframe from {bucket}{table}")
            logger.error(e)
            raise e

    def write_delta_table(self, df: DataFrame, bucket: str, table: str, write_mode: str = "append", **options):
        """
        Write delta table in the specified s3 bucket

        Args:
            df: Spark Dataframe
            bucket: AWS s3 path where the table will be save
            table: The name of the table
            write_mode: The write mode (append, overwrite)
            options: Optional arguments for dataframe
        """
        try:
            writer = df.write.options(**options).format("delta").mode(write_mode)

            logger.info(f"Writing Delta Table {table} in bucket: {bucket}")
            writer.save(f"{bucket}{table}")

        except Exception as e:
            logger.error(f"Error trying to write Delta Table {table} in bucket: {bucket}")
            logger.error(e)
            raise e

    def write_partitioned_delta_table(
        self,
        df: DataFrame,
        bucket: str,
        table: str,
        partition_columns: list[str],
        write_mode: str = "append",
    ):
        """
        Write partitioned delta table in the specified s3 bucket

        Args:
            df: Spark Dataframe
            bucket: AWS s3 path where the table will be save
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

            logger.info(f"Writing Delta Table {table} in bucket: {bucket} partitioned by {partition_columns}")
            writer.save(f"{bucket}{table}")

        except Exception as e:
            logger.error(f"Error trying to write Delta Table {table} in bucket: {bucket}")
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

    def delete_data(self, spark: SparkSession, df_new: DataFrame, pk_list: list, bucket: str, table: str):
        """
        Mark as deleted the record in the specified delta table

        Args:
            spark: Spark session
            df_new: New spark dataframe
            pk_list: List of primary keys
            bucket: AWS s3 path where the table will be delete
            table: The name of the table
        """

        spark_read_options = {
            "format": "delta",
        }
        df_base = self.read_spark_dataframe(spark=spark, s3_path=bucket, table=table, **spark_read_options)
        forms_id = df_new.select(pk_list[0]).distinct().rdd.map(lambda x: x[0]).collect()
        df_base = df_base.filter((F.col(pk_list[0]).isin(forms_id)) & (F.col("deleted_at").isNotNull()))

        df_deleted = df_base.select(*pk_list).exceptAll(df_new.select(*pk_list))
        df_deleted = df_deleted.withColumn("deleted_at", F.current_timestamp())
        # Check
        if ~df_deleted.isEmpty():
            print(df_deleted.show())
            table = DeltaTable.forPath(spark, f"{bucket}{table}")

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
        Create view of all delta tables within the list

        Args:
            spark: Spark session
            list_temp_views: List of name of views with a specified s3 path
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
        Update delta table in the specified delta table

        Args:
            spark: Spark session
            df_new: New spark dataframe
            pk_list: List of primary keys
            bucket: AWS s3 path where the table will be updated
            table: The name of the table
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
        Update or Insert data in the specified delta table.
        Case the record in destination table not match with update source data. The record will
        be marked as deleted

        Args:
            spark: Spark session
            df: New spark dataframe
            primary_keys: List of primary keys
            bucket: AWS s3 path where the table will be delete
            table: The name of the table
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
            df: Dataframe to remove duplicate data
            primary_keys: List of primary keys
            ts_column: timestamp column
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
        Check if delta table exists in the specified bucket.

        Args:
            zone_path: AWS s3 path
            table_name: The name of the table
        """
        aws_bucket = AWSBucket()
        s3_client = aws_bucket.get_boto3_session().client("s3")

        bucket_name = get_bucket_name_from_zone_path(zone_path)
        folder_name = get_folder_from_zone_path(zone_path)

        return not (aws_bucket.check_s3_folder(s3_client, bucket_name, folder_name + f"{table_name}/"))

    def get_df_latest_batch(self, df: DataFrame, date_col: str):
        """
        Get latest batch in dataframe

        Args:
            df: Dataframe
            date_col: Date column used to filter the latest batch
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
            df: Dataframe
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
        Extracts 'year' and 'month' partition columns from a specified date column in a DataFrame.

        Given a DataFrame and the name of a date column within it, this method adds two new columns 'year' and 'month'
        based on the values in the specified date column. It's useful for creating partitioned datasets based on date.

        Args:
            df (DataFrame): The input DataFrame containing the date column to be processed.
            base_column (str): The name of the date column in the input DataFrame from which
            'year' and 'month' columns are to be extracted.

        Returns:
            DataFrame: A new DataFrame with the added 'year' and 'month' columns.
            list: A list of the newly added column names ['year', 'month'].
        """

        new_df = df.withColumn("year", F.date_format(F.col(base_column), "yyyy")).withColumn(
            "month", F.date_format(F.col(base_column), "MM")
        )

        return new_df, ["year", "month"]

    def get_spark_equivalent_type(self, arrow_type: str):
        """
        Retrieve the Spark equivalent data type for a given PyArrow type.

        This method provides a mapping from PyArrow data types to Spark data types.
        If the provided `arrow_type` does not have a direct mapping, it defaults to Spark's StringType().

        Args:
            arrow_type (str): The PyArrow data type in string format for which the Spark equivalent is required.

        Returns:
            DataType: The Spark equivalent data type for the given PyArrow type. Defaults to StringType() if no mapping is found.
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
        Replace specific characters in the column names of a DataFrame.

        This method scans through the column names of the given DataFrame and replaces occurrences of `old_caracter` with `new_caracter`.

        Args:
            df (DataFrame): Input DataFrame whose column names are to be modified.
            old_caracter (str): The character (or string) to be replaced in the column names.
            new_caracter (str): The character (or string) that will replace the `old_caracter` in the column names.

        Returns:
            DataFrame: A DataFrame with modified column names.
        """
        new_columns = [col.replace(old_caracter, new_caracter) for col in df.columns]

        return df.toDF(*new_columns)

    def convert_pandas_df_to_spark_df(
        self, spark: SparkSession, df: PandasDataFrame, int_constant: int = -1234567
    ) -> DataFrame:
        """
        Convert a Pandas DataFrame to a Spark DataFrame.

        This method provides compatibility for conversions of Pandas DataFrames to Spark DataFrames,
        especially for Pandas versions >= 2.0.0 which aren't compatible with `spark.createDataFrame` directly.

        Args:
            spark (SparkSession): An active Spark session to create the Spark DataFrame.
            df (PandasDataFrame): The Pandas DataFrame to be converted.
            int_constant (int, optional): A constant integer value to be replaced in the resulting Spark DataFrame. Defaults to -1234567.

        Returns:
            DataFrame: A Spark DataFrame converted from the input Pandas DataFrame.

        """

        df_columns = list(df.columns)
        df_types = list(df.dtypes)
        spark_struct_list = []

        for df_column, df_type in zip(df_columns, df_types):
            spark_type = self.get_spark_equivalent_type(str(df_type))
            spark_struct_field = StructField(df_column, spark_type)

            spark_struct_list.append(spark_struct_field)

        spark_df_schema = StructType(spark_struct_list)

        data = convert_pandas_df_to_list(df)

        return spark.createDataFrame(data, schema=spark_df_schema).replace(int_constant, None)

    def select_by_prefix(
        self, df: DataFrame, desired_prefix: str, prefixes_to_ignore: list, always_include: list
    ) -> DataFrame:
        """
        Selects columns from a DataFrame based on desired prefixes, ignoring some prefixes and always including others.

        Args:
            df (DataFrame): The input DataFrame from which columns will be selected.
            desired_prefix (str): The prefix desired for columns to be selected.
            prefixes_to_ignore (list): List of column prefixes to be ignored in the selection.
            always_include (list): List of column names that should always be included regardless of their prefix.

        Returns:
            DataFrame: A new DataFrame containing only the selected columns.
        """
        return df.select(
            *[
                col
                for col in df.columns
                if col.startswith(desired_prefix)
                or not any(col.startswith(prefix) for prefix in prefixes_to_ignore)  # noqa: W503
                or col in always_include  # noqa: W503
            ]
        )
