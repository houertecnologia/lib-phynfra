# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.utils import AnalysisException

from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import ArrayType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from delta.tables import DeltaTable

from urllib.parse import urlparse

PROFILES = {
	'vanilla': {
		'spark.master': 'local[*]'
	},
	'delta': {
		'spark.master': 'local[*]',
		'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
		'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
		'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
		'spark.databricks.delta.schema.autoMerge.enabled': 'true',
		'spark.sql.sources.partitionOverwriteMode': 'dynamic'
	}
}

class Spark ():
	'''
	Spark Class
	Manages SparkSession
	SparkSession is to run code inside the container to reach "datanode", to query and write data
	spark-submit is not runnable from this class. It requires spark-cli installed in local machine
	'''
	
	def __init__ (self, application: str, configuration: dict = None, level: str = 'WARN', profile: str = 'delta') -> None:
		'''
		Initialize a SparkSession with profiles and custom configurations
		application:str - Application name
		configuration:dict - Custom Spark configurations (overrides profile defaults)
		level:str - Log level (e.g. 'WARN', 'INFO', 'DEBUG', 'ERROR')
		profile:str - Profile name ('vanilla' or 'delta', defaults to 'delta')
		'''

		if (not application) or (not isinstance(application, str)):

			raise ValueError('Spark Application Name must be a non-empty string!')

		if (profile is not None) and (profile not in PROFILES):

			raise ValueError('Invalid Spark profile: %s. Expected one of: %s' % (profile, list(PROFILES.keys())))

		if (configuration is not None) and (not isinstance(configuration, dict)):

			raise ValueError('Configuration must be a dictionary or None!')

		effectiveConfiguration = {}

		if (profile in PROFILES):

			effectiveConfiguration.update(PROFILES[profile])

		if isinstance(configuration, dict):

			effectiveConfiguration.update(configuration)

		try:

			builder = SparkSession.builder.appName(application)

			for (key, value) in effectiveConfiguration.items():

				builder.config(str(key).strip(), str(value).strip())

			self.spark = builder.getOrCreate()

		except Exception as error:

			raise error

		try:

			if level and hasattr(self.spark, 'sparkContext') and self.spark.sparkContext:

				self.spark.sparkContext.setLogLevel(str(level).strip())

		except Exception as error:

			raise error

	def get (self):
		'''
		Get spark session
		'''

		return self.spark

	def stop (self):
		'''
		Stop Spark Session
		'''

		self.spark.stop()

class DataFramer ():
	'''
	Encapsulates Operations of/to a SparkDataFrame
	'''

	def __init__ (self, spark):
		'''
		spark:Spark - A phynfra spark manager to retrieve session
		'''

		if isinstance(spark, Spark):

			self._spark = spark.get()

			self.dataframe = None

		else:

			raise ValueError('Cannot create a DataFrame without Spark Session (spark argument)')

	def get (self):
		'''
		Returns the current dataframe
		'''

		return self.dataframe

	def create (self, schema):
		'''
		Create an empty dataframe by a schema
		'''

		# TODO: handle schema

		self.dataframe = self._spark.createDataFrame([], schema = schema)

		return self

	def append (self, data):
		'''
		'''

		if (not data) or (len(data) == 0):

			raise ValueError('To append data to a DataFrame it should be a list of data according to DataFrame schema')

		if not self.dataframe:

			raise ValueError('DataFramer\'s DataFrame does not exists. Create it first.')

		new = self._spark.createDataFrame(data, schema = self.dataframe.schema)

		self.dataframe = self.dataframe.union(new)

		del new

		return self

	def read (self, fullpath, arguments):
		'''
		Build-in is a capacity of Spark itself
		fullpath:str* - URI
		arguments:dict
		'''

		if not fullpath:

			raise ValueError('Cannot read to a DataFrame an empty or None fullpath')

		try:

			parsed = urlparse(fullpath)

			basename = os.path.basename(parsed.path)

			filename, extension = os.path.splitext(basename)

			if not arguments:

				arguments = {
					'format': extension[1:]
				}

			elif 'format' not in arguments:

				arguments['format'] = extension[1:]

			self.dataframe = self.loadDataFrameNative(fullpath, arguments)

			return self

		except Exception as error:

			raise error

	def write (self, fullpath: str, arguments: dict = None, dataframe: SparkDataFrame = None, mode: str = 'append', partitions = None, overwrite: bool = None) -> 'DataFramer':
		'''
		Unified write method supporting simple and partitioned writes with Delta Lake default
		fullpath:str* - Target URI (e.g. s3a://..., file://...)
		arguments:dict - Spark write options
		dataframe:SparkDataFrame - DataFrame to be written (defaults to self.dataframe)
		mode:str - Write mode ('append', 'overwrite', 'ignore', 'error', defaults to 'append')
		partitions:list|str - Partition column names
		overwrite:bool - Legacy flag for backwards compatibility
		'''

		if (not fullpath) or (not isinstance(fullpath, str)):

			raise ValueError('Cannot write to a destination without a valid fullpath string')

		targetDataFrame = dataframe if isinstance(dataframe, SparkDataFrame) else self.dataframe

		if not targetDataFrame:

			raise ValueError('Cannot write: DataFramer has no active DataFrame')

		try:

			# Resolve legacy overwrite flag if explicitly provided
			effectiveMode = mode

			if (overwrite is True):

				effectiveMode = 'overwrite'

			elif (overwrite is False) and (mode == 'append'):

				effectiveMode = 'append'

			# Parse path and extension, stripping trailing slashes
			cleanPath = fullpath.rstrip('/')

			parsed = urlparse(cleanPath)

			basename = os.path.basename(parsed.path)

			filename, extension = os.path.splitext(basename)

			fileExtension = extension[1:] if extension.startswith('.') else extension

			# Normalize write arguments
			writeOptions = arguments.copy() if isinstance(arguments, dict) else {}

			# Determine file format: arguments['format'] > fileExtension > 'delta'
			if ('format' in writeOptions) and writeOptions['format']:

				fileFormat = str(writeOptions.pop('format')).strip()

			elif fileExtension:

				fileFormat = fileExtension.strip()

			else:

				fileFormat = 'delta'

			# Medallion architecture compliance check
			lowerPath = fullpath.lower()

			protectedZones = ['bronze', 'silver', 'gold']

			for zone in protectedZones:

				patterns = ['/%s/' % zone, '/%s' % zone, '-%s/' % zone, '-%s' % zone]

				if any([pattern in lowerPath for pattern in patterns]):

					if fileFormat.lower() != 'delta':

						raise ValueError('Medallion architecture violation: %s zone requires Delta Lake (delta) format, but received: %s' % (zone, fileFormat))

					break

			# Resolve partition columns: partitions argument > writeOptions['partitions'] > writeOptions['partition_columns']
			rawPartitions = partitions

			if (rawPartitions is None):

				if ('partitions' in writeOptions):

					rawPartitions = writeOptions.pop('partitions')

				elif ('partition_columns' in writeOptions):

					rawPartitions = writeOptions.pop('partition_columns')

			if isinstance(rawPartitions, str):

				partitionColumns = [rawPartitions.strip()]

			elif isinstance(rawPartitions, (list, tuple)):

				partitionColumns = [str(col).strip() for col in rawPartitions]

			else:

				partitionColumns = []

			# Build writer with chained calls
			writer = targetDataFrame.write.format(fileFormat).mode(effectiveMode).options(**writeOptions)

			if (len(partitionColumns) > 0):

				writer = writer.partitionBy(*partitionColumns)

			writer.save(fullpath)

			return self

		except Exception as error:

			raise error

	def upsert (self, destination: str, primaryKeys, dataframe: SparkDataFrame = None, trackDeletes: bool = False, deleteColumn: str = 'deleted_at') -> 'DataFramer':
		'''
		Atomic MERGE operation for Delta Lake tables with optional soft delete tracking
		destination:str - Target URI / path of the Delta table
		primaryKeys:list|str - Key column(s) for the merge condition
		dataframe:SparkDataFrame - Source DataFrame (defaults to self.dataframe)
		trackDeletes:bool - Enables logical deletion tracking (whenNotMatchedBySourceUpdate)
		deleteColumn:str - Column name for soft delete timestamp
		'''

		if (not destination) or (not isinstance(destination, str)):

			raise ValueError('Destination must be a non-empty string!')

		if not primaryKeys:

			raise ValueError('primaryKeys must be informed!')

		if isinstance(primaryKeys, str):

			keys = [primaryKeys.strip()]

		elif isinstance(primaryKeys, (list, tuple)):

			keys = [str(pk).strip() for pk in primaryKeys if str(pk).strip()]

		else:

			raise ValueError('primaryKeys must be a string or list/tuple of strings!')

		if len(keys) == 0:

			raise ValueError('primaryKeys must contain at least one valid column name!')

		sourceDataFrame = dataframe if isinstance(dataframe, SparkDataFrame) else self.dataframe

		if not sourceDataFrame:

			raise ValueError('Cannot perform upsert: DataFramer has no active DataFrame')

		mergeCondition = ' AND '.join([f'target.{pk} = source.{pk}' for pk in keys])

		try:

			deltaTable = DeltaTable.forPath(self._spark, destination)

			mergeBuilder = deltaTable.alias('target').merge(sourceDataFrame.alias('source'), mergeCondition).whenMatchedUpdateAll().whenNotMatchedInsertAll()

			if trackDeletes:

				mergeBuilder = mergeBuilder.whenNotMatchedBySourceUpdate(condition = f'target.{deleteColumn} IS NULL', set = {f'target.{deleteColumn}': F.current_timestamp()})

			mergeBuilder.execute()

		except AnalysisException:

			sourceDataFrame.write.format('delta').save(destination)

		except Exception as error:

			raise error

		return self

	def loadDataFrameNative (self, fullpath, arguments):
		'''
		'''

		return self._spark.read.load(fullpath, **arguments)

	def to_pandas (self):
		'''
		'''

		if self.dataframe:

			self.dataframe = self.dataframe.toPandas()

		else:

			raise ValueError('Cannot convert to Pandas\' DataFrame something that does not exist')

		return self

	def from_pandas (self, dataframe = None):
		'''
		'''

		reference = dataframe if dataframe else self.dataframe

		if reference and isinstance(reference, PandasDataFrame):

			self.dataframe = self._spark.createDataFrame(reference)

		else:

			raise ValueError('DataFrame must be an instance of pandas.DataFrame to be converted')

		return self

	def to_list (self):

		if self.dataframe:

			if isinstance(self.dataframe, SparkDataFrame):

				return self.dataframe.collect()

			elif isinstance(self.dataframe, PandasDataFrame):

				return self.dataframe.values.tolist()

			else:

				return []

		else:

			raise ValueError('Cannot convert to Python List something that does not exist')

		return self

	# get_df_latest_batch :: https://github.com/houertecnologia/project-turboman/blob/dev/processing/app/commons/helpers/spark_utils.py
	# flatten :: https://github.com/houertecnologia/project-turboman/blob/dev/processing/app/commons/helpers/spark_utils.py
	