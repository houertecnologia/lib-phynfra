# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import ArrayType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from delta.tables import DeltaTable

from urllib.parse import urlparse

from phynfra.atomic.utils import sanitize

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

	def restore (self, destination: str, version = None, timestamp = None) -> 'DataFramer':
		'''
		Restores a Delta Lake table to a previous version or timestamp
		destination:str - Target path / URI of the Delta table
		version:int|str - Version number to restore to
		timestamp:str - Timestamp to restore to
		'''

		if (not destination) or (not isinstance(destination, str)) or (not destination.strip()):

			raise ValueError('Destination must be a non-empty string!')

		if (version is None) and (timestamp is None):

			raise ValueError('At least one of version or timestamp must be provided for restore!')

		try:

			deltaTable = DeltaTable.forPath(self.spark, destination.strip())

			if version is not None:

				targetVersion = int(version) if isinstance(version, (int, str)) else version

				deltaTable.restoreToVersion(targetVersion)

			elif timestamp is not None:

				deltaTable.restoreToTimestamp(str(timestamp).strip())

			return DataFramer(self).read(destination.strip())

		except Exception as error:

			raise error

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

	def read (self, fullpath: str, arguments: dict = None, version = None, timestamp = None) -> 'DataFramer':
		'''
		Build-in is a capacity of Spark itself with Delta Time Travel support
		fullpath:str* - URI
		arguments:dict - Spark read options
		version:int|str - Version number for Delta Time Travel (versionAsOf)
		timestamp:str - Timestamp string for Delta Time Travel (timestampAsOf)
		'''

		if not fullpath:

			raise ValueError('Cannot read to a DataFrame an empty or None fullpath')

		try:

			parsed = urlparse(fullpath.rstrip('/'))

			basename = os.path.basename(parsed.path)

			filename, extension = os.path.splitext(basename)

			readOptions = arguments.copy() if isinstance(arguments, dict) else {}

			# Map time travel options with precedence of version over timestamp
			if version is not None:

				readOptions['versionAsOf'] = version

			elif timestamp is not None:

				readOptions['timestampAsOf'] = str(timestamp).strip()

			if 'format' not in readOptions:

				fileExtension = extension[1:] if extension.startswith('.') else extension

				readOptions['format'] = fileExtension if fileExtension else 'delta'

			self.dataframe = self.loadDataFrameNative(fullpath, readOptions)

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

	def deduplicate (self, primaryKeys, timestampColumn: str, dataframe: SparkDataFrame = None) -> 'DataFramer':
		'''
		Deterministic deduplication keeping the most recent version
		primaryKeys:list|str - Key column(s) for partitioning
		timestampColumn:str - Column name representing the record timestamp
		dataframe:SparkDataFrame - Target DataFrame (defaults to self.dataframe)
		'''

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

		if (not timestampColumn) or (not isinstance(timestampColumn, str)):

			raise ValueError('timestampColumn must be a non-empty string!')

		targetDataFrame = dataframe if isinstance(dataframe, SparkDataFrame) else self.dataframe

		if not targetDataFrame:

			raise ValueError('Cannot perform deduplicate: DataFramer has no active DataFrame')

		try:

			deduplicationWindow = Window.partitionBy(keys).orderBy(F.col(timestampColumn).desc(), F.col('tiebreak').desc())

			deduplicatedDataFrame = targetDataFrame.withColumn('tiebreak', F.monotonically_increasing_id()).withColumn('rank', F.rank().over(deduplicationWindow)).filter(F.col('rank') == 1).drop('rank', 'tiebreak')

			if dataframe is None:

				self.dataframe = deduplicatedDataFrame

			return self

		except Exception as error:

			raise error

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

	def flatten (self, columns = None, separator: str = '_', explodeArrays: bool = False, maxDepth: int = None, dataframe: SparkDataFrame = None) -> 'DataFramer':
		'''
		Structured in-memory flattening for nested StructType and ArrayType fields
		columns:list|str - Specific columns to flatten (defaults to all)
		separator:str - Separator between parent and child column names (default '_')
		explodeArrays:bool - Whether to explode ArrayType fields using explode_outer
		maxDepth:int - Maximum depth of flattening iterations
		dataframe:SparkDataFrame - Target DataFrame (defaults to self.dataframe)
		'''

		targetDataFrame = dataframe if isinstance(dataframe, SparkDataFrame) else self.dataframe

		if not targetDataFrame:

			raise ValueError('Cannot perform flatten: DataFramer has no active DataFrame')

		targetColumns = None

		if columns is not None:

			if isinstance(columns, str):

				targetColumns = [columns.strip()]

			elif isinstance(columns, (list, tuple)):

				targetColumns = [str(col).strip() for col in columns if str(col).strip()]

			else:

				raise ValueError('columns must be a string or list/tuple of strings!')

		try:

			currentDataFrame = targetDataFrame

			currentDepth = 0

			activeColumns = targetColumns.copy() if targetColumns is not None else None

			while True:

				if (maxDepth is not None) and isinstance(maxDepth, int) and (currentDepth >= maxDepth):

					break

				fields = currentDataFrame.schema.fields

				structFields = [f for f in fields if isinstance(f.dataType, StructType) and ((activeColumns is None) or (f.name in activeColumns))]

				arrayFields = [f for f in fields if isinstance(f.dataType, ArrayType) and ((activeColumns is None) or (f.name in activeColumns))] if explodeArrays else []

				if (len(structFields) == 0) and (len(arrayFields) == 0):

					break

				if len(arrayFields) > 0:

					for arrayField in arrayFields:

						currentDataFrame = currentDataFrame.withColumn(arrayField.name, F.explode_outer(F.col(arrayField.name)))

					fields = currentDataFrame.schema.fields

					structFields = [f for f in fields if isinstance(f.dataType, StructType) and ((activeColumns is None) or (f.name in activeColumns))]

				if len(structFields) > 0:

					selectExpressions = []

					newActiveColumns = set(activeColumns) if activeColumns is not None else None

					for field in fields:

						if field in structFields:

							if newActiveColumns is not None:

								newActiveColumns.discard(field.name)

							for childField in field.dataType.fields:

								newColName = f'{field.name}{separator}{childField.name}'

								selectExpressions.append(F.col(f'{field.name}.{childField.name}').alias(newColName))

								if newActiveColumns is not None:

									newActiveColumns.add(newColName)

						else:

							selectExpressions.append(F.col(field.name))

					currentDataFrame = currentDataFrame.select(*selectExpressions)

					if activeColumns is not None:

						activeColumns = list(newActiveColumns)

				currentDepth += 1

			self.dataframe = currentDataFrame

			return self

		except Exception as error:

			raise error

	def sanitizeColumns (self, dataframe: SparkDataFrame = None) -> 'DataFramer':
		'''
		Sanitizes column names in-memory, discarding blank or whitespace-only columns
		dataframe:SparkDataFrame - Target DataFrame (defaults to self.dataframe)
		'''

		targetDataFrame = dataframe if isinstance(dataframe, SparkDataFrame) else self.dataframe

		if not targetDataFrame:

			raise ValueError('Cannot perform sanitizeColumns: DataFramer has no active DataFrame')

		try:

			validColumns = [col for col in targetDataFrame.columns if col and col.strip()]

			if len(validColumns) == 0:

				raise ValueError('All columns in DataFrame are empty or whitespace!')

			filteredDataFrame = targetDataFrame.select(*validColumns)

			sanitizedNames = [sanitize(col) for col in validColumns]

			resultDataFrame = filteredDataFrame.toDF(*sanitizedNames)

			self.dataframe = resultDataFrame

			return self

		except Exception as error:

			raise error

	def enforce (self, schema: list, fallback: str = 'string', dataframe: SparkDataFrame = None) -> 'DataFramer':
		'''
		Enforces a strict schema contract with missing column injection and fallback type casting
		schema:list - List of dicts with 'source', 'target', and 'type' keys
		fallback:str - Fallback DataType when casting fails (default 'string')
		dataframe:SparkDataFrame - Target DataFrame (defaults to self.dataframe)
		'''

		if (not schema) or (not isinstance(schema, (list, tuple))) or (len(schema) == 0):

			raise ValueError('schema must be a non-empty list of dictionaries!')

		for index, item in enumerate(schema):

			if (not isinstance(item, dict)):

				raise ValueError('schema item at index %s must be a dictionary!' % index)

			if ('source' not in item) or ('target' not in item) or ('type' not in item):

				raise ValueError('schema item at index %s must contain keys: source, target, type!' % index)

			if (not item['source']) or (not item['target']) or (not item['type']):

				raise ValueError('schema item at index %s has empty source, target or type!' % index)

		targetDataFrame = dataframe if isinstance(dataframe, SparkDataFrame) else self.dataframe

		if not targetDataFrame:

			raise ValueError('Cannot perform enforce: DataFramer has no active DataFrame')

		try:

			currentDataFrame = targetDataFrame

			availableColumns = set(currentDataFrame.columns)

			# Ensure all source columns exist
			for item in schema:

				sourceCol = str(item['source']).strip()

				if sourceCol not in availableColumns:

					currentDataFrame = currentDataFrame.withColumn(sourceCol, F.lit(None))

					availableColumns.add(sourceCol)

			# Build projections with type casting and fallback
			selectExpressions = []

			for item in schema:

				sourceCol = str(item['source']).strip()

				targetCol = str(item['target']).strip()

				targetType = item['type']

				try:

					castExpr = F.col(sourceCol).cast(targetType).alias(targetCol)

					currentDataFrame.select(castExpr)

					selectExpressions.append(castExpr)

				except Exception:

					fallbackExpr = F.col(sourceCol).cast(fallback).alias(targetCol)

					selectExpressions.append(fallbackExpr)

			resultDataFrame = currentDataFrame.select(*selectExpressions)

			self.dataframe = resultDataFrame

			return self

		except Exception as error:

			raise error

	def view (self, name: str, dataframe: SparkDataFrame = None) -> 'DataFramer':
		'''
		Registers the current or specified DataFrame as a Temporary View in the SparkSession
		name:str - Name of the temporary view
		dataframe:SparkDataFrame - Target DataFrame (defaults to self.dataframe)
		'''

		if (not name) or (not isinstance(name, str)) or (not name.strip()):

			raise ValueError('View name must be a non-empty string!')

		targetDataFrame = dataframe if isinstance(dataframe, SparkDataFrame) else self.dataframe

		if not targetDataFrame:

			raise ValueError('Cannot create view: DataFramer has no active DataFrame')

		try:

			targetDataFrame.createOrReplaceTempView(name.strip())

			return self

		except Exception as error:

			raise error

	def latest (self, column: str, dataframe: SparkDataFrame = None) -> 'DataFramer':
		'''
		Filters the DataFrame keeping only records with the maximum value of the specified column
		column:str - Column name to find the maximum value
		dataframe:SparkDataFrame - Target DataFrame (defaults to self.dataframe)
		'''

		if (not column) or (not isinstance(column, str)) or (not column.strip()):

			raise ValueError('Column name must be a non-empty string!')

		targetDataFrame = dataframe if isinstance(dataframe, SparkDataFrame) else self.dataframe

		if not targetDataFrame:

			raise ValueError('Cannot perform latest: DataFramer has no active DataFrame')

		try:

			colName = column.strip()

			maxValue = targetDataFrame.select(F.max(F.col(colName))).collect()[0][0]

			resultDataFrame = targetDataFrame.filter(F.col(colName) == maxValue)

			self.dataframe = resultDataFrame

			return self

		except Exception as error:

			raise error
	