# -*- coding: utf-8 -*-

import re
import boto3

from phynfra.atomic.utils import is_not_string, is_not_list
from phynfra.apache.spark import DataFramer
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

STORAGE = {
	"parquet": {
		"InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
		"OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
		"SerdeInfo": {
			"SerializationLibrary":"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
		},
		"Parameters":  {
			"classification":"parquet"
		}
	},
	"orc": {
		"InputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
		"OutputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
		"SerdeInfo": {
			"SerializationLibrary":"org.apache.hadoop.hive.ql.io.orc.OrcSerde"
		},
		"Parameters":  {
			"classification":"orc"
		}
	},
	"json": {
		"InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
		"OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
		"SerdeInfo": {
			"SerializationLibrary":"org.openx.data.jsonserde.JsonSerDe"
		},
		"Parameters":  {
			"classification":"json"
		}
	},
	"csv": {
		"InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
		"OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
		"SerdeInfo": {
			"SerializationLibrary":"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
		},
		"Parameters":  {
			"classification":"csv" # set SerDe params (separator, quote) in TableInput.Parameters if needed
		}  
	},
	"avro": {
		"InputFormat": "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
		"OutputFormat": "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
		"SerdeInfo": {
			"SerializationLibrary":"org.apache.hadoop.hive.serde2.avro.AvroSerDe"
		},
		"Parameters":  {
			"classification":"avro"
		}
	},
	"delta": {
		"InputFormat": "io.delta.hive.DeltaInputFormat",
		"OutputFormat": "io.delta.hive.DeltaOutputFormat",
		"SerdeInfo": {
			"SerializationLibrary":"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
		},
		"Parameters": {
			"classification":"delta",
			"table_type":"DELTA" # keep Location at table SD
		}  
	}
}

COLUMNS = [
	r'^boolean$',
	r'^tinyint$',
	r'^smallint$',
	r'^int$',
	r'^bigint$',
	r'^float$',
	r'^double$',
	r'^decimal\(\d+,\d+\)$',
	r'^string$',
	r'^varchar\(\d+\)$',
	r'^char\(\d+\)$',
	r'^binary$',
	r'^date$',
	r'^timestamp(\swith\stime\szone)?$',
	r'^array<.+>$',
	r'^map<.+,.+>$',
	r'^struct<.+>$'
]

PARTITIONS = [
	r'^string$',
	r'^date$',
	r'^int$',
	r'^bigint$',
	r'^timestamp$'
]

class Glue ():
	'''
	'''

	def __init__ (self, region = None, accessKey = None, secretKey = None):
		'''
		'''

		if is_not_string(region):

			raise ValueError('Region must be a valid AWS Region string')

		if is_not_string(accessKey):

			raise ValueError('AWS Access Key ID must be a valid AWS Access Key')

		if is_not_string(secretKey):

			raise ValueError('AWS Secret Access Key must be a valid AWS Secret Access Key')

		try:

			self.client = boto3.client('glue', region_name = region, aws_access_key_id = accessKey, aws_secret_access_key = secretKey)

		except Exception as botoError:

			raise ValueError('Internal error initializating Glue Client') from botoError

	def save_database (self, name = None, description = None):
		'''
		'''

		if is_not_string(name):

			raise ValueError('Database name must be a valid string')

		if is_not_string(description):

			raise ValueError('Database description must be a valid string')

		try:

			self.client.get_database(Name = name)

			raise RuntimeError('%s already exists' % name)

		except self.client.exceptions.EntityNotFoundException:

			self.client.create_database(DatabaseInput = {
				"Name": name,
				"Description": description
			})

			return self

	def save_table (self, database = None, name = None, storagetype = None, location = None, columns = None, partitions = None, extra = None):
		'''
		Partitions: put low-cardinality first (e.g., year/month/day) and high-cardinality last, so list order is important
		'''

		if is_not_string(database):

			raise ValueError('To save a Glue table, you must inform the database name')

		if is_not_string(name):

			raise ValueError('To save a Glue table, you must provide the table name')

		if is_not_string(storagetype):

			raise ValueError('To save a Glue table, you must provide the table name')

		elif storagetype not in STORAGE:

			raise ValueError('%s is not a valid storage type - Acceptables: %s' % ', '.join(STORAGE.keys()))
		
		if is_not_string(location):

			raise ValueError('To save a Glue table, you must provide the s3:// location')

		if is_not_list(columns):

			raise ValueError('To save a Glue table, you must provide the columns')

		settings = STORAGE[storagetype]

		table = {
			"Name": name,
			"TableType": "EXTERNAL_TABLE",
			"Parameters": settings['Parameters'],
			"StorageDescriptor": {
				"Location": location,
				"Columns": columns,
				"InputFormat": settings['InputFormat'],
				"OutputFormat": settings['OutputFormat'],
				"SerdeInfo": settings['SerdeInfo']
			},
			"PartitionKeys": None
		}

		if extra != None and isinstance(extra, dict):

			table['Parameters'] = table['Parameters'] | extra

		if partitions != None and isinstance(partitions, list) and len(partitions) > 0:

			allset = True

			for partition in partitions:

				allset = allset and any([re.match(pattern, partition['Type']) for pattern in PARTITIONS])

			if not allset:

				raise RuntimeError('Partitions type are wrong. Fix then')

			table['PartitionKeys'] = partitions

		try:
			
			self.client.get_table(DatabaseName = database, Name = name)
		
			self.client.update_table(DatabaseName = database, TableInput = table)

		except self.client.exceptions.EntityNotFoundException:
		
			self.client.create_table(DatabaseName = database, TableInput = table)

		return self

	def save_table_dataframe (self, database = None, name = None, storagetype = None, location = None, dataframe = None, partitions = None, extra = None):
		'''
		'''

		df = None

		if isinstance(dataframe, SparkDataFrame):

			df = dataframe

		elif isinstance(dataframe, DataFramer):

			df = dataframe.get()

		else:

			raise ValueError('dataframe should be a phynfra Dataframer or a spark Dataframe')

		if not df.columns:

			raise RuntimeError('Spark dataframe has no columns - Evaluating by not df.columns')

		columns = []

		excludes = []

		if len(partitions) > 0:

			excludes = [p['Name'] for p in partitions]

		for f in df.schema.fields:

			columntype = f.dataType.simpleString()

			#print(f.dataType, f.dataType.simpleString())

			valid = any([re.match(p, columntype) for p in COLUMNS])

			if not valid:

				raise RuntimeError('%s is not a acceptable column type for Glue' % columntype)

			else:

				if f.name not in excludes:

					columns.append({
						"Name": f.name,
						"Type": columntype
					})

		return self.save_table(database, name, storagetype, location, columns, partitions, extra)
