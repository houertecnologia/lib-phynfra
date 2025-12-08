# -*- coding: utf-8 -*-

import re
import boto3

from phynfra.atomic.utils import is_not_string, is_not_list
from phynfra.apache.spark import DataFramer
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from botocore.config import Config

class Athena:
	'''
	'''

	def __init__ (self, region = None, accessKey = None, secretKey = None):
		'''
		'''

		if not isinstance(region, str) or region == '':

			raise ValueError('Region must be a valid AWS Region string')

		if not isinstance(accessKey, str) or accessKey == '':

			raise ValueError('AWS Access Key ID must be a valid AWS Access Key')

		if not isinstance(secretKey, str) or secretKey == '':

			raise ValueError('AWS Secret Access Key must be a valid AWS Secret Access Key')

		try:

			self.client = boto3.client('athena', region_name = region, aws_access_key_id = accessKey, aws_secret_access_key = secretKey)

		except Exception as botoError:

			raise ValueError('Internal error initializating Athena Boto Client') from botoError

	def repair (self, database = None, table = None, results = None):
		'''
		'''

		if not isinstance(database, str) or database == '':

			raise ValueError('Database must be a valid AWS Glue database')

		if not isinstance(table, str) or table == '':

			raise ValueError('Table must be a valid AWS Glue table in a database')

		if not isinstance(results, str) or results == '':

			raise ValueError('Results must be a valid AWS S3 Path to Athena Results Bucket, as s3://path/to/folder')

		try:

			output = self.client.start_query_execution (
				QueryString = 'MSCK REPAIR TABLE `%s`;' % (table),
				WorkGroup = 'primary',
				QueryExecutionContext = {
					'Database': database
				},
				ResultConfiguration = {
					'OutputLocation': results
				}
			)

		except Exception as athenaError:

			raise ValueError('Fail to repair table in Athena') from athenaError
