# -*- coding: utf-8 -*-

import re
import time
import boto3

from phynfra.atomic.utils import is_not_string, is_not_list
from phynfra.apache.spark import DataFramer
from pyspark.sql import DataFrame as SparkDataFrame

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

	def query (self, sql: str, database: str, results: str, asDataFramer: bool = True, spark = None, pollInterval: int = 1, timeout: int = 60):
		'''
		Synchronously executes an SQL query on AWS Athena with polling
		sql:str - SQL query statement
		database:str - AWS Glue database name
		results:str - S3 output location for query results
		asDataFramer:bool - Whether to return results as DataFramer or list of dicts (default True)
		spark:Spark - Spark manager instance (required if asDataFramer is True)
		pollInterval:int - Seconds between status polls (default 1)
		timeout:int - Maximum seconds to wait before timeout (default 60)
		'''

		if (not sql) or (not isinstance(sql, str)) or (not sql.strip()):

			raise ValueError('SQL query must be a valid non-empty string')

		if (not database) or (not isinstance(database, str)) or (not database.strip()):

			raise ValueError('Database must be a valid non-empty string')

		if (not results) or (not isinstance(results, str)) or (not results.strip()):

			raise ValueError('Results output location must be a valid non-empty string')

		try:

			execution = self.client.start_query_execution(
				QueryString = sql.strip(),
				QueryExecutionContext = {
					'Database': database.strip()
				},
				ResultConfiguration = {
					'OutputLocation': results.strip()
				}
			)

			executionId = execution['QueryExecutionId']

		except Exception as startError:

			raise ValueError('Internal error starting Athena query execution') from startError

		startTime = time.time()

		while True:

			elapsed = time.time() - startTime

			if elapsed > timeout:

				raise TimeoutError('Athena query execution timed out after %s seconds (Execution ID: %s)' % (timeout, executionId))

			try:

				response = self.client.get_query_execution(QueryExecutionId = executionId)

				statusInfo = response['QueryExecution']['Status']

				state = statusInfo['State']

			except Exception as statusError:

				raise ValueError('Internal error getting Athena query execution status') from statusError

			if state == 'SUCCEEDED':

				break

			elif state in ('FAILED', 'CANCELLED'):

				reason = statusInfo.get('StateChangeReason', 'Unknown reason')

				raise RuntimeError('Athena query %s. StateChangeReason: %s' % (state, reason))

			time.sleep(pollInterval)

		if asDataFramer:

			csvPath = '%s/%s.csv' % (results.rstrip('/'), executionId)

			if not spark:

				raise ValueError('A Spark manager instance is required when asDataFramer is True')

			dataFramer = DataFramer(spark)

			return dataFramer.read(csvPath)

		else:

			resultList = []

			nextToken = None

			headers = None

			while True:

				kwargs = {'QueryExecutionId': executionId}

				if nextToken:

					kwargs['NextToken'] = nextToken

				resultsResponse = self.client.get_query_results(**kwargs)

				rows = resultsResponse.get('ResultSet', {}).get('Rows', [])

				if not rows:

					break

				startIndex = 0

				if headers is None:

					headers = [col.get('VarCharValue', '') for col in rows[0].get('Data', [])]

					startIndex = 1

				for row in rows[startIndex:]:

					rowData = row.get('Data', [])

					rowDict = {}

					for idx, header in enumerate(headers):

						val = rowData[idx].get('VarCharValue', None) if idx < len(rowData) else None

						rowDict[header] = val

					resultList.append(rowDict)

				nextToken = resultsResponse.get('NextToken')

				if not nextToken:

					break

			return resultList
