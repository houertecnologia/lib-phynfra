# -*- coding: utf-8 -*-

import json

from phynfra.atomic.http import Fetcher
from phynfra.apache.spark import DataFramer, Spark
from pyspark.sql.types import StructType, StructField, StringType

class Sheets ():
	'''
	Google Sheets client for spreadsheet reading and ingestion
	'''

	def __init__ (self, accessToken: str):
		'''
		accessToken:str - OAuth2 Bearer Access Token for Google Sheets API
		'''

		if (not accessToken) or (not isinstance(accessToken, str)) or (not accessToken.strip()):

			raise ValueError('accessToken must be a non-empty string!')

		self.accessToken = accessToken.strip()

	def read (self, sheetId: str, sheetRange: str = 'A1:ZZ', asDataFramer: bool = True, spark = None):
		'''
		Reads a spreadsheet range from Google Sheets API v4
		sheetId:str - Google Sheets ID
		sheetRange:str - A1 notation range (default 'A1:ZZ')
		asDataFramer:bool - Whether to return results as DataFramer or list of dicts (default True)
		spark:Spark - Spark manager instance (required if asDataFramer is True and no active session)
		'''

		if (not sheetId) or (not isinstance(sheetId, str)) or (not sheetId.strip()):

			raise ValueError('sheetId must be a non-empty string!')

		if (not sheetRange) or (not isinstance(sheetRange, str)) or (not sheetRange.strip()):

			raise ValueError('sheetRange must be a non-empty string!')

		endpoint = f'https://sheets.googleapis.com/v4/spreadsheets/{sheetId.strip()}/values/{sheetRange.strip()}'

		headers = {
			'Authorization': f'Bearer {self.accessToken}'
		}

		try:

			fetcher = Fetcher(endpoint)

			response = fetcher.fetch('GET', headers = headers)

		except Exception as fetchError:

			raise ValueError('Internal error fetching data from Google Sheets API') from fetchError

		if (not response) or (not response.get('ok')):

			statusCode = response.get('status') if response else 'Unknown'

			errorBody = response.get('response') if response else 'No response body'

			raise RuntimeError('Google Sheets API request failed with status %s: %s' % (statusCode, errorBody))

		try:

			payload = json.loads(response['response'])

			values = payload.get('values', [])

		except Exception as parseError:

			raise ValueError('Failed to parse JSON response from Google Sheets API') from parseError

		if len(values) == 0:

			headers = []

			records = []

		else:

			headers = [str(col).strip() for col in values[0]]

			records = []

			for row in values[1:]:

				rowDict = {}

				for idx, header in enumerate(headers):

					val = row[idx] if idx < len(row) else None

					rowDict[header] = val

				records.append(rowDict)

		if not asDataFramer:

			return records

		if isinstance(spark, Spark):

			sparkInstance = spark

			sparkSession = spark.get()

		elif hasattr(spark, 'createDataFrame'):

			sparkSession = spark

			sparkInstance = None

		else:

			sparkInstance = Spark('google_sheets_reader')

			sparkSession = sparkInstance.get()

		try:

			if len(records) > 0:

				sparkDf = sparkSession.createDataFrame(records)

			else:

				schema = StructType([StructField(h, StringType(), True) for h in headers]) if headers else StructType([])

				sparkDf = sparkSession.createDataFrame([], schema = schema)

			if sparkInstance:

				dataFramer = DataFramer(sparkInstance)

			else:

				dataFramer = DataFramer.__new__(DataFramer)

				dataFramer._spark = sparkSession

			dataFramer.dataframe = sparkDf

			return dataFramer

		except Exception as sparkError:

			raise RuntimeError('Failed to convert Google Sheets records to Spark DataFrame') from sparkError
