# -*- coding: utf-8 -*-

import io
import re
import unicodedata
import pandas
import openpyxl

from urllib.parse import urlparse

from phynfra.atomic.utils import sanitize
from phynfra.aws.s3 import S3
from phynfra.apache.spark import DataFramer, Spark

class Excel ():
	'''
	Excel manipulation and intelligent header detection class
	'''

	def __init__ (self, source, spark = None, s3 = None):
		'''
		source:bytes|io.BytesIO|str - Excel file content, local path, or S3 URI (s3:// or s3a://)
		spark:Spark - Spark manager instance
		s3:S3 - S3 client instance
		'''

		self._spark = spark

		self._s3 = s3

		if isinstance(source, io.BytesIO):

			self._buffer = io.BytesIO(source.getvalue())

		elif isinstance(source, (bytes, bytearray)):

			self._buffer = io.BytesIO(source)

		elif isinstance(source, str):

			if source.startswith(('s3://', 's3a://')):

				if (not s3) or (not isinstance(s3, S3)):

					raise ValueError('An instance of S3 is required to download from S3 URI: %s' % source)

				parsed = urlparse(source)

				bucket = parsed.netloc

				key = parsed.path.lstrip('/')

				content = s3.read(bucket = bucket, keyPrefix = key, decoder = None)

				if not content:

					raise ValueError('Failed to download content from S3: %s' % source)

				self._buffer = io.BytesIO(content)

			else:

				with open(source, 'rb') as fileHandler:

					self._buffer = io.BytesIO(fileHandler.read())

		else:

			raise ValueError('Unsupported source type: %s' % type(source))

		self._buffer.seek(0)

		self._excelFile = pandas.ExcelFile(self._buffer, engine = 'openpyxl')

	def detective (self, expectedColumns, minRatio: float = 0.6, spark = None) -> DataFramer:
		'''
		Scans sheets up to 20 rows to detect the header matching expectedColumns and loads as DataFramer
		expectedColumns:list - Expected column names to identify header
		minRatio:float - Minimum proportion of matched columns (default 0.6)
		spark:Spark - Optional Spark manager instance override
		'''

		if (not expectedColumns) or (not isinstance(expectedColumns, (list, tuple, set))):

			raise ValueError('expectedColumns must be a non-empty list of column names!')

		normalizedExpected = set([sanitize(str(col)) for col in expectedColumns if col and str(col).strip()])

		if len(normalizedExpected) == 0:

			raise ValueError('expectedColumns must contain at least one valid column!')

		totalExpected = len(normalizedExpected)

		bestSheet = None

		bestHeaderRow = None

		bestRatio = 0.0

		for sheetName in self._excelFile.sheet_names:

			self._buffer.seek(0)

			sampleDf = pandas.read_excel(self._buffer, sheet_name = sheetName, header = None, nrows = 20)

			for rowIndex in range(len(sampleDf)):

				rowValues = sampleDf.iloc[rowIndex].dropna().tolist()

				sanitizedRow = set([sanitize(str(val)) for val in rowValues if val is not None and str(val).strip()])

				matched = normalizedExpected.intersection(sanitizedRow)

				ratio = len(matched) / totalExpected

				if (ratio >= minRatio) and (ratio > bestRatio):

					bestRatio = ratio

					bestSheet = sheetName

					bestHeaderRow = rowIndex

					if ratio == 1.0:

						break

			if bestRatio == 1.0:

				break

		if bestSheet is None:

			raise ValueError('No sheet matching at least %s of expected columns was found!' % minRatio)

		self._buffer.seek(0)

		fullDf = pandas.read_excel(self._buffer, sheet_name = bestSheet, header = bestHeaderRow)

		activeSpark = spark if spark else self._spark

		if not activeSpark:

			raise ValueError('A Spark instance is required to convert the Excel sheet into a DataFramer!')

		dataFramer = DataFramer(activeSpark)

		dataFramer.convertFrom(fullDf)

		return dataFramer
