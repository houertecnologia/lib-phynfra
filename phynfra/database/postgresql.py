# -*- coding: utf-8 -*-

from phynfra.apache.spark import DataFramer, Spark
from pyspark.sql.types import StructType, StructField, StringType

class PostgreSQL ():
	'''
	PostgreSQL connection and query manager
	'''

	def __init__ (self, host: str, database: str, user: str, password: str, port: int = 5432):
		'''
		host:str - Database host
		database:str - Database name
		user:str - Database user
		password:str - Database password
		port:int - Database port (default 5432)
		'''

		if (not host) or (not isinstance(host, str)):

			raise ValueError('host must be a non-empty string!')

		if (not database) or (not isinstance(database, str)):

			raise ValueError('database must be a non-empty string!')

		if (not user) or (not isinstance(user, str)):

			raise ValueError('user must be a non-empty string!')

		self.host = host.strip()

		self.database = database.strip()

		self.user = user.strip()

		self.password = str(password) if password is not None else ''

		self.port = int(port)

		self.connection = None

	def connect (self):
		'''
		Establishes connection to PostgreSQL database
		'''

		try:

			import psycopg2

			self.connection = psycopg2.connect(
				host = self.host,
				database = self.database,
				user = self.user,
				password = self.password,
				port = self.port
			)

		except ImportError:

			try:

				import psycopg

				self.connection = psycopg.connect(
					host = self.host,
					dbname = self.database,
					user = self.user,
					password = self.password,
					port = self.port
				)

			except ImportError as driverError:

				raise ImportError('psycopg2 or psycopg driver is required to connect to PostgreSQL') from driverError

		except Exception as connError:

			raise ConnectionError('Failed to connect to PostgreSQL at %s:%s/%s' % (self.host, self.port, self.database)) from connError

	def open (self):
		'''
		Opens and returns a new database cursor, ensuring active connection
		'''

		if (self.connection is None) or getattr(self.connection, 'closed', False):

			self.connect()

		return self.connection.cursor()

	def close (self) -> None:
		'''
		Closes the database connection safely
		'''

		try:

			if self.connection and (not getattr(self.connection, 'closed', False)):

				self.connection.close()

		except Exception:

			pass

	def query (self, sql: str, params = None, asDataFramer: bool = False, spark = None):
		'''
		Executes SQL query on PostgreSQL with robust cursor management and DataFramer support
		sql:str - SQL statement to execute
		params:tuple|list|dict - Optional query parameters
		asDataFramer:bool - Return as DataFramer if True (default False)
		spark:Spark - Spark manager or session instance
		'''

		if (not sql) or (not isinstance(sql, str)) or (not sql.strip()):

			raise ValueError('SQL statement must be a non-empty string!')

		if (self.connection is None) or getattr(self.connection, 'closed', False):

			self.connect()

		cursor = None

		try:

			cursor = self.open()

			if params is not None:

				cursor.execute(sql.strip(), params)

			else:

				cursor.execute(sql.strip())

			if cursor.description is None:

				if hasattr(self.connection, 'commit'):

					self.connection.commit()

				return cursor.rowcount

			columns = [col[0] for col in cursor.description]

			rawRows = cursor.fetchall()

			records = [dict(zip(columns, row)) for row in rawRows]

			if not asDataFramer:

				return records

			if isinstance(spark, Spark):

				sparkInstance = spark

				sparkSession = spark.get()

			elif hasattr(spark, 'createDataFrame'):

				sparkSession = spark

				sparkInstance = None

			else:

				sparkInstance = Spark('postgresql_query')

				sparkSession = sparkInstance.get()

			if len(records) > 0:

				sparkDf = sparkSession.createDataFrame(records)

			else:

				schema = StructType([StructField(c, StringType(), True) for c in columns]) if columns else StructType([])

				sparkDf = sparkSession.createDataFrame([], schema = schema)

			if sparkInstance:

				dataFramer = DataFramer(sparkInstance)

			else:

				dataFramer = DataFramer.__new__(DataFramer)

				dataFramer._spark = sparkSession

			dataFramer.dataframe = sparkDf

			return dataFramer

		except Exception as error:

			raise RuntimeError('Failed to execute PostgreSQL query: %s. Error: %s' % (sql, error)) from error

		finally:

			if cursor is not None:

				try:

					cursor.close()

				except Exception:

					pass
