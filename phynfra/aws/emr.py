# -*- coding: utf-8 -*-

import boto3
import json

from botocore.config import Config
from phynfra.atomic.utils import is_not_string

class Serverless:
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

			self.client = boto3.client('emr-serverless', region_name = region, aws_access_key_id = accessKey, aws_secret_access_key = secretKey)

		except Exception as botoError:

			raise ValueError('Internal error initializating EMRServerless Boto Client') from botoError

	def run (self, applicationid = None, arn = None, entrypoint = None, files = None, commandline = None, sparksettings = None):
		'''
		applicationid:str - EMR Serverless Application
		arn:str - EMR execution role ARN
		entrypoint:str - s3://path/to/entrypoint/plain/python.py
		files:str - s3://path/to/package/wheel.whl
		commandline:dict - key,value of --argument=value (command line arguments)
		sparksettings:dict - key:value of spark settings
		An example:
		response = run (
			applicationid = "app-1234567890",
			arn = "arn:aws:iam::111111111111:role/emr-serverless-exec-role",
			entrypoint = "s3://my-artifacts/jobs/main_etl.py",
			files = "s3://my-artifacts/wheels/project-0.1.0-py3-none-any.whl",
			commandline = ["key1", "value1", "key2", "value2"],
			sparksettings = {
				"spark.executor.memory": "8g",
				"spark.driver.memory": "8g",
				"spark.sql.shuffle.partitions": "8"
			}
		)
		'''

		if is_not_string(applicationid):

			raise ValueError('EMR Application ID should be informed')

		if is_not_string(arn):

			raise ValueError('Execution role ARN should be informed')

		if is_not_string(entrypoint):

			raise ValueError('Entrypoint python file in s3:// should be informed')

		if is_not_string(files):

			raise ValueError('Python package in s3:// should be informed')

		settings = ""

		for key, value in sparksettings.items():

			settings = settings + '--conf %s=%s ' % (key, value)
		
		settings = settings + '--py-files=%s' % files

		arguments = []

		for key, value in commandline.items():

			arguments = arguments + [key, value]

		response = self.client.start_job_run(
			applicationId = applicationid,
			executionRoleArn = arn,
			jobDriver = {
				"sparkSubmit": {
					"entryPoint": entrypoint,
					"entryPointArguments": arguments,
					"sparkSubmitParameters": settings
				}
			}
			# configurationOverrides={
			# 	"monitoringConfiguration": {
			# 		"s3MonitoringConfiguration": {
			# 			"logUri": f"s3://my-logs-bucket/emr/"
			# 		}
			# 	}
			# }
		)

		return response["jobRunId"]
