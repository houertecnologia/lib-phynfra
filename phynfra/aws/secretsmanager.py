# -*- coding: utf-8 -*-

import boto3

from botocore.config import Config

class SecretsManager:
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

			self.client = boto3.client('secretsmanager', region_name = region, aws_access_key_id = accessKey, aws_secret_access_key = secretKey, config = Config(signature_version = 's3v4'))

		except Exception as botoError:

			raise ValueError('Internal error initializating S3 Boto Client') from botoError

	def read (self, secret = None):
		'''
		secret:str - The key
		'''

		if not isinstance(secret, str) or secret == '':

			raise ValueError('Secret must be a valid AWS Secrets Manager key')

		try:

			response = self.client.get_secret_value(SecretId = secret)

			if response and ('SecretString' in response):

				return response['SecretString']

			else:

				return None

		except Exception as getSecretError:

			raise ValueError('Internal error when fetching "%s" from Secrets Manager' % (secret)) from getSecretError

	def create (self, secret = None, value = None):
		'''
		secret:str - The key
		'''

		if not isinstance(secret, str) or secret == '':

			raise ValueError('Secret must be a valid AWS Secrets Manager key')

		if not isinstance(value, str) or value == '':

			raise ValueError('Value must be string')

		try:

			response = self.client.create_secret(Name = secret, SecretString = value)

			return self

		except Exception as createSecretError:

			raise ValueError('Internal error when creating "%s" in Secrets Manager' % (secret)) from createSecretError

	def update (self, secret = None, value = None):
		'''
		secret:str - The key
		'''

		if not isinstance(secret, str) or secret == '':

			raise ValueError('Secret must be a valid AWS Secrets Manager key')

		if not isinstance(value, str) or value == '':

			raise ValueError('Value must be string')

		try:

			response = self.client.put_secret_value(SecretId = secret, SecretString = value)

			return self

		except Exception as updateSecretError:

			raise ValueError('Internal error when creating "%s" in Secrets Manager' % (secret)) from updateSecretError
