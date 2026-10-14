# -*- coding: utf-8 -*-

import os
import io
import tempfile

from ftplib import FTP, FTP_TLS
from phynfra.aws.s3 import S3

class Ftp ():
	'''
	FTP and FTPS operations and synchronization manager
	'''

	def __init__ (self, host: str, user: str = None, password: str = None, port: int = 21, secure: bool = False, timeout: int = 60):
		'''
		host:str - FTP server hostname
		user:str - FTP username
		password:str - FTP password
		port:int - FTP port (default 21)
		secure:bool - Uses FTP_TLS when True
		timeout:int - Connection timeout in seconds (default 60)
		'''

		if (not host) or (not isinstance(host, str)) or (not host.strip()):

			raise ValueError('host must be a non-empty string!')

		try:

			if secure:

				self._client = FTP_TLS(timeout = timeout)

			else:

				self._client = FTP(timeout = timeout)

			self._client.connect(host = host.strip(), port = port, timeout = timeout)

			if user and password:

				self._client.login(user = user, passwd = password)

			elif user:

				self._client.login(user = user)

			else:

				self._client.login()

			if secure and isinstance(self._client, FTP_TLS):

				self._client.prot_p()

		except Exception as connError:

			raise ConnectionError('Failed to connect or authenticate to FTP host %s:%s' % (host, port)) from connError

	def download (self, remotePath: str, localPath: str = None) -> str:
		'''
		Downloads a remote FTP file using retrbinary
		remotePath:str - Path to remote file on FTP server
		localPath:str - Local destination path (generates NamedTemporaryFile if None)
		'''

		if (not remotePath) or (not isinstance(remotePath, str)) or (not remotePath.strip()):

			raise ValueError('remotePath must be a non-empty string!')

		try:

			if localPath:

				targetPath = os.path.abspath(localPath)

				with open(targetPath, 'wb') as fileHandler:

					self._client.retrbinary('RETR %s' % remotePath.strip(), fileHandler.write)

				return targetPath

			else:

				filename = os.path.basename(remotePath.strip())

				suffix = os.path.splitext(filename)[1]

				with tempfile.NamedTemporaryFile(delete = False, suffix = suffix) as tempHandler:

					targetPath = os.path.abspath(tempHandler.name)

					self._client.retrbinary('RETR %s' % remotePath.strip(), tempHandler.write)

				return targetPath

		except Exception as downloadError:

			raise RuntimeError('Failed to download %s from FTP' % remotePath) from downloadError

	def sync (self, remotePath: str, bucket: str, keyPrefix: str = '', s3: S3 = None) -> list:
		'''
		Synchronizes files from remote FTP directory to AWS S3 bucket, uploading only new or modified files
		remotePath:str - Remote FTP directory path
		bucket:str - Target AWS S3 Bucket
		keyPrefix:str - Key prefix inside S3 bucket (default empty)
		s3:S3 - Instance of phynfra.aws.s3.S3
		'''

		if (not remotePath) or (not isinstance(remotePath, str)) or (not remotePath.strip()):

			raise ValueError('remotePath must be a non-empty string!')

		if (not bucket) or (not isinstance(bucket, str)) or (not bucket.strip()):

			raise ValueError('bucket must be a non-empty string!')

		if (not s3) or (not isinstance(s3, S3)):

			raise ValueError('An instance of phynfra.aws.s3.S3 is required for sync!')

		try:

			prefix = keyPrefix.strip('/') + '/' if keyPrefix and keyPrefix.strip('/') else ''

			s3Objects = s3.scan(bucket = bucket.strip(), keyPrefix = prefix)

			existingS3Files = {obj['Key']: obj['Size'] for obj in s3Objects if 'Key' in obj and 'Size' in obj}

			cleanRemoteDir = remotePath.strip().rstrip('/')

			remoteFiles = []

			try:

				for entryName, facts in self._client.mlsd(cleanRemoteDir):

					if facts.get('type') == 'file':

						filePath = '%s/%s' % (cleanRemoteDir, entryName)

						fileSize = int(facts.get('size', -1))

						remoteFiles.append((entryName, filePath, fileSize))

			except Exception:

				fileList = self._client.nlst(cleanRemoteDir)

				for item in fileList:

					entryName = os.path.basename(item)

					filePath = item if item.startswith(cleanRemoteDir) else '%s/%s' % (cleanRemoteDir, item)

					try:

						fileSize = self._client.size(filePath)

					except Exception:

						fileSize = -1

					remoteFiles.append((entryName, filePath, fileSize))

			syncedFiles = []

			for (entryName, filePath, fileSize) in remoteFiles:

				s3Key = '%s%s' % (prefix, entryName)

				s3Size = existingS3Files.get(s3Key)

				# Skip if already exists in S3 with same file size
				if (s3Size is not None) and (fileSize != -1) and (s3Size == fileSize):

					continue

				buffer = io.BytesIO()

				self._client.retrbinary('RETR %s' % filePath, buffer.write)

				payloadBytes = buffer.getvalue()

				s3.write(bucket = bucket.strip(), payload = payloadBytes, keyPrefix = s3Key)

				syncedFiles.append(entryName)

			return syncedFiles

		except Exception as syncError:

			raise RuntimeError('Internal error during FTP to S3 synchronization from %s' % remotePath) from syncError

	def close (self) -> None:
		'''
		Safely closes the FTP client connection
		'''

		try:

			if hasattr(self, '_client') and self._client:

				self._client.quit()

		except Exception:

			try:

				self._client.close()

			except Exception:

				pass
