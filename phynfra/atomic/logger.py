import logging
import sys
#import watchtower
import boto3

from uuid_extensions import uuid7str
from logging.handlers import TimedRotatingFileHandler

def record_factory(*args, **kwargs):
	'''
	'''

	global DEFAULT_FACTORY

	record = DEFAULT_FACTORY(*args, **kwargs)

	setattr(record, 'id', uuid7str())

	return record

class ColorFormatter (logging.Formatter):
	'''
	'''

	def format (self, record):

		global COLORS

		color = COLORS.get(record.levelname.upper(), 'DEBUG')

		record.levelname = '%s%s%s' % (
			color,
			record.levelname,
			COLORS.get('RESET')
		)

		return super().format(record)

DEFAULT_FACTORY = logging.getLogRecordFactory()

COLORS = {
	'DEBUG': '\033[36m',   # Cyan
	'INFO': '\033[32m',    # Green
	'WARNING': '\033[33m', # Yellow
	'ERROR': '\033[31m',   # Red
	'RESET': '\033[0m'
}

NAME = 'phynfra'

FORMAT = '🐍 %(asctime)s %(name)s [%(levelname)s] :: id=%(id)s message=%(message)s'

FORMATTER = logging.Formatter (
	FORMAT,
	datefmt = '%Y-%m-%d %H:%M:%S'
)

class Logger:
	'''
	'''

	# TODO add watchtower and clodwatch

	def __init__ (self):
		'''
		'''

		logging.setLogRecordFactory(record_factory)

		self.logger = logging.getLogger(NAME)
		self.logger.setLevel(logging.DEBUG)

		self.stdout = logging.StreamHandler(sys.stdout)
		self.stdout.setFormatter(ColorFormatter(FORMAT, datefmt = '%Y-%m-%d %H:%M:%S'))
		self.stdout.setLevel(logging.DEBUG)

		#self.file = logging.FileHandler('%s.log' % NAME)
		#self.file.setFormatter(FORMATTER)
		#self.file.setLevel(logging.DEBUG)

		self.file = TimedRotatingFileHandler (
			filename = '%s.log' % NAME,
			when = 'midnight',
			interval = 1,
			backupCount = 30,
			encoding = 'utf-8',
			utc = True
		)

		self.file.setFormatter(FORMATTER)
		self.file.setLevel(logging.DEBUG)

		self.logger.addHandler(self.stdout)
		self.logger.addHandler(self.file)
		# self.logger.addHandler(self.watchtower)

	def info (self, message):
		'''
		'''

		self.logger.info(message)

	def debug (self, message):
		'''
		'''

		self.logger.debug(message)

	def error (self, message):
		'''
		'''

		self.logger.error(message)

	def warning (self, message):
		'''
		'''

		self.logger.warning(message)
