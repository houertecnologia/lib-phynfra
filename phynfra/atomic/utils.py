# -*- coding: utf-8 -*-

import re
import unicodedata

def is_string (value):
	'''
	'''

	return isinstance(value, str) and value != '' and value != None

def is_not_string (value):
	'''
	'''

	return (not isinstance(value, str)) or value == '' or value == None

def is_list (value):
	'''
	'''

	return isinstance(value, list) and len(value) > 0

def is_not_list (value):
	'''
	'''

	return (not isinstance(value, list)) or len(value) == 0

def slug(value):
	'''
	'''
	
	s = unicodedata.normalize("NFKD", value)
	s = s.encode("ascii", "ignore").decode("ascii")
	s = re.sub(r"[^A-Za-z0-9\s-]", "", s)
	s = s.strip().lower()
	s = re.sub(r"[\s_]+", "-", s)
	s = re.sub(r"-{2,}", "-", s)
	
	return s.strip("-")

def sanitize (value: str) -> str:
	'''
	Converts text to ASCII, removes accents, replaces non-alphanumeric chars with underscore,
	forces lowercase, collapses multiple underscores and trims edges
	'''

	if not value:

		return ''

	s = unicodedata.normalize('NFKD', str(value))

	s = s.encode('ascii', 'ignore').decode('ascii')

	s = s.lower()

	s = re.sub(r'[^a-z0-9]+', '_', s)

	s = re.sub(r'_+', '_', s)

	return s.strip('_')
