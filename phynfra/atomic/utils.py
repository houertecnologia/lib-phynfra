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
