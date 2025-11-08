# -*- coding: utf-8 -*-

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
