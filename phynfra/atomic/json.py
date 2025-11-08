# -*- coding: utf-8 -*-

import json

def recursive_object_flatten (source = None, parent = '', separator = '.'):
	'''
	Inner function
	'''

	output = {}

	if isinstance(source, str) or isinstance(source, int) or isinstance(source, float) or source is None:

		output[parent] = source

	else:

		for (key, value) in source.items():

			composedkey = f"{parent}{separator}{key}" if parent else key

			if isinstance(value, dict):

				output.update(recursive_object_flatten(value, composedkey, separator = separator))

			elif isinstance(value, list):

				for (i, item) in enumerate(value):

					output.update(recursive_object_flatten(item, f"{composedkey}{separator}{i}", separator = separator))

			else:

				output[composedkey] = value

	return output

def flatten (source = None, parent = '', separator = '.'):
	'''
	source:str || dict
	parent:str - The parent key
	separator:str - To join columns name
	recursive function :: return a flatterned JSON
	'''

	data = None

	if isinstance(source, str):

		try:

			data = json.loads(source, ensure_ascii = False)

		except Exception as parseError:

			raise ValueError('JSON string source was not parseable') from parseError

	else:

		data = source

		if isinstance(data, dict):

			return recursive_object_flatten(data, parent = parent, separator = separator)

		elif isinstance(data, list):

			return [recursive_object_flatten(r, parent = parent, separator = separator) for r in data]
