# -*- coding: utf-8 -*-

from typing import TypedDict, Any

class RunArguments (TypedDict, total = False):
	'''
	settings: dict
	logger: phynfra.logger.Logger
	'''

	settings:Any
	logger: Any
