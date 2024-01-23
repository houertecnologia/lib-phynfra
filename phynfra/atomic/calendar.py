# -*- coding: utf-8 -*-

import pytz

from datetime import datetime, timezone

def now (string = True, zone = None):
	'''
	'''

	tz = timezone.utc

	if isinstance(zone, str):

		if zone != '':

			tz = pytz.timezone(zone)

	output = datetime.now(tz)

	return output if not string else output.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
