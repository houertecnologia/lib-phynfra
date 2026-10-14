# -*- coding: utf-8 -*-

import json

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

class Slack ():
	'''
	'''

	def __init__ (self, token = None):
		'''
		'''

		self.__token = token

		self.__client = WebClient(token = self.__token)

	def notify (self, channel = None, message = None):
		'''
		channel:str
		message:str
		'''

		try:
		
			response = self.__client.chat_postMessage(channel = channel, text = message)

			return response

		except Exception as postError:

			raise RuntimeError("Fail to post message to channel in Slack") from postError

	def card (self, channel: str, title: str, status: str = 'info', details: dict = None, buttonText: str = None, buttonUrl: str = None):
		'''
		Sends a structured Slack message using Block Kit
		channel:str - Slack channel ID or name
		title:str - Message title
		status:str - Status type ('success', 'error', 'warning', 'info')
		details:dict - Dictionary of field labels and values
		buttonText:str - Button label text
		buttonUrl:str - Target redirect URL for the button
		'''

		if (not channel) or (not isinstance(channel, str)) or (not channel.strip()):

			raise ValueError('channel must be a non-empty string!')

		if (not title) or (not isinstance(title, str)) or (not title.strip()):

			raise ValueError('title must be a non-empty string!')

		statusIcons = {
			'success': '🟢',
			'error': '🔴',
			'warning': '🟡',
			'info': 'ℹ️'
		}

		currentStatus = status.lower().strip() if status else 'info'

		statusIcon = statusIcons.get(currentStatus, statusIcons['info'])

		blocks = [
			{
				'type': 'header',
				'text': {
					'type': 'plain_text',
					'text': f'{statusIcon} {title.strip()}',
					'emoji': True
				}
			}
		]

		if isinstance(details, dict) and (len(details) > 0):

			fields = [
				{
					'type': 'mrkdwn',
					'text': f'*{k}:*\n{v}'
				}
				for k, v in details.items()
			]

			blocks.append({
				'type': 'section',
				'fields': fields
			})

		if buttonText and buttonUrl:

			buttonElement = {
				'type': 'button',
				'text': {
					'type': 'plain_text',
					'text': str(buttonText).strip(),
					'emoji': True
				},
				'url': str(buttonUrl).strip()
			}

			if currentStatus == 'success':

				buttonElement['style'] = 'primary'

			elif currentStatus == 'error':

				buttonElement['style'] = 'danger'

			blocks.append({
				'type': 'actions',
				'elements': [buttonElement]
			})

		try:

			response = self.__client.chat_postMessage(channel = channel.strip(), text = title.strip(), blocks = blocks)

			return response

		except SlackApiError as apiError:

			errorMessage = apiError.response.get('error', 'Unknown Slack API Error') if hasattr(apiError, 'response') else str(apiError)

			raise RuntimeError('Slack API Error: %s' % errorMessage) from apiError

		except Exception as error:

			raise RuntimeError('Failed to send Slack card message: %s' % error) from error
