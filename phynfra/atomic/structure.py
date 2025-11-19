import os
import os
import yaml

from importlib import resources
from phynfra.atomic.arguments import RunArguments

DIRECTORY = resources.files("phynfra.boilerplate").joinpath("directory.yaml").read_text()

def recursive_create_directory_from_yaml (root = None, data = None):
	'''
	'''

	for name, node in data.items():

		if name == "type":

			continue

		path = os.path.join(root, name)
		
		nodetype = node.get("type")

		if nodetype == "folder":
			
			os.makedirs(path, exist_ok = True)
			
			recursive_create_directory_from_yaml(path, node)

		elif nodetype == "file":

			os.makedirs(os.path.dirname(path), exist_ok = True)
			
			content = node.get("content") or ""
			
			content = content.replace("    ", "\t")
			
			with open(path, "w") as f:
			
				f.write(content)

def create(**kwargs:RunArguments):
	'''
	kwargs['settings']: OrderedDict
	kwargs['logger']: phynfra.atomic.logger.Logger
	kwargs['extra']: dict

	extra['project']:str - Root Folder

	Creates the phynfra directory structure

	python -m phynfra --command run --configuration /path/to/.env --module phynfra.atomic.structure.create
	'''

	data = yaml.safe_load(DIRECTORY)

	root = kwargs['extra']['project']

	recursive_create_directory_from_yaml(root, data)

	kwargs['logger'].info('Phynfra directories created')
	