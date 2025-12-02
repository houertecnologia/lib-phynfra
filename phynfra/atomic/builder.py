import os
import sys
import subprocess
import shutil

from importlib import resources
from pathlib import Path
from phynfra.atomic.arguments import RunArguments
from phynfra.atomic.utils import is_string, slug
from phynfra.aws.s3 import S3

ENTRYPOINT = resources.files("phynfra.boilerplate").joinpath("entrypoint.py").read_text()

PYPROJECT = resources.files("phynfra.boilerplate").joinpath("pyproject.toml").read_text()

KEYS = [
	'name',
	'project',
	's3',
	'version'
]

ENVIRONMENT = [
	'AWS_ACCESS_KEY_ID',
	'AWS_SECRET_ACCESS_KEY',
	'AWS_REGION_NAME',
	'AWS_DEFAULT_BUCKET'
]

def get_packages():
	'''
	'''

	if not (getattr(sys, 'base_prefix', sys.prefix) != sys.prefix or getattr(sys, 'real_prefix', None)):

		return None

	for path in sys.path:

		p = Path(path)

		if p.name == 'site-packages' and p.exists():
		
			try:
			
				p.relative_to(sys.prefix)
			
				return p

			except ValueError:

				continue

	return None

def scan (root):
	'''
	root:str - The root folder of a python project
	'''

	packages = []
	modules = []

	if os.path.exists(os.path.join(root, '__init__.py')):

		packages.append('"%s"' % os.path.basename(root))

	for name in os.listdir(root):
	
		path = os.path.join(root, name)

		if os.path.isdir(path) and os.path.isfile(os.path.join(path, "__init__.py")):
			# package = directory containing __init__.py
	
			packages.append('"%s"' % name)

		elif os.path.isfile(path) and name.endswith(".py") and name != "__init__.py":
			# module = .py file (excluding __init__.py)
	
			modules.append('"%s"' % name[:-3])

	return packages, modules

def build(**kwargs:RunArguments):
	'''
	kwargs['settings']: OrderedDict
	kwargs['logger']: phynfra.atomic.logger.Logger
	kwargs['extra']: dict

	extra['project']:str - Project root folder (path)
	extra['s3']:str - S3 Bucket and prefix for this build
	extra['version']:str - Package version

	Build the current application that hosts phynfra with a .whl
	adding pyproject.toml (if necessary) and uploads to s3, rewriting
	if version exists

	python -m phynfra --command run --configuration /path/to/.env --module phynfra.atomic.builder.build --extra name=example-phynfra project=/path/to s3=s3://bucket/prefix version=1.0.0
	'''

	LOGGER = kwargs['logger']

	if not kwargs['extra']:

		LOGGER.error('Cannot build a package without extra arguments')

		return

	allset = all([x in kwargs['extra'] and is_string(kwargs['extra'][x]) for x in KEYS])

	if not allset:

		LOGGER.error('All --extra arguments should informed using command line - %s' % ', '.join(KEYS))

		return

	allset = all([x in kwargs['settings'] and is_string(kwargs['settings'][x]) for x in ENVIRONMENT])

	if not allset:

		LOGGER.error('All AWS variables to upload to S3 should be present in settings - %s' % ', '.join(ENVIRONMENT))

		return

	if shutil.which('zip') is None:

		LOGGER.error('Cannot use phynfra build command. You must have a zip command in PATH to be called.')

		return

	root = Path(kwargs['extra']['project'])
	
	targetfile = root / "pyproject.toml"

	if not targetfile.exists():
		
		pipoutput = subprocess.check_output(["pip", "freeze"]).decode().strip().split("\n")

		dependencies = []
		
		for line in pipoutput:
		
			if "@" in line: # skip vcs installs
			
				continue
		
			if line.startswith("-"):
		
				continue
		
			dependencies.append('"%s"' % line)

		packages, modules = scan(kwargs['extra']['project'])

		pyproject = str(PYPROJECT).replace('{{version}}', kwargs['extra']['version']) 
		pyproject = pyproject.replace('{{name}}', kwargs['extra']['name'])
		pyproject = pyproject.replace('{{dependencies}}', ', '.join(dependencies))
		pyproject = pyproject.replace('{{modules}}', ', '.join(modules))
		
		targetfile.write_text(pyproject)

		#pyproject = pyproject.replace('{{packages}}', ', '.join(packages))
		#subprocess.check_call(['git', 'add', str(targetfile)])
		#subprocess.check_call(['git', 'commit', '-m', 'pyproject.toml added by phynfra to allow building the project'])

	targetfile = root / "entrypoint.py"

	entrypoint = None

	if not targetfile.exists():
		
		entrypoint = str(ENTRYPOINT)
		
		targetfile.write_text(entrypoint)

		#subprocess.check_call(['git', 'add', str(targetfile)])
		#subprocess.check_call(['git', 'commit', '-m', 'entrypoint.py added by phynfra to be the entrypoint for Spark / EMRServerless'])

	else:

		handler = open(str(targetfile), 'r', encoding = 'utf-8')

		entrypoint = handler.read()

		handler.close()

	dist = root / "dist"
	
	if not dist.exists():
		
		dist.mkdir()

	subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'build'])
	subprocess.check_call([sys.executable, '-m', 'build'], cwd = root)

	wheel = None

	handler = open(next(dist.glob("*.whl")), 'rb')

	wheel = handler.read()

	handler.close()

	client = S3(kwargs['settings']['AWS_DEFAULT_REGION'], kwargs['settings']['AWS_ACCESS_KEY_ID'], kwargs['settings']['AWS_SECRET_ACCESS_KEY'])

	sluggedname = slug(kwargs['extra']['name'])

	outputwheel = client.write(bucket = kwargs['settings']['AWS_DEFAULT_BUCKET'], payload = wheel, public = False, contentType = 'application/x-wheel+zip', keyPrefix = '%s/%s-%s-py3-none-any.whl' % (
		sluggedname,
		sluggedname,
		kwargs['extra']['version']		 
	))

	outputentrypoint = client.write(bucket = kwargs['settings']['AWS_DEFAULT_BUCKET'], payload = entrypoint, public = False, contentType = 'text/x-python', keyPrefix = '%s/entrypoint.py' % (
		sluggedname
	))

	# Zipping...

	zipfile = str(root / dist / '%s-%s-py3.zip') % (sluggedname, kwargs['extra']['version'])

	subprocess.run(["zip", "-r9", zipfile, "."], cwd = get_packages(), check = True)

	handler = open(zipfile, 'rb')

	zipbytes = handler.read()

	handler.close()

	outputzipfile = client.write(bucket = kwargs['settings']['AWS_DEFAULT_BUCKET'], payload = zipbytes, public = False, contentType = 'application/zip', keyPrefix = '%s/%s-%s-py3.zip' % (
		sluggedname,
		sluggedname,
		kwargs['extra']['version']		 
	))

	# Printing....

	print(outputwheel)
	print(outputentrypoint)
	print(outputzipfile)
