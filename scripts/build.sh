#!/bin/bash

cd ..

rm -fr venv
rm -fr phynfra.egg-info
rm poetry.lock
rm ./dist/*.whl
rm ./dist/*.gz

python3 -m venv venv

source venv/bin/activate

pip install poetry
poetry self add poetry-plugin-export
poetry export -f requirements.txt --output requirements.txt --without-hashes
poetry install
poetry build

#git push origin master
