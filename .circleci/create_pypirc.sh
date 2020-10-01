#!/bin/bash
PYPI_TOKEN=${PYPI_TOKEN:-""}
TESTPYPI_TOKEN=${TESTPYPI_TOKEN:-""}

echo -e "[pypi]" >> ~/.pypirc
echo -e "username = __token__" >> ~/.pypirc
echo -e "password = $PYPI_TOKEN" >> ~/.pypirc
echo -e "[testpypi]" >> ~/.pypirc
echo -e "username = __token__" >> ~/.pypirc
echo -e "password = $TESTPYPI_TOKEN" >> ~/.pypirc