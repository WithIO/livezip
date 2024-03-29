PYTHON_BIN ?= python

format: isort black

black:
	'$(PYTHON_BIN)' -m black --target-version py38 --exclude '/(\.git|\.hg|\.mypy_cache|\.nox|\.tox|\.venv|_build|buck-out|build|dist|node_modules|webpack_bundles)/' .

isort:
	'$(PYTHON_BIN)' -m isort -rc src

build:
	poetry install -E api -E dates
	cd docs && poetry run make html
	poetry run pip freeze | grep -v typefit > requirements.txt
