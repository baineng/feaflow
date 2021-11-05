ROOT_DIR 	:= $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

format:
	bin/format.sh

lint:
	mypy feaflow/ tests/
	isort feaflow/ tests/ --check-only
	flake8 feaflow/ tests/
	black --check feaflow tests