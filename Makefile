ROOT_DIR 	:= $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

format:
	# Sort
	cd ${ROOT_DIR}; isort feaflow tests setup.py

	# Format
	cd ${ROOT_DIR}; black --target-version py37 feaflow tests setup.py

lint:
	cd ${ROOT_DIR}; mypy feaflow/ tests/
	cd ${ROOT_DIR}; isort feaflow/ tests/ --check-only
	cd ${ROOT_DIR}; flake8 feaflow/ tests/
	cd ${ROOT_DIR}; black --check feaflow tests