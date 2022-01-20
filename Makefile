install:
	@echo "Installing main dependencies..."
	@pip install geopandas google-cloud-bigquery

install-dev: install
	@echo "Installing development dependencies..."
	@pip install black flake8 isort

fmt:
	@black .
	@isort .
