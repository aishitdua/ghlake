HOUR ?= $(shell date -u -v-1d +%Y-%m-%dT12 2>/dev/null || date -u -d yesterday +%Y-%m-%dT12)
DBT = uv run dbt --no-use-colors
DBT_DIRS = --project-dir dbt --profiles-dir dbt

.PHONY: demo check

demo:
	uv sync
	uv run python -m ghlake.ingest hour $(HOUR)
	$(DBT) build $(DBT_DIRS)
	uv run python -m ghlake.publish export
	@echo "dashboard: http://localhost:8000/app/?gold=/data/gold/"
	uv run python -m http.server 8000

check:
	uv run ruff check . && uv run ruff format --check .
	uv run pytest -q
