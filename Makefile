.PHONY: install lint test

install:
	uv sync

lint:
	uv run ruff check --select I --fix
	uv run ruff format coredis_utils/ tests/
	uv run ruff check coredis_utils/ tests/
	uv run pyright coredis_utils/ tests/

test:
	uv run pytest -v tests/
