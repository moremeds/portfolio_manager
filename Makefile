SHELL := /bin/zsh
.PHONY: install test lint typecheck run run-dry run-no-email

install:
	source .venv/bin/activate && pip install -e ".[dev]"

test:
	source .venv/bin/activate && pytest

lint:
	source .venv/bin/activate && ruff check src/ tests/

format:
	source .venv/bin/activate && ruff format src/ tests/

typecheck:
	source .venv/bin/activate && mypy src/portfolio_manager/

run:
	source ~/.zshrc && source .venv/bin/activate && python -m portfolio_manager.main --verbose

run-dry:
	source ~/.zshrc && source .venv/bin/activate && python -m portfolio_manager.main --dry-run --verbose

run-no-email:
	source ~/.zshrc && source .venv/bin/activate && python -m portfolio_manager.main --no-email --verbose
