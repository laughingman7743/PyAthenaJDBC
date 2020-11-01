.PHONY: fmt
fmt:
	poetry run isort -rc .
	poetry run black .

.PHONY: chk
chk:
	poetry run isort -c -rc .
	poetry run black --check --diff .
