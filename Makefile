compile:
	uv pip compile pyproject.toml --output-file requirements.lock
sync:
	uv pip sync requirements.lock