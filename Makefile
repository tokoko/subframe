compile:
	uv pip compile pyproject.toml --output-file requirements.lock
sync:
	uv pip sync requirements.lock
venv: 
	uv venv /home/vscode/venv

#  source /home/vscode/venv/bin/activate