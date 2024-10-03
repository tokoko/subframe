compile:
	uv pip compile pyproject.toml --output-file requirements.lock
sync:
	uv pip sync requirements.lock
venv: 
	uv venv /home/vscode/venv

antlr:
	java -jar ~/lib/antlr-4.13.1-complete.jar -o subframe/gen -Dlanguage=Python3 SubstraitType.g4

#  source /home/vscode/venv/bin/activate