FROM mcr.microsoft.com/vscode/devcontainers/python:3.10-buster
USER vscode
RUN curl -fsSL https://pixi.sh/install.sh | bash
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH=$PATH:/home/vscode/.cargo/bin
RUN uv venv ~/.local
ENV VIRTUAL_ENV=~/.local
ENV PATH=$VIRTUAL_ENV/bin:$PATH
USER root