FROM mcr.microsoft.com/vscode/devcontainers/python:3.10-buster
USER vscode
RUN curl -fsSL https://pixi.sh/install.sh | bash
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH=$PATH:/home/vscode/.cargo/bin
# RUN uv. venv ~/.local
# ENV VIRTUAL_ENV=~/.local
# ENV PATH=$VIRTUAL_ENV/bin:$PATH
RUN curl -s "https://get.sdkman.io" | bash
SHELL ["/bin/bash", "-c"]
RUN source "/home/vscode/.sdkman/bin/sdkman-init.sh" && sdk install java 20.0.2-graalce
RUN mkdir -p ~/lib && cd ~/lib && curl -L -O http://www.antlr.org/download/antlr-4.13.1-complete.jar
ENV CLASSPATH="~/lib/antlr-4.13.1-complete.jar:$CLASSPATH"
RUN curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | sh -s -- -y
RUN cd ~ && curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v25.1/protoc-25.1-linux-x86_64.zip && \
    unzip protoc-25.1-linux-x86_64.zip -d ~/.local && \
    rm protoc-25.1-linux-x86_64.zip
USER root

# mkdir -p ~/lib && cd ~/lib && curl -L -O http://www.antlr.org/download/antlr-4.13.1-complete.jar
# export CLASSPATH="~/lib/antlr-4.13.1-complete.jar:$CLASSPATH"
# java -jar ~/lib/antlr-4.13.1-complete.jar