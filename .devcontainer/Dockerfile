FROM mcr.microsoft.com/devcontainers/go:1-1.22

RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        tmux \
    && rm -rf /var/lib/apt/lists/*
