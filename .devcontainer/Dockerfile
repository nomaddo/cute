FROM mcr.microsoft.com/devcontainers/go:1-1.22

RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        ruby \
        ruby-dev \
        tmux \
        libmagickwand-dev libmagickcore-dev \
    && rm -rf /var/lib/apt/lists/*
RUN gem install rubygems-requirements-system 
RUN gem install bioshogi parquet
