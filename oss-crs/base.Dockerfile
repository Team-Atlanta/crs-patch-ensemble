FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    git \
    rsync \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Python 3.12 (deadsnakes PPA)
RUN apt-get update && apt-get install -y software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update && apt-get install -y \
    python3.12 python3.12-venv python3.12-dev \
    && rm -rf /var/lib/apt/lists/*
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.12
RUN ln -sf /usr/bin/python3.12 /usr/bin/python3 \
    && ln -sf python3 /usr/bin/python

# Node.js 20.x (required for Claude Code CLI)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Claude Code CLI (used by selector for patch selection)
RUN npm install -g @anthropic-ai/claude-code

# Git config
RUN git config --global user.email "crs@oss-crs.dev" \
    && git config --global user.name "OSS-CRS Patcher" \
    && git config --global --add safe.directory '*'
