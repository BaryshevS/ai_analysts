#!/bin/bash

# Установка Docker Desktop (включает Docker и Compose V2)
brew install --cask docker

# Или установка standalone docker-compose через Homebrew
brew install docker-compose

# Настройка registry mirror через GUI Docker Desktop:
# Открыть Docker Desktop → Preferences → Docker Engine → вставить JSON:
# { "registry-mirrors": ["https://dockerhub.timeweb.cloud"] } → Apply & Restart

# Альтернативно, через терминал (если Docker Desktop работает)
mkdir -p ~/.docker
echo '{"registry-mirrors": ["https://dockerhub.timeweb.cloud"]}' > ~/.docker/daemon.json
# Перезапустить Docker Desktop вручную или через killall Docker