# Docker Installation Script for Windows (PowerShell)
# This script provides instructions for installing Docker Desktop and configuring registry mirrors

# Установка Docker Desktop (включает Docker и Compose V2)
# Скачать с https://www.docker.com/products/docker-desktop/ и установить

# Настройка registry mirror через GUI:
# Открыть Docker Desktop → Settings → Docker Engine → вставить JSON:
# { "registry-mirrors": ["https://dockerhub.timeweb.cloud"] } → Apply & Restart

# Альтернативно, файл конфигурации находится по пути:
# %USERPROFILE%\.docker\daemon.json
# Создать или отредактировать и перезапустить Docker Desktop