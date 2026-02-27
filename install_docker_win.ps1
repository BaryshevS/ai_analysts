# Docker Installation Script for Windows (PowerShell)
# This script provides instructions for installing Docker Desktop and configuring registry mirrors

# Проверка наличия прав администратора
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Warning "This script requires administrator privileges to install Docker."
    Write-Host "Please run PowerShell as Administrator and try again."
    exit 1
}

# Установка Docker Desktop (включает Docker и Compose V2)
# Скачать с https://www.docker.com/products/docker-desktop/ и установить
Write-Host "Please download Docker Desktop from https://www.docker.com/products/docker-desktop/ and install it manually."

# Настройка registry mirror через GUI:
# Открыть Docker Desktop → Settings → Docker Engine → вставить JSON:
# { "registry-mirrors": ["https://dockerhub.timeweb.cloud"] } → Apply & Restart

# Альтернативно, файл конфигурации находится по пути:
# %USERPROFILE%\.docker\daemon.json
# Создать или отредактировать и перезапустить Docker Desktop

# Создание конфигурационного файла daemon.json с настройками зеркала registry
$dockerConfigPath = "$env:USERPROFILE\.docker"
$daemonConfigPath = "$dockerConfigPath\daemon.json"

# Создаем директорию .docker если она не существует
if (!(Test-Path $dockerConfigPath)) {
    New-Item -ItemType Directory -Path $dockerConfigPath
}

# Создаем или обновляем daemon.json с настройками зеркала
$daemonConfig = @{
    "registry-mirrors" = @("https://dockerhub.timeweb.cloud")
}

# Преобразуем конфигурацию в JSON и сохраняем в файл
$daemonConfig | ConvertTo-Json | Out-File -FilePath $daemonConfigPath -Encoding utf8

Write-Host "Docker daemon configuration file created at $daemonConfigPath"
Write-Host "Please restart Docker Desktop to apply the registry mirror settings."