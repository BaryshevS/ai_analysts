# Построение AI-агента: Говори с данными на языке бизнеса (ai_analysts)

Презентация доступна по ссылке https://clck.ru/3S33s3

## Автор и экспертные материалы

**Автор проекта:** [@baryshevs](https://t.me/baryshevs) - эксперт в области искусственного интеллекта

**Канал:** [@baryshev_ai](https://t.me/baryshev_ai) - практические кейсы, инсайты и советы по работе с AI

Подписывайтесь на экспертный канал для получения актуальных материалов по аналитике данных и применению искусственного интеллекта в бизнесе.

## Последовательность установки и настройки проекта

### 1. Установка среды разработки (опционально)

Для работы с проектом рекомендуется использовать одну из следующих сред разработки:

Мы будем использовать для работы в этом репозитории PyCharm

#### Традиционные IDE и редакторы кода:
- [PyCharm](https://www.jetbrains.com/pycharm/) - мощная IDE от JetBrains специально для Python разработки
- [Visual Studio Code](https://code.visualstudio.com/) - популярный бесплатный редактор кода от Microsoft с богатой экосистемой расширений
- [Zed](https://zed.dev/) - современный высокопроизводительный редактор кода, ориентированный на скорость и простоту

#### Совместимые редакторы для Opencode / Claude Code :
- [Obsidian](https://obsidian.md/) - редактор Markdown с поддержкой плагинов

#### Альтернативные редакторы для Opencode / Claude Code:
- [Cursor](https://cursor.sh/) - редактор с встроенным AI, совместим с Claude Code
- [Windsurf](https://windsurf.com/) - редактор с интеграцией AI ассистентов
- [Replit](https://replit.com/) - облачная IDE с поддержкой различных языков программирования

Все перечисленные среды предоставляют отличную поддержку Python и позволяют эффективно работать с проектом. Выбор зависит от ваших предпочтений и рабочего процесса.

### 2. Настройка виртуального окружения Python

Перед началом работы необходимо создать и активировать виртуальное окружение:

**Linux/macOS:**
```bash
# Создание виртуального окружения
python3 -m venv .venv

# Активация виртуального окружения
source .venv/bin/activate
```

**Windows:**
```cmd
# Создание виртуального окружения
python -m venv .venv

# Активация виртуального окружения
.venv\Scripts\activate
```

### 3. Установка зависимостей

После активации виртуального окружения установите необходимые зависимости:

**Linux:**
```bash
# Установка Python зависимостей
pip install -r requirements.txt

# Установка системных утилит (если не установлены)
sudo apt update
sudo apt install curl wget bash clickhouse-client
```

**macOS:**
```bash
# Установка Python зависимостей
pip install -r requirements.txt

# Установка системных утилит (если не установлены)
brew install curl wget bash clickhouse-client
```

**Windows:**
```cmd
# Установка Python зависимостей
pip install -r requirements.txt

# Для Windows большинство утилит можно установить через Chocolatey или использовать WSL
# Установка через Chocolatey:
choco install curl wget

# Или использование WSL для полной совместимости
```

### 4. Установка Opencode / Claude Code

Для использования возможностей ИИ агента Opencode следуйте официальной документации по установке на сайте https://opencode.ai

Альтернативой выступает Claude Code https://claude.com/product/claude-code


### 5. Настройка конфигурационного файла .env

Создайте или отредактируйте файл `.env` в корне проекта, заполнив необходимые параметры:

```env
DB_USER=click_ro
DB_PASSWORD=click_pass
CLICKHOUSE_DB=company_one
CLICKHOUSE_HOSTS=1.1.1.1:9000
REDASH_TOKEN=redash_token
REDASH_HOST=https://public.url
```

💡 **Поддержите проект:** Мы специально для простоты держим тестовый контур рабочим, поставьте звезду в GitHub репозитории! ⭐ [https://github.com/BaryshevS/ai_analysts](https://github.com/BaryshevS/ai_analysts)

### 6. Инициализация данных

После настройки конфигурации выполните инициализацию данных:

**Linux/macOS:**
```bash
# Экспорт DDL схем из ClickHouse
./clickhouse_export_ddl.sh

# Загрузка истории запросов из Redash
python update_queries.py

# Загрузка сниппетов запросов из Redash
python update_query_snippets.py
```

**Windows:**
```cmd
# Экспорт DDL схем из ClickHouse
bash clickhouse_export_ddl.sh

# Загрузка истории запросов из Redash
python update_queries.py

# Загрузка сниппетов запросов из Redash
python update_query_snippets.py
```

Примечание для пользователей Windows: Для запуска shell скриптов может потребоваться установка Git Bash, WSL или Cygwin.

Готово! Теперь проект полностью настроен и готов к использованию.
