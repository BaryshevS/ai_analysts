#!/bin/bash

# Загрузка переменных окружения из .env файла
if [ -f ".env" ]; then
    # Используем более надежный способ загрузки переменных с особыми символами
    while IFS= read -r line; do
        # Пропускаем комментарии и пустые строки
        if [[ ! "$line" =~ ^#.* ]] && [[ -n "$line" ]]; then
            # Разделяем ключ и значение только по первому знаку =
            key=$(echo "$line" | cut -d'=' -f1 | xargs)
            value=$(echo "$line" | cut -d'=' -f2-)
            # Экранируем специальные символы в значении
            export "$key=$value"
        fi
    done < <(grep -v '^#' .env | grep -v '^$')
fi

# Проверка наличия необходимых переменных
if [ -z "$DB_USER" ] || [ -z "$DB_PASSWORD" ] || [ -z "$CLICKHOUSE_HOSTS" ]; then
    echo "Ошибка: Не все необходимые переменные окружения заданы в .env файле"
    echo "Требуются: DB_USER, DB_PASSWORD, CLICKHOUSE_HOSTS"
    exit 1
fi

# Получение первого хоста из списка (включая порт, если указан)
CLICKHOUSE_HOST_PORT=$(echo $CLICKHOUSE_HOSTS | cut -d',' -f1)

# Определяем протокол (HTTP или HTTPS) в зависимости от переменной CLICKHOUSE_SECURE
# Если переменная не установлена, проверяем наличие порта 8123 для выбора HTTP по умолчанию
if [ "$CLICKHOUSE_SECURE" = "true" ]; then
    PROTOCOL="https"
elif [ "$CLICKHOUSE_SECURE" = "false" ]; then
    PROTOCOL="http"
else
    # По умолчанию используем http для порта 8123, иначе https
    PORT=$(echo "$CLICKHOUSE_HOST_PORT" | cut -d':' -f2)
    if [ "$PORT" = "8123" ] || [ -z "$PORT" ]; then
        PROTOCOL="http"
    else
        PROTOCOL="https"
    fi
fi

# Параметры подключения из переменных окружения
CH_HOST="$PROTOCOL://$CLICKHOUSE_HOST_PORT"
CH_USER="$DB_USER"
CH_PASSWORD="$DB_PASSWORD"

# Таймаут для команд ClickHouse (в секундах)
CLICKHOUSE_TIMEOUT=15

# Количество попыток для повторных запросов
MAX_RETRIES=3

# Директория для экспорта
EXPORT_DIR="clickhouse_db"
mkdir -p "$EXPORT_DIR"

echo "🚀 Начинаем экспорт DDL схем из ClickHouse..."
echo "Хост: $CH_HOST"
echo "Пользователь: $CH_USER"
echo "Директория экспорта: $EXPORT_DIR"
echo ""

# Функция для выполнения HTTP-запроса к ClickHouse с повторными попытками
run_clickhouse_query() {
    local query="$1"
    local max_retries=${2:-$MAX_RETRIES}
    local retry_count=0

    while [ $retry_count -lt $max_retries ]; do
        # Выполняем запрос
        result=$(curl -s -u "$CH_USER:$CH_PASSWORD" \
            -H "Content-Type: text/plain; charset=utf-8" \
            --max-time $CLICKHOUSE_TIMEOUT \
            -X POST \
            -d "$query" \
            "$CH_HOST" 2>&1)

        exit_code=$?

        # Проверяем успешность выполнения
        if [ $exit_code -eq 0 ] && [ -n "$result" ] && [[ ! "$result" == *"Error"* ]] && [[ ! "$result" == *"Exception"* ]]; then
            echo "$result"
            return 0
        fi

        # Если не успешно, увеличиваем счетчик попыток
        retry_count=$((retry_count + 1))
        if [ $retry_count -lt $max_retries ]; then
            echo "    ⚠️  Попытка $retry_count не удалась, повторяем через 1 секунду..."
            sleep 1
        fi
    done

    # Все попытки исчерпаны
    echo "$result"
    return $exit_code
}

# Получаем список баз (исключая системные)
echo "📋 Получаем список баз данных..."
databases=$(run_clickhouse_query "SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema');")

# Проверяем код возврата и выводим ошибку если есть
if [ $? -ne 0 ]; then
    echo "❌ Ошибка при получении списка баз данных:"
    echo "$databases"
    exit 1
fi

# Проверяем, что список баз не пустой
if [ -z "$databases" ]; then
    echo "❌ Список баз данных пуст"
    exit 1
fi

# Преобразуем в массив для подсчета
db_array=($databases)
total_dbs=${#db_array[@]}
echo "📊 Найдено баз данных: $total_dbs"
echo ""

# Список проблемных таблиц, которые могут вызывать таймауты
PROBLEMATIC_TABLES=()

# Функция для альтернативного экспорта таблицы через system.tables
alternative_export() {
    local db=$1
    local table=$2
    local output_file=$3

    echo "    🔧 Пытаюсь экспортировать через альтернативный метод..."

    # Получаем DDL из system.tables с более надежным запросом и повторными попытками
    table_ddl=$(run_clickhouse_query "SELECT create_table_query FROM system.tables WHERE database = '$db' AND name = '$table' AND create_table_query != '';" 2)

    # Проверяем код возврата и содержимое
    if [ $? -eq 0 ] && [ -n "$table_ddl" ] && [[ ! "$table_ddl" == *"Error"* ]] && [[ ! "$table_ddl" == *"Exception"* ]] && [ "$table_ddl" != "''" ]; then
        # Сохраняем DDL из system.tables
        echo "$table_ddl" > "$output_file"
        echo "    ✅ Альтернативный экспорт завершен успешно"
        return 0
    else
        echo "    ⚠️  Альтернативный метод не удался:"
        echo "    $table_ddl"
        return 1
    fi
}

# Экспортируем каждую базу
counter=0
success_dbs=0
failed_dbs=0

for db in $databases; do
    counter=$((counter + 1))
    echo "📁 [$counter/$total_dbs] Экспортирую базу: $db"
    DB_DIR="$EXPORT_DIR/$db"
    mkdir -p "$DB_DIR"

    # Получаем список таблиц с таймаутом и повторными попытками
    echo "  📋 Получаем список таблиц..."
    table_list=$(run_clickhouse_query "SHOW TABLES FROM \`$db\`;" 2)

    # Проверяем код возврата
    if [ $? -ne 0 ]; then
        echo "  ⚠️  Ошибка при получении списка таблиц для базы $db:"
        echo "  $table_list"
        failed_dbs=$((failed_dbs + 1))
        echo "  ❌ База $db не обработана из-за ошибки получения списка таблиц"
        echo ""
        continue
    fi

    # Сохраняем список таблиц в файл
    echo "$table_list" > "$DB_DIR/tables.txt"

    # Подсчитываем количество таблиц
    table_count=$(grep -vc '^$' "$DB_DIR/tables.txt")
    echo "  📊 Найдено таблиц: $table_count"

    # Экспортируем DDL каждой таблицы
    table_counter=0
    success_tables=0
    failed_tables=0

    while read -r table; do
        if [ -n "$table" ]; then
            table_counter=$((table_counter + 1))

            # Проверяем, является ли таблица проблемной
            full_table_name="$db.$table"
            is_problematic=false
            for prob_table in "${PROBLEMATIC_TABLES[@]}"; do
                if [ "$full_table_name" == "$prob_table" ]; then
                    is_problematic=true
                    break
                fi
            done

            if [ "$is_problematic" = true ]; then
                echo "  [$table_counter/$table_count] ⏭️  Пропускаю проблемную таблицу: $table"
                continue
            fi

            # Проверяем, заканчивается ли имя таблицы на _del, _temp или _tmp, или начинается с temp_
            if [[ "$table" == *_del ]] || [[ "$table" == *_temp ]] || [[ "$table" == *_tmp ]] || [[ "$table" == *_to_del ]] || [[ "$table" == temp_* ]]; then
                echo "  [$table_counter/$table_count] ⏭️  Пропускаю временную/удаляемую таблицу: $table"
                continue
            fi

            echo "  [$table_counter/$table_count] 📄 Экспортирую таблицу: $table"

            # Проверяем, существует ли файл с DDL
            if [ -f "$DB_DIR/${table}.sql" ]; then
                echo "    ⏭️  Файл уже существует, пропускаю: $DB_DIR/${table}.sql"
                success_tables=$((success_tables + 1))
                continue
            fi

            # Экспортируем DDL с таймаутом
            table_ddl=$(run_clickhouse_query "SHOW CREATE TABLE \`$db\`.\`$table\`;" 2)

            # Проверяем код возврата
            if [ $? -ne 0 ]; then
                echo "    ⚠️  Ошибка при экспорте таблицы $table:"
                echo "    $table_ddl"

                # Пробуем альтернативный метод экспорта
                if alternative_export "$db" "$table" "$DB_DIR/${table}.sql"; then
                    echo "    ✅ Таблица $table экспортирована альтернативным методом"
                    success_tables=$((success_tables + 1))
                else
                    # Создаем файл с сообщением об ошибке если все методы не удалась
                    echo "-- ОШИБКА: Не удалось экспортировать таблицу $table из базы $db" > "$DB_DIR/${table}.sql"
                    echo "    ❌ Все методы экспорта не удалась для таблицы $table"
                    failed_tables=$((failed_tables + 1))
                fi
            else
                # Сохраняем DDL в файл
                echo "$table_ddl" > "$DB_DIR/${table}.sql"
                success_tables=$((success_tables + 1))
            fi
        fi
    done < "$DB_DIR/tables.txt"

    echo "  ✅ База $db обработана: $success_tables успешных, $failed_tables с ошибками"
    # Увеличиваем счетчик успешных баз (база считается успешной если мы смогли получить список таблиц)
    success_dbs=$((success_dbs + 1))
    echo ""
done

echo "🏁 Завершено! Успешно обработано баз: $success_dbs, с ошибками: $failed_dbs"

echo "🎉 Готово! Экспорт сохранен в: $EXPORT_DIR"