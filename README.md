# Data Discovery Tool (MCP)

Инструмент для поиска данных в разных источниках (SQLite, CSV) через MCP API.

## Возможности

- Подключение к SQLite базам данных и CSV файлам
- Индексация структуры таблиц и колонок
- Поиск по названиям таблиц и колонок
- MCP API для AI-агентов
- CLI интерфейс с сохранением состояния
- Веб-интерфейс на Flask

## Установка

```bash
# Клонировать репозиторий
git clone https://github.com/yourusername/data-discovery-tool
cd data-discovery-tool

# Создать виртуальное окружение
python -m venv venv
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Установить зависимости
pip install -r requirements.txt
pip install -e .
```
## Использование
### Создание тестовых данных

```bash
python create_test_data.py
```
### Работа через командную строку

```bash
# Добавить SQLite базу данных
python ui/cli.py add-sqlite mydb data/sample.db

# Добавить папку с CSV файлами
python ui/cli.py add-csv mycsv data/

# Проиндексировать конкретный источник
python ui/cli.py index mydb
python ui/cli.py index mycsv

# Или все сразу (индексируем по очереди)
python ui/cli.py index mydb
python ui/cli.py index mycsv

# Поиск по ключевым словам
python ui/cli.py search user
python ui/cli.py search email
python ui/cli.py search product
python ui/cli.py search москва

# Список всех источников
python ui/cli.py sources

# Статистика системы
python ui/cli.py stats

# Схема конкретной таблицы
python ui/cli.py schema mydb users
python ui/cli.py schema mycsv employees.csv

# Подсказки для поиска
python ui/cli.py suggest user
```

### Работа через веб-интерфейс

```bash
python ui/web.py
```
После запуска можно открыть браузер и перейти по адресу: http://localhost:5000

#### Возможности веб-интерфейса:

###### Поиск: 
- После введения ключевого слова и можно увидеть все совпадения
- Подсветка результатов
- Быстрый просмотр схемы таблиц
- Количество найденных строк

###### Источники данных 
- Реализовано управление источниками через интерфейс
- Просмотр всех добавленных источников
- Статус индексации (индексирован, ожидает)
- Кнопка для индексации
- Информация о количестве таблиц

##### Схемы таблиц 
- Можно увидеть детальную информацию о структуре
- Список всех колонок
- Примеры значений

### Автор
SofiaPurtova