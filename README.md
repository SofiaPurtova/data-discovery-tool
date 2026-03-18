# Data Discovery Tool (MCP)

Инструмент для поиска данных в разных источниках (SQLite, CSV) через MCP API.

## 🚀 Возможности

- Подключение к SQLite базам данных и CSV файлам
- Индексация структуры таблиц и колонок
- Поиск по названиям таблиц и колонок
- MCP API для AI-агентов
- CLI интерфейс с сохранением состояния

## 📦 Установка

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