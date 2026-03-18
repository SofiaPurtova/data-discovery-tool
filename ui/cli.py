"""
CLI интерфейс для Data Discovery Tool.
Позволяет управлять системой из командной строки.
"""

import click
import json
from mcp.tools import MCPTool

# Создаем экземпляр MCPTool
tool = MCPTool()


@click.group()
def cli():
    """🔍 Data Discovery Tool - поиск данных в разных источниках"""
    pass


@cli.command()
def sources():
    """Список всех источников данных"""
    sources = tool.list_sources()
    
    if not sources:
        click.echo("❌ Нет добавленных источников")
        click.echo("   Используйте: python ui/cli.py add-sqlite --help")
        return
    
    click.echo("\n📊 Источники данных:")
    click.echo("-" * 60)
    
    for s in sources:
        status_icon = "✅" if s['status'] == 'indexed' else "⏳"
        click.echo(f"{status_icon} ID: {s['id']}")
        click.echo(f"   Тип: {s['type']}")
        click.echo(f"   Путь: {s['connection_string']}")
        click.echo(f"   Таблиц: {s['tables_count']}")
        if s['last_indexed']:
            click.echo(f"   Индексирован: {s['last_indexed']}")
        click.echo()


@cli.command()
@click.argument('source_id')
def index(source_id):
    """Индексировать указанный источник"""
    click.echo(f"🔄 Индексация источника {source_id}...")
    
    result = tool.index_source(source_id)
    
    if result['success']:
        click.echo(f"✅ {result['message']}")
        click.echo(f"   Проиндексировано таблиц: {result['tables_indexed']}")
    else:
        click.echo(f"❌ Ошибка: {result['error']}")


@cli.command()
@click.argument('query')
@click.option('--case-sensitive', is_flag=True, help='Учитывать регистр')
def search(query, case_sensitive):
    """Поиск по ключевому слову"""
    click.echo(f"🔍 Поиск: '{query}'")
    click.echo("-" * 60)
    
    results = tool.search(query, case_sensitive)
    
    if not results:
        click.echo("❌ Ничего не найдено")
        return
    
    click.echo(f"📋 Найдено {len(results)} результатов:\n")
    
    for i, r in enumerate(results, 1):
        if r['match_type'] == 'table':
            icon = "📋"
            name = r['table_name']
        else:
            icon = "🔍"
            name = f"{r['table_name']}.{r['column_name']}"
        
        click.echo(f"{i}. {icon} {name}")
        click.echo(f"   Источник: {r['source_id']}")
        click.echo(f"   Строк: {r['row_count']}")
        
        if i < len(results):
            click.echo()


@cli.command()
@click.argument('source_id')
@click.argument('path')
def schema(source_id, path):
    """Показать схему таблицы"""
    click.echo(f"📊 Схема таблицы {path} из {source_id}")
    click.echo("-" * 60)
    
    schema = tool.get_schema(source_id, path)
    
    if 'error' in schema:
        click.echo(f"❌ {schema['error']}")
        return
    
    click.echo(f"Таблица: {schema['table_name']}")
    click.echo(f"Всего строк: {schema['row_count']}")
    click.echo("\nКолонки:")
    
    for col in schema['columns']:
        samples = ', '.join(str(s) for s in col['samples'][:3])
        click.echo(f"  • {col['name']} ({col['type']})")
        click.echo(f"    Примеры: [{samples}]")


@cli.command()
@click.argument('source_id')
@click.argument('db_path')
def add_sqlite(source_id, db_path):
    """Добавить SQLite источник"""
    result = tool.add_source(source_id, 'sqlite', db_path)
    
    if result['success']:
        click.echo(f"✅ {result['message']}")
        click.echo(f"   Теперь выполните: python ui/cli.py index {source_id}")
    else:
        click.echo(f"❌ Ошибка: {result['error']}")


@cli.command()
@click.argument('source_id')
@click.argument('folder_path')
def add_csv(source_id, folder_path):
    """Добавить CSV источник (папку с файлами)"""
    result = tool.add_source(source_id, 'csv', folder_path)
    
    if result['success']:
        click.echo(f"✅ {result['message']}")
        click.echo(f"   Теперь выполните: python ui/cli.py index {source_id}")
    else:
        click.echo(f"❌ Ошибка: {result['error']}")


@cli.command()
def stats():
    """Показать статистику системы"""
    stats = tool.get_stats()
    
    click.echo("\n📈 Статистика системы:")
    click.echo("-" * 60)
    click.echo(f"Всего источников: {stats['total_sources']}")
    click.echo(f"Индексировано: {stats['indexed_sources']}")
    click.echo(f"Всего таблиц: {stats['total_tables']}")
    click.echo(f"Всего колонок: {stats['total_columns']}")
    
    if stats['sources']:
        click.echo("\nПо источникам:")
        for s in stats['sources']:
            status = "✅" if s['last_indexed'] else "⏳"
            click.echo(f"  {status} {s['id']}: {s['tables']} таблиц")


@cli.command()
@click.argument('prefix')
@click.option('--limit', default=5, help='Количество подсказок')
def suggest(prefix, limit):
    """Получить подсказки для поиска"""
    suggestions = tool.get_suggestions(prefix, limit)
    
    if suggestions:
        click.echo("Подсказки:")
        for s in suggestions:
            click.echo(f"  • {s}")
    else:
        click.echo("Нет подсказок")


if __name__ == '__main__':
    cli()