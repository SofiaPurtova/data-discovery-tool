"""
Модуль поиска по проиндексированным метаданным.
Позволяет искать таблицы и колонки по ключевым словам.
"""
import os
from typing import List, Dict, Any, Optional
from index.indexer import Indexer
from index.models import Table, Column


class SearchResult:
    """Класс для представления результата поиска"""
    
    def __init__(self, table: Table, column: Column = None, match_type: str = 'table'):
        """
        Инициализация результата поиска
        
        Args:
            table: Таблица, в которой найдено совпадение
            column: Колонка (если поиск по колонкам)
            match_type: Тип совпадения ('table', 'column', 'data')
        """
        self.table = table
        self.column = column
        self.match_type = match_type
        self.relevance = self._calculate_relevance()
    
    def _calculate_relevance(self) -> int:
        """Вычисление релевантности результата"""
        # Таблицы считаем более релевантными, чем колонки
        if self.match_type == 'table':
            return 10
        elif self.match_type == 'column':
            return 5
        else:
            return 1
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразование в словарь для API"""
        result = {
            'table_name': self.table.name,
            'source_id': self.table.source_id,
            'match_type': self.match_type,
            'relevance': self.relevance,
            'row_count': self.table.row_count,
            'path': self.table.path
        }
        
        if self.column:
            result['column_name'] = self.column.name
            result['column_type'] = self.column.data_type
        
        return result
    
    def __repr__(self):
        if self.match_type == 'table':
            return f"Таблица: {self.table.name} (в {self.table.source_id})"
        else:
            return f"Колонка: {self.table.name}.{self.column.name} (в {self.table.source_id})"


class Searcher:
    """Класс для поиска по индексу"""
    
    def __init__(self, indexer: Indexer):
        """
        Инициализация поисковика
        
        Args:
            indexer: Индексатор с данными
        """
        self.indexer = indexer
    
    def search(self, query: str, case_sensitive: bool = False) -> List[SearchResult]:
        """
        Поиск по ключевому слову
        
        Args:
            query: Поисковый запрос
            case_sensitive: Учитывать регистр
            
        Returns:
            Список результатов поиска
        """
        results = []
        
        # Подготовка запроса
        if not case_sensitive:
            query = query.lower()
        
        # Поиск по всем источникам
        for source in self.indexer.get_all_sources():
            for table in source.tables:
                # Поиск по названию таблицы
                table_name = table.name if case_sensitive else table.name.lower()
                if query in table_name:
                    results.append(SearchResult(table, match_type='table'))
                
                # Поиск по названиям колонок
                for column in table.columns:
                    column_name = column.name if case_sensitive else column.name.lower()
                    if query in column_name:
                        results.append(SearchResult(table, column, 'column'))
        
        # Сортировка по релевантности
        results.sort(key=lambda x: x.relevance, reverse=True)
        
        return results
    
    def search_tables(self, query: str) -> List[Table]:
        """
        Поиск только по таблицам
        
        Args:
            query: Поисковый запрос
            
        Returns:
            Список таблиц
        """
        results = []
        query_lower = query.lower()
        
        for source in self.indexer.get_all_sources():
            for table in source.tables:
                if query_lower in table.name.lower():
                    results.append(table)
        
        return results
    
    def search_columns(self, query: str) -> List[tuple]:
        """
        Поиск только по колонкам
        
        Args:
            query: Поисковый запрос
            
        Returns:
            Список кортежей (таблица, колонка)
        """
        results = []
        query_lower = query.lower()
        
        for source in self.indexer.get_all_sources():
            for table in source.tables:
                for column in table.columns:
                    if query_lower in column.name.lower():
                        results.append((table, column))
        
        return results
    
    def get_table_schema(self, source_id: str, table_path: str) -> Optional[Dict]:
        """
        Получить детальную схему таблицы
        
        Args:
            source_id: Идентификатор источника
            table_path: Путь к таблице
            
        Returns:
            Словарь с описанием таблицы или None
        """
        # Нормализуем путь
        if '\\' in table_path or '/' in table_path:
            table_path = os.path.basename(table_path)

        table = self.indexer.get_table(source_id, table_path)
        if not table:
            return None
        
        return {
            'table_name': table.name,
            'source_id': table.source_id,
            'row_count': table.row_count,
            'columns': [
                {
                    'name': col.name,
                    'type': col.data_type,
                    'samples': [str(s) if s is not None else 'NULL' for s in col.sample_values]
                }
                for col in table.columns
            ]
        }
    
    def get_search_suggestions(self, prefix: str, limit: int = 5) -> List[str]:
        """
        Получить подсказки для поиска
        
        Args:
            prefix: Начало названия
            limit: Максимальное количество подсказок
            
        Returns:
            Список подсказок
        """
        suggestions = set()
        prefix_lower = prefix.lower()
        
        for source in self.indexer.get_all_sources():
            for table in source.tables:
                if table.name.lower().startswith(prefix_lower):
                    suggestions.add(table.name)
                
                for column in table.columns:
                    if column.name.lower().startswith(prefix_lower):
                        suggestions.add(f"{table.name}.{column.name}")
        
        return list(suggestions)[:limit]