"""
Индексатор для сбора метаданных из всех подключенных источников.
Управляет коннекторами и хранит проиндексированные данные.
"""
import os
from typing import Dict, List, Optional
from datetime import datetime
from connectors.sqlite import SQLiteConnector
from connectors.csv_connector import CSVConnector
from index.models import Source, Table


class Indexer:
    """Класс для индексации метаданных из различных источников"""
    
    def __init__(self):
        """Инициализация индексатора"""
        self.sources: Dict[str, Source] = {}  # source_id -> Source
        self.connectors: Dict[str, object] = {}  # source_id -> connector
    
    def add_source(self, source_id: str, source_type: str, connection_string: str) -> None:
        """
        Добавить новый источник данных
        
        Args:
            source_id: Уникальный идентификатор источника
            source_type: Тип источника ('sqlite' или 'csv')
            connection_string: Строка подключения (путь к БД или папке)
            
        Raises:
            ValueError: Если тип источника не поддерживается
        """
        # Создаем соответствующий коннектор
        if source_type == 'sqlite':
            connector = SQLiteConnector(source_id, connection_string)
        elif source_type == 'csv':
            connector = CSVConnector(source_id, connection_string)
        else:
            raise ValueError(f"Неподдерживаемый тип источника: {source_type}")
        
        # Сохраняем коннектор и источник
        self.connectors[source_id] = connector
        self.sources[source_id] = Source(
            id=source_id,
            type=source_type,
            connection_string=connection_string,
            tables=[]
        )
        
        print(f"Добавлен источник: {source_id} ({source_type})")
    
    def index_source(self, source_id: str) -> Optional[Source]:
        """
        Проиндексировать указанный источник
        
        Args:
            source_id: Идентификатор источника для индексации
            
        Returns:
            Обновленный объект Source или None в случае ошибки
            
        Raises:
            ValueError: Если источник не найден
        """
        if source_id not in self.connectors:
            raise ValueError(f"Источник {source_id} не найден")
        
        connector = self.connectors[source_id]
        source = self.sources[source_id]
        
        print(f"Индексация источника: {source_id}...")
        
        try:
            # Подключаемся к источнику
            if not connector.connect():
                print(f"Не удалось подключиться к {source_id}")
                return None
            
            # Получаем список таблиц
            table_names = connector.get_tables()
            print(f"   Найдено таблиц: {len(table_names)}")
            
            # Индексируем каждую таблицу
            tables = []
            for i, table_name in enumerate(table_names, 1):
                print(f"   Индексация [{i}/{len(table_names)}]: {table_name}")
                try:
                    table = connector.get_table_schema(table_name)
                    tables.append(table)
                except Exception as e:
                    print(f"      Ошибка индексации {table_name}: {e}")
            
            # Обновляем источник
            source.tables = tables
            source.last_indexed = datetime.now()
            
            print(f"Источник {source_id} успешно проиндексирован")
            print(f"Всего таблиц: {len(tables)}")
            print(f"Время индексации: {source.last_indexed.strftime('%Y-%m-%d %H:%M:%S')}")
            
            return source
            
        except Exception as e:
            print(f"Ошибка индексации {source_id}: {e}")
            return None
            
        finally:
            # Всегда закрываем соединение
            connector.disconnect()

    def load_tables(self, source_id: str, tables_data: list):
        """
        Загрузить таблицы из сохранённых данных
        
        Args:
            source_id: Идентификатор источника
            tables_data: Данные таблиц из JSON
        """
        if source_id not in self.sources:
            return
        
        source = self.sources[source_id]
        tables = []
        
        from index.models import Table, Column
        
        for table_data in tables_data:
            columns = []
            for col_data in table_data.get('columns', []):
                column = Column(
                    name=col_data['name'],
                    data_type=col_data['type'],
                    sample_values=col_data.get('sample_values', [])
                )
                columns.append(column)
            
            table = Table(
                name=table_data['name'],
                columns=columns,
                row_count=table_data.get('row_count', 0),
                source_id=source_id,
                path=table_data.get('path', table_data['name'])
            )
            tables.append(table)
        
        source.tables = tables
        
    def index_all_sources(self) -> Dict[str, bool]:
        """
        Проиндексировать все добавленные источники
        
        Returns:
            Словарь с результатами индексации для каждого источника
        """
        results = {}
        for source_id in self.sources.keys():
            result = self.index_source(source_id)
            results[source_id] = result is not None
        return results
    
    def get_source(self, source_id: str) -> Optional[Source]:
        """
        Получить источник по ID
        
        Args:
            source_id: Идентификатор источника
            
        Returns:
            Объект Source или None
        """
        return self.sources.get(source_id)
    
    def get_all_sources(self) -> List[Source]:
        """
        Получить все источники
        
        Returns:
            Список всех источников
        """
        return list(self.sources.values())
    
    def get_table(self, source_id: str, table_path: str) -> Optional[Table]:
        """
        Получить таблицу из источника по пути
        
        Args:
            source_id: Идентификатор источника
            table_path: Путь к таблице (имя таблицы или файла)
            
        Returns:
            Объект Table или None
        """
        source = self.get_source(source_id)
        if not source:
            return None
        
        # Нормализуем путь для поиска
        search_path = table_path.replace('/', '\\').lower()
        search_name = os.path.basename(search_path)  # Получаем только имя файла
        
        for table in source.tables:
            # Сравниваем по разным вариантам
            table_path_norm = table.path.replace('/', '\\').lower()
            table_name = os.path.basename(table_path_norm)
            
            if (table.path == table_path or 
                table_path_norm == search_path or 
                table_name == search_name or
                table.name == table_path):
                return table
    
        return None
    
    def remove_source(self, source_id: str) -> bool:
        """
        Удалить источник
        
        Args:
            source_id: Идентификатор источника для удаления
            
        Returns:
            True если источник удален, иначе False
        """
        if source_id in self.sources:
            del self.sources[source_id]
        if source_id in self.connectors:
            del self.connectors[source_id]
        print(f"Источник {source_id} удален")
        return True
    
    def get_stats(self) -> Dict:
        """
        Получить статистику по индексатору
        
        Returns:
            Словарь со статистикой
        """
        total_sources = len(self.sources)
        total_tables = sum(len(s.tables) for s in self.sources.values())
        total_columns = sum(len(t.columns) for s in self.sources.values() for t in s.tables)
        
        indexed_sources = sum(1 for s in self.sources.values() if s.last_indexed is not None)
        
        return {
            'total_sources': total_sources,
            'indexed_sources': indexed_sources,
            'total_tables': total_tables,
            'total_columns': total_columns,
            'sources': [
                {
                    'id': s.id,
                    'type': s.type,
                    'tables': len(s.tables),
                    'last_indexed': s.last_indexed.isoformat() if s.last_indexed else None
                }
                for s in self.sources.values()
            ]
        }