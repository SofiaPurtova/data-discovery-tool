"""
Коннектор для SQLite баз данных.
Реализует подключение, получение списка таблиц и их схем.
"""

import sqlite3
from typing import List
from connectors.base import BaseConnector
from index.models import Table, Column


class SQLiteConnector(BaseConnector):
    """Коннектор для работы с SQLite базами данных"""
    
    def __init__(self, source_id: str, db_path: str):
        """
        Инициализация коннектора SQLite
        
        Args:
            source_id: Уникальный идентификатор источника
            db_path: Путь к SQLite файлу базы данных
        """
        super().__init__(source_id, db_path)
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """
        Подключение к SQLite базе данных
        
        Returns:
            True если подключение успешно, иначе False
        """
        try:
            self.connection = sqlite3.connect(self.connection_string)
            self.cursor = self.connection.cursor()
            return True
        except Exception as e:
            print(f"Ошибка подключения к SQLite: {e}")
            return False
    
    def get_tables(self) -> List[str]:
        """
        Получение списка всех таблиц в базе данных
        
        Returns:
            Список названий таблиц
        """
        if not self.cursor:
            raise ConnectionError("Нет подключения к базе данных. Сначала вызови connect()")
        
        # SQLite хранит информацию о таблицах в системной таблице sqlite_master
        self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
        )
        tables = [row[0] for row in self.cursor.fetchall()]
        return tables
    
    def get_table_schema(self, table_name: str) -> Table:
        """
        Получение схемы таблицы с примерами данных
        
        Args:
            table_name: Название таблицы
            
        Returns:
            Объект Table с метаданными
        """
        if not self.cursor:
            raise ConnectionError("Нет подключения к базе данных")
        
        # Получаем информацию о колонках через PRAGMA table_info
        self.cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = self.cursor.fetchall()
        
        columns = []
        for col in columns_info:
            # col: [cid, name, type, notnull, dflt_value, pk]
            col_name = col[1]
            col_type = col[2]
            
            # Получаем примеры данных (первые 3 строки)
            try:
                self.cursor.execute(f"SELECT {col_name} FROM {table_name} WHERE {col_name} IS NOT NULL LIMIT 3")
                samples = [row[0] for row in self.cursor.fetchall()]
            except:
                samples = []  # Если не получилось получить примеры
            
            columns.append(Column(
                name=col_name,
                data_type=col_type,
                sample_values=samples
            ))
        
        # Получаем количество строк в таблице
        try:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = self.cursor.fetchone()[0]
        except:
            row_count = 0
        
        return Table(
            name=table_name,
            columns=columns,
            row_count=row_count,
            source_id=self.source_id,
            path=table_name  # Для SQLite путь - это имя таблицы
        )
    
    def disconnect(self):
        """Закрытие соединения с базой данных"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            self.connection = None
            self.cursor = None