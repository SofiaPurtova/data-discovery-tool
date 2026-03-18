"""
Базовый абстрактный класс для всех коннекторов к источникам данных.
Определяет интерфейс, который должны реализовать все конкретные коннекторы.
"""

from abc import ABC, abstractmethod
from typing import List
from index.models import Table


class BaseConnector(ABC):
    """Абстрактный базовый класс для всех коннекторов"""
    
    def __init__(self, source_id: str, connection_string: str):
        """
        Инициализация коннектора
        
        Args:
            source_id: Уникальный идентификатор источника
            connection_string: Строка подключения (путь к БД или папке)
        """
        self.source_id = source_id
        self.connection_string = connection_string
    
    @abstractmethod
    def connect(self) -> bool:
        """
        Подключение к источнику данных
        
        Returns:
            True если подключение успешно, иначе False
        """
        pass
    
    @abstractmethod
    def get_tables(self) -> List[str]:
        """
        Получить список всех таблиц/файлов в источнике
        
        Returns:
            Список названий таблиц
        """
        pass
    
    @abstractmethod
    def get_table_schema(self, table_name: str) -> Table:
        """
        Получить схему конкретной таблицы с примерами данных
        
        Args:
            table_name: Название таблицы или файла
            
        Returns:
            Объект Table с метаданными
        """
        pass
    
    @abstractmethod
    def disconnect(self):
        """Закрыть соединение с источником"""
        pass
    
    def __enter__(self):
        """Поддержка контекстного менеджера (with)"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Автоматическое закрытие при выходе из контекста"""
        self.disconnect()