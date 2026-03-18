"""
Коннектор для CSV файлов.
Реализует чтение CSV файлов из папки и получение их схем.
"""

import os
import pandas as pd
from typing import List
from connectors.base import BaseConnector
from index.models import Table, Column


class CSVConnector(BaseConnector):
    """Коннектор для работы с CSV файлами в папке"""
    
    def __init__(self, source_id: str, folder_path: str):
        """
        Инициализация коннектора CSV
        
        Args:
            source_id: Уникальный идентификатор источника
            folder_path: Путь к папке с CSV файлами
        """
        super().__init__(source_id, folder_path)
        self.folder_path = folder_path
        self.is_connected = False
    
    def connect(self) -> bool:
        """
        Проверка доступности папки с CSV файлами
        
        Returns:
            True если папка существует и доступна для чтения
        """
        self.is_connected = os.path.exists(self.folder_path) and os.path.isdir(self.folder_path)
        return self.is_connected
    
    def get_tables(self) -> List[str]:
        """
        Получение списка всех CSV файлов в папке
        
        Returns:
            Список имен CSV файлов
        """
        if not self.is_connected:
            raise ConnectionError("Нет подключения к папке. Сначала вызови connect()")
        
        # Ищем все файлы с расширением .csv
        csv_files = []
        for file in os.listdir(self.folder_path):
            if file.lower().endswith('.csv'):
                csv_files.append(file)
        
        return sorted(csv_files)
    
    def get_table_schema(self, table_name: str) -> Table:
        """
        Получение схемы CSV файла с примерами данных
        
        Args:
            table_name: Имя CSV файла
            
        Returns:
            Объект Table с метаданными
        """
        if not self.is_connected:
            raise ConnectionError("Нет подключения к папке")
        
        file_path = os.path.join(self.folder_path, table_name)
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Файл {table_name} не найден")
        
        try:
            # Читаем CSV с разными параметрами для лучшей совместимости
            df = pd.read_csv(file_path, nrows=5, encoding='utf-8')
        except UnicodeDecodeError:
            # Пробуем другую кодировку
            df = pd.read_csv(file_path, nrows=5, encoding='cp1251')
        except Exception as e:
            print(f"Ошибка чтения CSV {table_name}: {e}")
            # Возвращаем пустую таблицу
            return Table(
                name=table_name,
                columns=[],
                row_count=0,
                source_id=self.source_id,
                path=file_path
            )
        
        columns = []
        for col_name in df.columns:
            # Получаем примеры данных (до 3 значений)
            samples = df[col_name].head(3).tolist()
            # Заменяем NaN на None для лучшей сериализации
            samples = [None if pd.isna(x) else x for x in samples]
            
            columns.append(Column(
                name=str(col_name),
                data_type=str(df[col_name].dtype),
                sample_values=samples
            ))
        
        # Получаем общее количество строк в файле
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                row_count = sum(1 for line in f) - 1  # минус заголовок
        except:
            row_count = 0
        
        return Table(
            name=table_name,
            columns=columns,
            row_count=row_count,
            source_id=self.source_id,
            path=file_path
        )
    
    def disconnect(self):
        """Закрытие соединения (для CSV ничего делать не нужно)"""
        self.is_connected = False