"""
Модели данных для представления метаинформации об источниках,
таблицах и колонках.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Any
from datetime import datetime


@dataclass
class Column:
    """Модель колонки таблицы"""
    name: str
    data_type: str
    sample_values: List[Any] = field(default_factory=list)
    
    def __repr__(self):
        return f"Column(name='{self.name}', type={self.data_type})"


@dataclass
class Table:
    """Модель таблицы (или CSV файла)"""
    name: str
    columns: List[Column]
    row_count: int = 0
    source_id: str = ""
    path: str = ""  # файл или таблица в БД
    
    def __repr__(self):
        return f"Table(name='{self.name}', columns={len(self.columns)}, rows={self.row_count})"


@dataclass
class Source:
    """Модель источника данных (SQLite БД или папка с CSV)"""
    id: str
    type: str  # 'sqlite', 'csv'
    connection_string: str
    tables: List[Table] = field(default_factory=list)
    last_indexed: Optional[datetime] = None
    
    def __repr__(self):
        return f"Source(id='{self.id}', type={self.type}, tables={len(self.tables)})"