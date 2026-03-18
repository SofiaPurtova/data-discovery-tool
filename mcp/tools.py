"""
MCP (Model Context Protocol) инструменты для AI-агента.
Предоставляет API для взаимодействия с системой.
"""

import json
import os
from typing import List, Dict, Any, Optional
from index.indexer import Indexer
from search.searcher import Searcher


class MCPTool:
    """MCP инструменты для AI-агента"""
    
    def __init__(self, state_file: str = "mcp_state.json"):
        """
        Инициализация MCP инструментов
        
        Args:
            state_file: Файл для сохранения состояния
        """
        self.state_file = state_file
        self.indexer = Indexer()
        self.searcher = Searcher(self.indexer)
        
        # Загружаем сохранённое состояние
        self._load_state()
    
    def _load_state(self):
        """Загрузить состояние из файла"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    
                # Восстанавливаем источники и их таблицы
                for source_data in state.get('sources', []):
                    try:
                        # Добавляем источник
                        self.indexer.add_source(
                            source_data['id'],
                            source_data['type'],
                            source_data['connection_string']
                        )
                        
                        # Восстанавливаем таблицы, если они есть
                        if 'tables' in source_data and source_data['tables']:
                            self.indexer.load_tables(
                                source_data['id'],
                                source_data['tables']
                            )
                            
                            # Восстанавливаем время индексации
                            source = self.indexer.get_source(source_data['id'])
                            if source and source_data.get('last_indexed'):
                                from datetime import datetime
                                source.last_indexed = datetime.fromisoformat(source_data['last_indexed'])
                                
                    except Exception as e:
                        print(f"Ошибка загрузки источника {source_data.get('id')}: {e}")
                        
                print(f"Загружено {len(state.get('sources', []))} источников из {self.state_file}")
            except Exception as e:
                print(f"Ошибка загрузки состояния: {e}")

    def _save_state(self):
        """Сохранить состояние в файл"""
        try:
            sources = self.indexer.get_all_sources()
            state = {
                'sources': []
            }
            
            for s in sources:
                source_dict = {
                    'id': s.id,
                    'type': s.type,
                    'connection_string': s.connection_string,
                    'last_indexed': s.last_indexed.isoformat() if s.last_indexed else None,
                    'tables': []
                }
                
                # Сохраняем информацию о таблицах
                for table in s.tables:
                    table_dict = {
                        'name': table.name,
                        'path': table.path,
                        'row_count': table.row_count,
                        'columns': []
                    }
                    
                    for col in table.columns:
                        table_dict['columns'].append({
                            'name': col.name,
                            'type': col.data_type,
                            'sample_values': [str(v) if v is not None else None for v in col.sample_values]
                        })
                    
                    source_dict['tables'].append(table_dict)
                
                state['sources'].append(source_dict)
            
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
                
            print(f"Состояние сохранено в {self.state_file}")
                
        except Exception as e:
            print(f"Ошибка сохранения состояния: {e}")
    
    def list_sources(self) -> List[Dict[str, Any]]:
        """
        listSources() - список всех источников данных
        
        Returns:
            Список источников с метаданными
        """
        sources = self.indexer.get_all_sources()
        return [
            {
                "id": s.id,
                "type": s.type,
                "connection_string": s.connection_string,
                "tables_count": len(s.tables),
                "last_indexed": s.last_indexed.isoformat() if s.last_indexed else None,
                "status": "indexed" if s.last_indexed else "not_indexed"
            }
            for s in sources
        ]
    
    def index_source(self, source_id: str) -> Dict[str, Any]:
        """
        indexSource(sourceId) - индексировать источник
        
        Args:
            source_id: Идентификатор источника
            
        Returns:
            Результат индексации
        """
        try:
            source = self.indexer.index_source(source_id)
            if source:
                # Сохраняем состояние после индексации
                self._save_state()
                
                return {
                    "success": True,
                    "source_id": source.id,
                    "tables_indexed": len(source.tables),
                    "timestamp": source.last_indexed.isoformat() if source.last_indexed else None,
                    "message": f"Источник {source_id} успешно проиндексирован"
                }
            else:
                return {
                    "success": False,
                    "source_id": source_id,
                    "error": "Не удалось проиндексировать источник"
                }
        except Exception as e:
            return {
                "success": False,
                "source_id": source_id,
                "error": str(e)
            }
    
    def search(self, query: str, case_sensitive: bool = False) -> List[Dict[str, Any]]:
        """
        search(query) - поиск по ключевому слову
        
        Args:
            query: Поисковый запрос
            case_sensitive: Учитывать регистр
            
        Returns:
            Список результатов поиска
        """
        results = self.searcher.search(query, case_sensitive)
        return [r.to_dict() for r in results]
    
    def get_schema(self, source_id: str, path: str) -> Dict[str, Any]:
        """
        getSchema(sourceId, path) - получить схему таблицы
        
        Args:
            source_id: Идентификатор источника
            path: Путь к таблице (имя таблицы или файла)
            
        Returns:
            Схема таблицы или сообщение об ошибке
        """
        # Для CSV файлов извлекаем только имя, если пришёл полный путь
        if '\\' in path or '/' in path:
            path = os.path.basename(path)
            
        schema = self.searcher.get_table_schema(source_id, path)
        if not schema:
            return {
                "error": "Таблица не найдена",
                "source_id": source_id,
                "path": path
            }
        
        return schema
    
    def add_source(self, source_id: str, source_type: str, connection_string: str) -> Dict[str, Any]:
        """
        Добавить новый источник данных
        
        Args:
            source_id: Уникальный идентификатор источника
            source_type: Тип источника ('sqlite' или 'csv')
            connection_string: Строка подключения
            
        Returns:
            Результат добавления
        """
        try:
            self.indexer.add_source(source_id, source_type, connection_string)
            # Сохраняем состояние после добавления
            self._save_state()
            
            return {
                "success": True,
                "source_id": source_id,
                "type": source_type,
                "connection_string": connection_string,
                "message": f"Источник {source_id} добавлен"
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Получить статистику системы
        
        Returns:
            Статистика по индексатору
        """
        return self.indexer.get_stats()
    
    def get_suggestions(self, prefix: str, limit: int = 5) -> List[str]:
        """
        Получить подсказки для поиска
        
        Args:
            prefix: Начало названия
            limit: Максимальное количество подсказок
            
        Returns:
            Список подсказок
        """
        return self.searcher.get_search_suggestions(prefix, limit)