"""
Веб-интерфейс для Data Discovery Tool на Flask.
Позволяет искать и просматривать данные через браузер.
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from flask import Flask, render_template, request, jsonify
from mcp.tools import MCPTool

app = Flask(__name__)
tool = MCPTool()


@app.route('/')
def index():
    """Главная страница с поиском"""
    sources = tool.list_sources()
    return render_template('web_index.html', sources=sources)


@app.route('/search')
def search():
    """API для поиска"""
    query = request.args.get('q', '')
    if len(query) < 2:
        return jsonify([])
    
    results = tool.search(query)
    return jsonify(results)


@app.route('/sources')
def list_sources():
    """Список источников"""
    sources = tool.list_sources()
    return jsonify(sources)


@app.route('/schema/<source_id>/<path:table_path>')
def get_schema(source_id, table_path):
    """Получить схему таблицы"""
    schema = tool.get_schema(source_id, table_path)
    return jsonify(schema)


@app.route('/index/<source_id>', methods=['POST'])
def index_source(source_id):
    """Индексировать источник"""
    result = tool.index_source(source_id)
    return jsonify(result)


@app.route('/stats')
def stats():
    """Статистика системы"""
    return jsonify(tool.get_stats())


@app.route('/add-source', methods=['POST'])
def add_source():
    """Добавить новый источник"""
    data = request.json
    result = tool.add_source(
        data['source_id'],
        data['source_type'],
        data['connection_string']
    )
    return jsonify(result)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)