"""
Скрипт для создания тестовых данных:
- SQLite база данных с тестовыми таблицами
- CSV файлы для тестирования
"""

import sqlite3
import pandas as pd
import os


def create_sqlite_db():
    """Создание тестовой SQLite базы данных"""
    print("🗄️ Создание SQLite базы данных...")
    
    # Убедимся, что папка data существует
    os.makedirs('data', exist_ok=True)
    
    # Подключаемся к базе (файл sample.db)
    conn = sqlite3.connect('data/sample.db')
    cursor = conn.cursor()
    
    # Таблица пользователей
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        username TEXT NOT NULL,
        email TEXT UNIQUE,
        age INTEGER,
        city TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Таблица продуктов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        price REAL,
        stock INTEGER,
        category TEXT
    )
    ''')
    
    # Таблица заказов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status TEXT,
        FOREIGN KEY (user_id) REFERENCES users (id),
        FOREIGN KEY (product_id) REFERENCES products (id)
    )
    ''')
    
    # Добавляем тестовые данные
    
    # Пользователи
    users_data = [
        ('alice', 'alice@email.com', 28, 'Москва'),
        ('bob', 'bob@email.com', 32, 'Санкт-Петербург'),
        ('charlie', 'charlie@email.com', 25, 'Новосибирск'),
        ('diana', 'diana@email.com', 35, 'Екатеринбург'),
        ('eve', 'eve@email.com', 29, 'Казань')
    ]
    
    cursor.executemany(
        'INSERT OR IGNORE INTO users (username, email, age, city) VALUES (?,?,?,?)',
        users_data
    )
    
    # Продукты
    products_data = [
        ('Ноутбук', 'Мощный ноутбук для работы', 999.99, 10, 'Электроника'),
        ('Мышь', 'Беспроводная мышь', 29.99, 50, 'Электроника'),
        ('Клавиатура', 'Механическая клавиатура', 89.99, 30, 'Электроника'),
        ('Монитор', '27 дюймовый 4K монитор', 399.99, 15, 'Электроника'),
        ('Книга', 'Python для начинающих', 49.99, 100, 'Книги')
    ]
    
    cursor.executemany(
        'INSERT OR IGNORE INTO products (name, description, price, stock, category) VALUES (?,?,?,?,?)',
        products_data
    )
    
    # Заказы
    orders_data = [
        (1, 1, 1, 'completed'),
        (1, 2, 2, 'pending'),
        (2, 3, 1, 'completed'),
        (3, 1, 1, 'cancelled'),
        (2, 4, 1, 'completed')
    ]
    
    cursor.executemany(
        'INSERT OR IGNORE INTO orders (user_id, product_id, quantity, status) VALUES (?,?,?,?)',
        orders_data
    )
    
    conn.commit()
    conn.close()
    
    print("SQLite база создана: data/sample.db")


def create_csv_files():
    """Создание тестовых CSV файлов"""
    print("\nСоздание CSV файлов...")
    
    os.makedirs('data', exist_ok=True)
    
    # 1. Сотрудники (employees.csv)
    employees_data = {
        'employee_id': [101, 102, 103, 104, 105],
        'first_name': ['Иван', 'Мария', 'Петр', 'Анна', 'Сергей'],
        'last_name': ['Иванов', 'Петрова', 'Сидоров', 'Смирнова', 'Козлов'],
        'department': ['IT', 'HR', 'IT', 'Sales', 'Sales'],
        'salary': [75000, 65000, 80000, 70000, 72000],
        'hire_date': ['2020-01-15', '2019-03-20', '2021-06-01', '2020-11-10', '2018-05-25']
    }
    
    df = pd.DataFrame(employees_data)
    df.to_csv('data/employees.csv', index=False, encoding='utf-8')
    print("Создан: data/employees.csv")
    
    # 2. Отделы (departments.csv)
    departments_data = {
        'dept_id': ['IT', 'HR', 'Sales', 'Marketing'],
        'dept_name': ['Информационные технологии', 'Кадры', 'Продажи', 'Маркетинг'],
        'location': ['Москва', 'Москва', 'Санкт-Петербург', 'Казань'],
        'budget': [1000000, 500000, 1500000, 800000]
    }
    
    df = pd.DataFrame(departments_data)
    df.to_csv('data/departments.csv', index=False, encoding='utf-8')
    print("Создан: data/departments.csv")
    
    # 3. Продажи (sales.csv)
    import random
    from datetime import datetime, timedelta
    
    sales_data = []
    start_date = datetime(2023, 1, 1)
    
    for i in range(50):
        date = start_date + timedelta(days=random.randint(0, 365))
        sales_data.append({
            'sale_id': i + 1,
            'product': random.choice(['Ноутбук', 'Мышь', 'Клавиатура', 'Монитор']),
            'amount': random.randint(1, 10),
            'price': round(random.uniform(10, 1000), 2),
            'sale_date': date.strftime('%Y-%m-%d'),
            'customer': f'customer_{random.randint(1, 20)}@email.com'
        })
    
    df = pd.DataFrame(sales_data)
    df.to_csv('data/sales.csv', index=False, encoding='utf-8')
    print("Создан: data/sales.csv")


if __name__ == '__main__':
    print("Создание тестовых данных...")
    print("=" * 40)
    
    create_sqlite_db()
    create_csv_files()
    
    print("\n" + "=" * 40)
    print("Все тестовые данные созданы!")
    print("Папка data содержит:")
    for f in os.listdir('data'):
        print(f"   - {f}")