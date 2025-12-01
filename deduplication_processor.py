import psycopg2
import hashlib
import os
import time

# --- КОНФИГУРАЦИЯ БАЗЫ ДАННЫХ (PostgreSQL) ---
DB_CONFIG = {
    "host": "localhost",
    "database": "deduplication_db",
    "user": "postgres",
    "password": "artem"
}

# --- КОНФИГУРАЦИЯ АЛГОРИТМА ---
INPUT_FILENAME = "text_data.txt"
CHUNK_SIZE = 4  # Размер сегмента в байтах

def connect_db():
    """Устанавливает соединение с PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД. Проверьте: запущен ли PostgreSQL, верны ли user/password. Ошибка: {e}")
        return None

def process_file(conn, filename, chunk_size):
    """
    Чтение бинарного файла по сегментам (4 байта), вычисление хэша и
    обновление/вставка данных в таблицу file_chunks.
    """
    if not os.path.exists(filename):
        print(f"Ошибка: Файл '{filename}' не найден.")
        return

    cursor = conn.cursor()
    file_size = os.path.getsize(filename)
    segment_offset = 0  # Смещение в байтах
    processed_count = 0
    start_time = time.time()
    
    # 1. ОТКРЫТИЕ ФАЙЛА В БИНАРНОМ РЕЖИМЕ ('rb')
    with open(filename, 'rb') as f: 
        while True:
            # Читаем фиксированный сегмент (4 байта)
            chunk = f.read(chunk_size)
            if not chunk:
                break

            # Вычисляем хэш
            hash_object = hashlib.sha256(chunk)
            hash_value = hash_object.hexdigest()

            # UPSERT: Сначала пытаемся обновить счетчик (если хэш уже есть)
            cursor.execute(
                """
                UPDATE file_chunks
                SET repetition_count = repetition_count + 1
                WHERE hash_value = %s
                """,
                (hash_value,)
            )

            # Если обновление не затронуло ни одной строки (хэш новый), делаем INSERT
            if cursor.rowcount == 0:
                cursor.execute(
                    """
                    INSERT INTO file_chunks (hash_value, file_name, segment_offset, repetition_count)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (hash_value, filename, segment_offset, 1)
                )

            segment_offset += chunk_size
            processed_count += 1

            if processed_count % 10000 == 0:
                print(f"Прогресс: обработано {processed_count} сегментов...")

    conn.commit() # Фиксируем все изменения
    end_time = time.time()
    
    print("-" * 40)
    print(f"✅ Обработка завершена.")
    print(f"   Файл: {filename}")
    print(f"   Размер сегмента: {chunk_size} байт")
    print(f"   Общее время: {end_time - start_time:.2f} сек.")
    print(f"   Общее количество сегментов: {processed_count}")
    print("-" * 40)


if __name__ == "__main__":
    db_connection = connect_db()
    
    if db_connection:
        process_file(db_connection, INPUT_FILENAME, CHUNK_SIZE)
        db_connection.close()