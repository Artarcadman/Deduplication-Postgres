import time
import psycopg2
import hashlib
import os
from dotenv import load_dotenv

# --- Load /.env variables --- 
load_dotenv()
HOST = os.getenv("HOST")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

# --- PosgreSQL Configuration ---
DB_CONFIG = {
    "host": HOST,
    "database": DATABASE,  
    "user": USER,            
    "password": PASSWORD
}

CHUNK_SIZE = 4  # Size of segment

# --- Input file choice ---
def select_file(directory = "./origin_data"):
    """
    Makes list of files in "./origin_data" directory
    """
    try:
        
        all_entries = os.listdir(directory)
        files = []
        for entry in all_entries:
            full_path = os.path.join(directory, entry)
            if os.path.isfile(full_path) and not entry.startswith('.gitkeep'):
                files.append(entry)
            
    except ValueError:
        print("Directory not found")
        return None
    
    if not files:
        print(f'Directory "{directory}" has no files')
        return None
    
    print(f'Available files in "{directory}":\n')
    for i, file in enumerate(files):
        print(f"{i+1}. {file}")

    while True:
        choice = input(f"\nChoose file (1 - {len(files)})")
        try:
            index = int(choice)-1
            if 0 <= index < len(files):
                return os.path.join(directory, files[index])
            else:
                print("This file not exists. Try your BEST again")
        except ValueError:
            print("Only numbers")
   
    
def connect_db():
    """Connect to PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Can't connect to PostgreSQL. Check PostgreSQL launcher, db name/user/password. Error: {e}")
        return None
    
    
def check_file_processed(conn, filepath):
    """Make hash of FULL file and check if it was done before"""
    # Make HASH for file
    hasher = hashlib.sha256()
    with open(filepath, 'rb') as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    full_hash = hasher.hexdigest()
    
    # Check HASH in database
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed_files WHERE file_hash = %s", (full_hash,))
    
    if cursor.fetchone():
        print(f'File "{filepath}" ALREADY PROCESSED in this database!')
        return True, full_hash
    
    return False, full_hash # return full_hash for insert in database for first time


def register_file(conn, filename, file_hash):
    """Insert file record into processed_files table after succesful processing"""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO processed_files (file_name, file_hash)
            VALUES (%s, %s)
            """, 
            (os.path.basename(filename), file_hash)
        )
        conn.commit()
        print(f'File "{filename}" succesfully registered in database')
    except Exception as e:
        print(f"Error registering file: {e}")
        conn.rollback()


def process_file_chunks(conn, filename, chunk_size):
    """
    Reading binary file (4 bytes per segment), hash-function,
    keep data segments, update/insert DATA
    
    """
    if not os.path.exists(filename):
        print(f"Error. File: '{filename}' not found.")
        return

    cursor = conn.cursor()
    file_size = os.path.getsize(filename)
    segment_offset = 0  # byte offset
    processed_count = 0
    start_time = time.time()
    
    print(f'Starting chunk processing. Size: {file_size} bytes...')
    print(f'Total segment count is about {file_size/chunk_size}...')
    
    
    # 1. Reading file in banary (rb)
    with open(filename, 'rb') as f: 
        while True:
            # Read a segment (4 bytes)
            chunk = f.read(chunk_size)
            if not chunk:
                break

            # HASH function
            hash_object = hashlib.sha256(chunk)
            hash_value = hash_object.hexdigest()

            # UPSERT: Try to update counter if we already have HASH
            cursor.execute(
                """
                UPDATE file_chunks
                SET repetition_count = repetition_count + 1
                WHERE hash_value = %s
                """,
                (hash_value,)
            )

            #  If hash is new, do INSERT
            if cursor.rowcount == 0:
        
                cursor.execute(
                    """
                    INSERT INTO file_chunks (hash_value, file_name, segment_offset, repetition_count, chunk_data)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (hash_value, os.path.basename(filename), segment_offset, 1, chunk) 
                )

            segment_offset += chunk_size
            processed_count += 1

            if processed_count % 10000 == 0:
                print(f"Progress... {processed_count} segments done...")

    conn.commit()
    end_time = time.time()
    
    print("-" * 40)
    print(f"   Processing done")
    print(f"   File: {filename}")
    print(f"   Segment size: {chunk_size} bytes")
    print(f"   Total time: {end_time - start_time:.2f} sec.")
    print(f"   Total segments count: {processed_count}")
    print("-" * 40)


if __name__ == "__main__":
    
    # 1. Select File
    selected_file = select_file(directory=r".\origin_data")
    
    if selected_file:
        print(f"Selected: {selected_file}")
        
        # 2. Connect DB
        db_connection = connect_db()
    
        if db_connection:
            try:
                # 3. Check if it is processed
                is_processed, file_hash = check_file_processed(db_connection, selected_file)
                
                if not is_processed:
                    # 4. Process chunks (for new only)
                    process_file_chunks(db_connection, selected_file, CHUNK_SIZE)
                    
                    # 5. Register file in registry
                    register_file(db_connection, selected_file, file_hash)
                else:
                    print("Skipping processing")
            except Exception as e:
                print(f"Critical error during execution: {e}")
            finally:
                db_connection.close()
                print("\nDB Connection closed")
        else:
            print("No file selected. Go to sleep")