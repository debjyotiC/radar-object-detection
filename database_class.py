import sqlite3
import threading


class DatabaseConnector:
    def __init__(self, db_file):
        self.conn = None
        self.db_file = db_file
        self.lock = threading.Lock()  # Create a lock for synchronization

    def connect(self):
        self.conn = sqlite3.connect(self.db_file, isolation_level=None, timeout=10, check_same_thread=False)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def create_schema(self):
        with self.lock:
            conn = sqlite3.connect(self.db_file)
            conn.execute('''
            CREATE TABLE IF NOT EXISTS obj_table (
                Obj_Detected TEXT,
                Obj_detection_flag BOOLEAN,
                Threshold REAL,
                Sum REAL
            )
            ''')

    def insert_data(self, obj_dict):
        with self.lock:
            conn = sqlite3.connect(self.db_file)
            conn.execute('''
            INSERT INTO obj_table (Obj_Detected, Obj_detection_flag, Threshold, Sum)
            VALUES (?, ?, ?, ?)
            ''', (obj_dict['Obj_Detected'], obj_dict['Obj_detection_flag'], obj_dict['Threshold'], obj_dict['Sum']))
            # Commit the transaction
            conn.commit()

            # Close the connection
            conn.close()
