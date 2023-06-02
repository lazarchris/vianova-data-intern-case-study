import os
import pandas as pd
import sqlite3


class DbManager:
    def __init__(self, file_path, db_path, columns_used):
        self.file_path = file_path
        self.db_path = db_path
        self.columns_used = columns_used
        self.connection = None

    def load_data_db(self):
        if not os.path.exists(self.file_path):
            print("Excel file does not exist.")
            return False

        self.db = pd.read_excel(self.file_path)
        self.db = self.db[self.columns_used]

        self.connection = sqlite3.connect(self.db_path)
        return True

    def create_table(self, table_name, table_format):
        cursor = self.connection.cursor()
        command = f"CREATE TABLE IF NOT EXISTS {table_name} ({table_format});"
        cursor.execute(command)

    def insert_data(self, sql_command, supplied_columns_data):
        cursor = self.connection.cursor()

        for _, row in self.db.iterrows():
            values = tuple(row[col] for col in supplied_columns_data)
            cursor.execute(sql_command, values)

        self.connection.commit()

    def print_table(self, table_name):
        cursor = self.connection.cursor()
        select_query = f"SELECT * FROM {table_name}"
        cursor.execute(select_query)
        data = cursor.fetchall()

        for row in data:
            print(row)

    def fetch_data(self, table_name):
        cursor = self.connection.cursor()
        select_query = f"SELECT * FROM {table_name}"
        cursor.execute(select_query)
        data = cursor.fetchall()
        return data

    def close_connection(self):
        self.connection.close()
