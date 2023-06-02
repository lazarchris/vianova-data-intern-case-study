import requests
import csv
import os.path
from DBManager import *
from tabulate import tabulate


class CityPopulationAnalyzer:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def download_xlsx_file(self, url):
        # This condition can be removed if this script runs often, commented for convenience to test locally as downloading takes some time
        if os.path.exists(self.db_manager.file_path):
            print("XLSX file already exists.")
            return
        try:
            print("File is downloading. Please wait...")
            response = requests.get(url)
            response.raise_for_status()
            with open(self.db_manager.file_path, "wb") as file:
                file.write(response.content)

            print("File is downloaded successfully.")

        except requests.exceptions.RequestException as e:
            print("Error: ", e)

    def create_database(self):
        print("Loading  file. It may take some time. Please wait ..")
        if self.db_manager.load_data_db():
            table_name = "city_population"
            table_structure = """
                CITY_NAME TEXT PRIMARY KEY, COUNTRY TEXT, COUNTRY_CODE TEXT, POPULATION INTEGER """
            self.db_manager.create_table(table_name, table_structure)
            print("The xlsx is loaded ..")

    def insert_csv_data_to_database(self):
        print("Writing data to db ..")
        sql_command = "INSERT OR IGNORE INTO city_population (CITY_NAME, COUNTRY, COUNTRY_CODE,POPULATION) VALUES (?, ?, ?,?);"
        self.db_manager.insert_data(sql_command, self.db_manager.columns_used)
        print("Data written to the db successfully ..")

    def retrieve_non_megapolis_countries(self):
        cursor = self.db_manager.connection.cursor()

        cursor.execute(
            " SELECT DISTINCT COUNTRY_CODE, COUNTRY FROM city_population WHERE COUNTRY NOT IN (SELECT DISTINCT COUNTRY FROM city_population WHERE POPULATION >= 10000000) ORDER BY COUNTRY ASC;"
        )
        non_megapolis_countries = cursor.fetchall()

        return non_megapolis_countries

    def write_to_file(self, results, file_path):
        with open(file_path, "w") as file:
            headers = ["Country Code", "Country"]
            table = tabulate(results, headers, tablefmt="presto")
            file.write(table)
        print(f"Results are generated into {file_path}")

    def exit(self):
        self.db_manager.close_connection()
