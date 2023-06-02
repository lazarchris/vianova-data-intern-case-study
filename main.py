from CityPopulationAnalyser import *

# Specify the URL of the XLSX file and the desired save path
xlsx_url = "https://tinyurl.com/yzhtem69"
file_path = "city_population.xlsx"
db_path = "city_population.db"


# Create DbManager
colomns_used = ["Name", "Country name EN", "Country Code","Population"]
db_manager = DbManager(file_path, db_path, colomns_used)

# Create an instance of the CityPopulationAnalyzer class
analyzer = CityPopulationAnalyzer(db_manager)

# Download the XLSX file
analyzer.download_xlsx_file(xlsx_url)

# Create the database
conn = analyzer.create_database()

# # Insert XLSX data into the database
analyzer.insert_csv_data_to_database()

# # Retrieve the countries that don't host a megapolis
non_megapolis_countries = analyzer.retrieve_non_megapolis_countries()

results_file = "non_megapolis_countries.txt"
analyzer.write_to_file(non_megapolis_countries, results_file)

# Close the database connection
analyzer.exit()
