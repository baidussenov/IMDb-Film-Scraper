import pandas as pd
import logging

read_file = pd.read_csv (r'combined_film_data.csv')
read_file.to_excel (r'combined_film_data.xlsx', index = None, header=True)
logging.info("Converted CSV to Excel")