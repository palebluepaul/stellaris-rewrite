import os
import sys
from data_loader import load_data
from data_processor import process_data
from write_output import write_output_files
from dotenv import load_dotenv
import pyodbc

load_dotenv()

def main(run_id, input_directory, output_directory):
    connection_string = os.environ['AZURE_SQL_CONNECTION_STRING']

    load_data(run_id, input_directory, connection_string)
    process_data(connection_string)
    write_output_files(run_id, output_directory, connection_string)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python main.py <run_id> <input_directory> <output_directory>")
        sys.exit(1)

    run_id_param = sys.argv[1]
    input_directory_param = sys.argv[2]
    output_directory_param = sys.argv[3]
    main(run_id_param, input_directory_param, output_directory_param)
