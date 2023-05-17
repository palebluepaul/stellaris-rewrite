# Stellaris Rewrite

This repository contains scripts for loading, processing, and writing game data for Stellaris. The scripts connect to a database, fetch and process data in batches using the OpenAI API, and write the processed data to output files.

## Scripts

The repository contains three main scripts:

- `main.py`: This is the primary entry point for the application. It takes command-line arguments for `run_id`, `input_directory`, and `output_directory`, and calls the functions `load_data()`, `process_data()`, and `write_output_files()` with the respective parameters.

- `data_processor.py`: This script fetches unprocessed rows from the database, submits the data to the OpenAI API for rewriting, validates the rewritten data, and updates the database to indicate that the data has been processed.

- `write_output.py`: This script fetches processed data from the database based on a given `run_id` and writes the data to new files in the specified `output_directory`.

## Usage

Run the `main.py` script with the following command:

```bash
python main.py <run_id> <input_directory> <output_directory>
```

Replace `<run_id>`, `<input_directory>`, and `<output_directory>` with your desired parameters.

## Requirements

- Python 3
- Libraries: `os`, `sys`, `openai`, `time`, `pyodbc`, `dotenv`
- An Azure SQL Database
- An OpenAI API key

Please ensure you have the correct environment variables set for your Azure SQL connection string and OpenAI API key.

