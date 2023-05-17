import os
import pyodbc

def write_output_files(run_id, output_directory, connection_string):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM stellaris_mod_strings WHERE run_id = ?", (run_id,))
    rows = cursor.fetchall()

    file_map = {}

    for row in rows:
        if row.file_name not in file_map:
            file_map[row.file_name] = []

        file_map[row.file_name].append(f"{row.tag}: {row.rewritten_content}\n")

    for file_name, file_content in file_map.items():
        with open(os.path.join(output_directory, file_name), 'w') as output_file:
            output_file.writelines(file_content)

    cursor.close()
    conn.close()
    print("Output files written successfully.")
