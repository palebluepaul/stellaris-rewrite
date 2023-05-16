import os
import pyodbc


def load_data(run_id, directory, connection_string):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    for file in os.listdir(directory):
        with open(os.path.join(directory, file), 'r') as f:
            for line in f:
                if ':' in line:
                    tag, content = line.split(':', 1)
                    store_data(cursor, run_id, file, tag, content.strip())

    cursor.close()
    conn.close()
    print("Data loaded successfully.")


def store_data(cursor, run_id, file_name, tag, content):
    cursor.execute("""
        INSERT INTO your_table (run_id, file_name, tag, content, rewritten_content, processed)
        OUTPUT INSERTED.ID
        VALUES (?, ?, ?, ?, ?, ?)
    """, (run_id, file_name, tag, content, '', 'N'))
    sequence = cursor.fetchone()[0]
    unique_row_id = f"SPROC{run_id}{sequence}"
    cursor.execute("""
        UPDATE your_table
        SET unique_row_id = ?
        WHERE ID = ?
    """, (unique_row_id, sequence))
    cursor.commit()
