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
    if content is None:
        processed = 'Y'
    else:
        processed = 'N'

    try:
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM stellaris_mod_strings WHERE tag = ?)
            BEGIN
                INSERT INTO stellaris_mod_strings (run_id, file_name, tag, content, rewritten_content, processed)
                VALUES (?, ?, ?, ?, ?, ?)
            END
        """, (tag, run_id, file_name, tag, content, '', 'N'))
        cursor.execute("SELECT @@IDENTITY")
        sequence = cursor.fetchone()
        
        if sequence is not None:
            sequence = sequence[0]
            unique_row_id = f"SPROC{run_id}{sequence}"
            cursor.execute("""
                UPDATE stellaris_mod_strings
                SET unique_row_id = ?
                WHERE ID = ?
            """, (unique_row_id, sequence))
            cursor.commit()
    except pyodbc.ProgrammingError as error:
        print(f"Error: {error}")