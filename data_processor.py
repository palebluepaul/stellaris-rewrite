import os

def process_data(connection_string):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # Fetch unprocessed rows
    cursor.execute("SELECT * FROM your_table WHERE processed = 'N'")
    unprocessed_rows = cursor.fetchall()

    for row in unprocessed_rows:
        # Process the data (add your processing logic here)
        processed_content = process_row(row)

        # Update the database with the processed data
        cursor.execute("""
            UPDATE your_table
            SET rewritten_content = ?, processed = 'Y'
            WHERE unique_row_id = ?
        """, (processed_content, row.unique_row_id))
        conn.commit()

    cursor.close()
    conn.close()
    print("Data processed successfully.")


def process_row(row):
    # Add your processing logic here and return the processed content
    processed_content = row.content  # Example: Keeping the original content
    return processed_content
