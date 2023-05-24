import openai
import time
import pyodbc
import logging
import concurrent.futures

# Configure logging
logging.basicConfig(filename='data_processor.log', level=logging.INFO)


def submit_to_api(prompt):
    max_retry_count = 5
    retry_count = 0
    while retry_count < max_retry_count:
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[{"role": "system", "content": prompt}, {"role": "user", "content": prompt}],
                max_tokens=3000,
                n=1,
                stop=None,
                temperature=0,
            )
            return response.choices[0].message['content'].strip()
        except Exception as e:
            print(f"Error: {e}")
            retry_count += 1
            time.sleep(5)
    return None

def validate_record(content, rewritten_content):
    if rewritten_content is None or rewritten_content == "":
        return True
    if not (rewritten_content.startswith('"') and rewritten_content.endswith('"')):
        return False
    return True

def process_data(connection_string):
    batch_size = 20

    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # Fetch unprocessed rows
    cursor.execute("SELECT * FROM stellaris_mod_strings WHERE processed = 'N'")
    unprocessed_rows = cursor.fetchall()

    for i in range(0, len(unprocessed_rows), batch_size):
        batch_rows = unprocessed_rows[i:i + batch_size]
        prompt = (
        "As a hard sci-fi author in Alastair Reynolds' style with a serious tone, rewrite game file content:\n"
        "Process input lines without altering their structure.\n"
        "Do not rewrite tags (content immediately before a colon at the start of the line).\n"
        "Preserve tags, text length, special characters, and variables. Do not rewrite content which already has an appropriate tone or is well known military language.\n"
        "Text to rewrite:\n"
        )

        for row in batch_rows:
            prompt += f"{row.tag}: {row.content}\n"
        
        print(f"Prompt: {prompt}")
        rewritten_data = submit_to_api(prompt)
        if rewritten_data is None:
            print("No data returned after 5 attempts.")
            continue
        print(f"Rewritten data: {rewritten_data}")
        rewritten_rows = rewritten_data.split("\n")

        for row_data in rewritten_rows:
            if row_data:
                try:
                    tag, content = row_data.split(": ", 1)
                except ValueError:
                    print(f"Error with row_data: {row_data}")
                    continue

                # Validate the record  
                original_row = next((row for row in batch_rows if row.tag == tag), None)  
                if original_row and validate_record(original_row.content, content):  
                    # Update the database with the processed data  
                    update_success = False
                    while not update_success:
                        try:
                            cursor.execute("""  
                            UPDATE stellaris_mod_strings  
                            SET rewritten_content = ?, processed = 'Y'  
                            WHERE tag = ?  
                            """, (content, tag))  
                            conn.commit()  
                            update_success = True
                        except pyodbc.Error as e:
                            print(f"Database error: {e}")
                            print("Reconnecting and retrying...")
                            if conn is not None:
                                 conn.close()
                            conn = pyodbc.connect(connection_string)  
                            cursor = conn.cursor() 
                            cursor.execute("""  
                            UPDATE stellaris_mod_strings  
                            SET rewritten_content = ?, processed = 'Y'  
                            WHERE tag = ?  
                            """, (content, tag))  
                            conn.commit()  
                            update_success = True

            time.sleep(1) 

        time.sleep(1)
        print(f"Batch {i + 1} of {len(unprocessed_rows)} processed.")

    cursor.close()
    conn.close()
    print("Data processed successfully.")

    
