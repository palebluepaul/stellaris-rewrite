import openai
import time
import pyodbc

def submit_to_api(prompt):
    max_retry_count = 5
    retry_count = 0
    while retry_count < max_retry_count:
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[{"role": "system", "content": prompt}, {"role": "user", "content": prompt}],
                max_tokens=2000,
                n=1,
                stop=None,
                temperature=0,
            )
            return response.choices[0].message['content'].strip()
        except Exception as e:
            print(f"Error: {e}")
            retry_count += 1
            time.sleep(5)
                       


def process_data(connection_string):
    batch_size = 30

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
        "Preserve tags, text length, special characters, and variables.\n"
        "Text to rewrite:\n"
        )

        for row in batch_rows:
            prompt += f"{row.tag}: {row.content}\n"
        
        print(f"Prompt: {prompt}")
        rewritten_data = submit_to_api(prompt)
        print(f"Rewritten data: {rewritten_data}")
        rewritten_rows = rewritten_data.split("\n")

        for row_data in rewritten_rows:
            if row_data:
                try:
                    tag, content = row_data.split(": ", 1)
                except ValueError:
                    print(f"Error with row_data: {row_data}")
                    continue

                # Update the database with the processed data
                cursor.execute("""
                    UPDATE stellaris_mod_strings
                    SET rewritten_content = ?, processed = 'Y'
                    WHERE tag = ?
                """, (content, tag))
                conn.commit()

        time.sleep(1)

    cursor.close()
    conn.close()
    print("Data processed successfully.")