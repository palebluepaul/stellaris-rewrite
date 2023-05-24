import os
import pyodbc

def write_output_files(input_directory, output_directory, connection_string):  
    conn = pyodbc.connect(connection_string)  
    cursor = conn.cursor()  

    cursor.execute("SELECT * FROM stellaris_mod_strings WHERE processed = 'Y'")  
    rows = cursor.fetchall()  

    # Build a dictionary of processed strings for easy access
    processed_strings = {(row.file_name, row.tag): row.rewritten_content for row in rows}

    for input_file_name in os.listdir(input_directory):
        with open(os.path.join(input_directory, input_file_name), 'r') as input_file:
            lines = input_file.readlines()

        output_lines = []
        for line in lines:
            if ':' in line:
                tag, _ = line.split(':', 1)
                tag = tag.strip()  # To remove leading/trailing spaces
                rewritten_content = processed_strings.get((input_file_name, tag))

                # If we found a processed string, use it; otherwise, keep the original line
                if rewritten_content is not None:
                    output_lines.append(f"{tag}: {rewritten_content}\n")
                else:
                    output_lines.append(line)
            else:
                output_lines.append(line)  # lines without tags are kept as is

        # Write the output file
        with open(os.path.join(output_directory, input_file_name), 'w') as output_file:  
            output_file.writelines(output_lines)

    cursor.close()  
    conn.close()  
    print("Output files written successfully.")

