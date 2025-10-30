import csv

input_file = 'channels_with_emails.csv'
output_file = 'channels_with_emails_cleaned.csv'

with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
     open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    
    # Read header
    headers = next(reader)
    
    # Find the index of 'other_links' column
    if 'other_links' in headers:
        other_links_index = headers.index('other_links')
        # Write header without 'other_links'
        new_headers = [h for h in headers if h != 'other_links']
        writer.writerow(new_headers)
        
        # Process the rest of the rows
        for row in reader:
            if len(row) > other_links_index:
                new_row = row[:other_links_index] + row[other_links_index+1:]
                writer.writerow(new_row)
            else:
                writer.writerow(row)
    else:
        # If 'other_links' column doesn't exist, just copy the file
        writer.writerow(headers)
        for row in reader:
            writer.writerow(row)

print(f"File processed successfully. Cleaned data saved to '{output_file}'")
