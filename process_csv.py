import csv
import sys

# Increase the field size limit
maxInt = sys.maxsize
while True:
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/2)

def clean_csv(input_file, output_file):
    with open(input_file, 'r', newline='', encoding='utf-8', errors='replace') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        try:
            # Read header
            headers = next(reader)
            
            # Find the index of 'other_links' column if it exists
            if 'other_links' in headers:
                other_links_index = headers.index('other_links')
                # Write header without 'other_links'
                new_headers = [h for i, h in enumerate(headers) if h != 'other_links']
                writer.writerow(new_headers)
                
                # Process the rest of the rows
                for row in reader:
                    try:
                        if len(row) > other_links_index:
                            new_row = [row[i] for i in range(len(row)) if i != other_links_index]
                            writer.writerow(new_row)
                        else:
                            # If row is shorter than expected, write as is
                            writer.writerow(row)
                    except Exception as e:
                        print(f"Warning: Error processing row: {e}")
                        continue
            else:
                # If 'other_links' column doesn't exist, just copy the file
                writer.writerow(headers)
                writer.writerows(reader)
                
        except Exception as e:
            print(f"Error: {e}")
            return False
            
    return True

if __name__ == "__main__":
    input_file = 'channels_with_emails.csv'
    output_file = 'channels_with_emails_cleaned.csv'
    
    print(f"Processing {input_file}...")
    if clean_csv(input_file, output_file):
        print(f"Success! Cleaned data saved to '{output_file}'")
    else:
        print("Failed to process the file. Please check for any error messages above.")
