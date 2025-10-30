#!/usr/bin/env python3
"""
Filter CSV file to only include rows with email addresses.
Usage: python filter_emails.py input.csv output.csv
"""

import csv
import re
import sys
from typing import List, Dict, Any, TextIO

# Increase the field size limit
csv.field_size_limit(2**31 - 1)  # Maximum value for 32-bit integer

def is_valid_email(email: str) -> bool:
    """Check if a string is a valid email address."""
    if not email or not isinstance(email, str):
        return False
    email = email.strip()
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(email_regex, email))

def contains_email(text: str) -> bool:
    """Check if text contains any valid email addresses."""
    if not text or not isinstance(text, str):
        return False
    # Look for email patterns in the string
    emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
    return any(is_valid_email(email) for email in emails)

def process_csv_chunk(reader, writer, fieldnames) -> int:
    """Process a chunk of CSV rows and write those with emails."""
    count = 0
    for row in reader:
        try:
            # Check each field in the row for email addresses
            for value in row:
                if isinstance(value, str) and contains_email(value):
                    # Ensure we only write the fields that match our fieldnames
                    filtered_row = {k: row.get(k, '') for k in fieldnames}
                    writer.writerow(filtered_row)
                    count += 1
                    break  # Move to next row if email found in any field
        except Exception as e:
            print(f"Warning: Error processing row: {e}")
            continue
    return count

def filter_csv(input_file: str, output_file: str) -> int:
    """
    Filter CSV file to only include rows with email addresses.
    Returns the number of rows with emails found.
    """
    rows_with_emails = 0
    
    try:
        with open(input_file, 'r', encoding='utf-8', newline='', errors='replace') as infile:
            # Read a sample to determine the dialect
            sample = infile.read(1024 * 1024)  # Read 1MB for better dialect detection
            infile.seek(0)
            
            try:
                dialect = csv.Sniffer().sniff(sample)
            except:
                dialect = 'excel'  # Fallback to Excel dialect
                
            # Read the first few rows to determine the structure
            reader = csv.reader(infile, dialect=dialect)
            try:
                fieldnames = next(reader)  # Read header row
            except StopIteration:
                print("Error: Empty CSV file")
                return 0
                
            # Open output file and write header
            with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                
                # Process the first row (after header)
                first_row = next(reader, None)
                if first_row:
                    row_dict = dict(zip(fieldnames, first_row))
                    if any(isinstance(v, str) and contains_email(v) for v in first_row):
                        writer.writerow(row_dict)
                        rows_with_emails += 1
                
                # Process the rest of the file
                for row in reader:
                    try:
                        row_dict = dict(zip(fieldnames, row))
                        if any(isinstance(v, str) and contains_email(v) for v in row):
                            writer.writerow(row_dict)
                            rows_with_emails += 1
                    except Exception as e:
                        print(f"Warning: Error processing row: {e}")
                        continue
            
            if rows_with_emails > 0:
                print(f"Found {rows_with_emails} rows with email addresses. Saved to {output_file}")
            else:
                print("No email addresses found in the input file.")
                
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")
        return 0
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return 0
    
    return rows_with_emails

def main():
    if len(sys.argv) != 3:
        print("Usage: python filter_emails.py input.csv output.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    print(f"Processing {input_file}...")
    rows_with_emails = filter_csv(input_file, output_file)
    
    if rows_with_emails == 0:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()
