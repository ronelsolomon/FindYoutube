import pandas as pd

# Read the CSV file
df = pd.read_csv('channels_with_emails.csv')

# Check if 'other_links' column exists and remove it
if 'other_links' in df.columns:
    df = df.drop(columns=['other_links'])

# Save to a new file
df.to_csv('channels_with_emails_cleaned.csv', index=False)
print("File saved as 'channels_with_emails_cleaned.csv' with 'other_links' column removed.")
