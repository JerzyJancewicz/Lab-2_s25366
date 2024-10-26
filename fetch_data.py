import json
import os
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np

def fetch_data(sheet_id):
    credentials_dict = json.loads(os.getenv("GOOGLE_SHEETS_CREDENTIALS"))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        credentials_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id).sheet1
    data = sheet.get_all_records()
    
    return pd.DataFrame(data)

def clean_data(df):
    original_size = df.shape[0]  # Original number of rows
    changed_cells = 0  # Counter for changed cells

    df_cleaned = df.dropna(thresh=len(df.columns) - 2)
    removed_rows = original_size - df_cleaned.shape[0]  # Count removed rows

    for column in df_cleaned.select_dtypes(include=[np.number]).columns:
        mean_value = df_cleaned[column].mean()
        num_changed = df_cleaned[column].isnull().sum()  # Count changed cells
        df_cleaned[column] = df_cleaned[column].fillna(mean_value)  # Use assignment instead of inplace
        changed_cells += num_changed  # Update changed cells count

    for column in df_cleaned.select_dtypes(exclude=[np.number]).columns:
        num_changed = df_cleaned[column].isnull().sum()  # Count changed cells
        df_cleaned[column] = df_cleaned[column].fillna('Brak danych')  # Use assignment instead of inplace
        changed_cells += num_changed  # Update changed cells count

    for column in df_cleaned.select_dtypes(include=[object]).columns:
        df_cleaned[column] = pd.to_numeric(df_cleaned[column], errors='ignore')

    numeric_cols = df_cleaned.select_dtypes(include=[np.number]).columns
    df_standardized = (df_cleaned[numeric_cols] - df_cleaned[numeric_cols].mean()) / df_cleaned[numeric_cols].std()

    # Calculate percentages
    changed_percentage = (changed_cells / df.size) * 100 if df.size > 0 else 0
    removed_percentage = (removed_rows / original_size) * 100 if original_size > 0 else 0

    return df_standardized, changed_percentage, removed_percentage

def generate_report(changed_percentage, removed_percentage):
    report_content = f"""# Data Cleaning Report

## Summary
- **Percentage of changed data**: {changed_percentage:.2f}%
- **Percentage of removed data**: {removed_percentage:.2f}%
    """
    
    with open('report.md', 'w') as f:
        f.write(report_content)

if __name__ == "__main__":
    sheet_id = '1qLz321mYARmPy-PmN1dVsYGWY47WLq9MzjAavGhJ7q8'
    df = fetch_data(sheet_id)
    df_cleaned, changed_percentage, removed_percentage = clean_data(df)

    print(df_cleaned)
    df_cleaned.to_csv('cleaned_data.csv', index=False)

    # Generate and save the report
    generate_report(changed_percentage, removed_percentage)
