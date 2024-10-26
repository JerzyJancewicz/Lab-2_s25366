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
    df_cleaned = df.dropna(thresh=len(df.columns) - 2)

    numeric_cols = df_cleaned.select_dtypes(include=[np.number]).columns
    for column in numeric_cols:
        df_cleaned[column] = df_cleaned[column].fillna(df_cleaned[column].mean())

    categorical_cols = df_cleaned.select_dtypes(exclude=[np.number]).columns
    for column in categorical_cols:
        df_cleaned[column] = df_cleaned[column].fillna('Brak danych')

    for column in numeric_cols:
        df_cleaned[column] = pd.to_numeric(df_cleaned[column], errors='coerce')

    df_cleaned.dropna(subset=numeric_cols, inplace=True)

    df_standardized = (df_cleaned[numeric_cols] - df_cleaned[numeric_cols].mean()) / df_cleaned[numeric_cols].std()

    df_standardized = pd.concat([df_standardized, df_cleaned[categorical_cols].reset_index(drop=True)], axis=1)

    return df_standardized

if __name__ == "__main__":
    sheet_id = '1qLz321mYARmPy-PmN1dVsYGWY47WLq9MzjAavGhJ7q8'
    df = fetch_data(sheet_id)
    df_cleaned = clean_data(df)

    print(df_cleaned)
    df_cleaned.to_csv('cleaned_data.csv', index=False)
