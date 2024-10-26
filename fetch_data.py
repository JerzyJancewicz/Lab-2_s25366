import json
import os
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np

# Funkcja do pobierania danych z Google Sheets
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
    # Usuwanie wierszy z nadmiernymi brakującymi wartościami
    df_cleaned = df.dropna(thresh=len(df.columns) - 2)

    # Uzupełnianie brakujących wartości średnią dla kolumn numerycznych
    for column in df_cleaned.select_dtypes(include=[np.number]).columns:
        df_cleaned[column].fillna(df_cleaned[column].mean(), inplace=True)

    # Uzupełnianie braków dla kolumn kategorycznych z wartością domyślną
    for column in df_cleaned.select_dtypes(exclude=[np.number]).columns:
        df_cleaned[column].fillna('Brak danych', inplace=True)

    df_standardized = (df_cleaned - df_cleaned.mean()) / df_cleaned.std()

    return df_standardized

if __name__ == "__main__":

    sheet_id = '1qLz321mYARmPy-PmN1dVsYGWY47WLq9MzjAavGhJ7q8'
    df = fetch_data(sheet_id)
    df_cleaned = clean_data(df)

    print(df_cleaned)
    df_cleaned.to_csv('cleaned_data.csv', index=False)
