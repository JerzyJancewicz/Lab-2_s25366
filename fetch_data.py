import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

# Konfiguracja połączenia z API Google Sheets
credentials_dict = json.loads(os.getenv("GOOGLE_SHEETS_CREDENTIALS"))
creds = ServiceAccountCredentials.from_json_keyfile_dict(
    credentials_dict,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
client = gspread.authorize(creds)

# Odczyt danych z arkusza
sheet_id = "1qLz321mYARmPy-PmN1dVsYGWY47WLq9MzjAavGhJ7q8"
sheet = client.open_by_key(sheet_id).sheet1
data = sheet.get_all_records()
print(data)
