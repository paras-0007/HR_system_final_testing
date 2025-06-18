import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file


SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# --- Database Credentials ---
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Google API Scopes
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/calendar'
]

# App Settings
CHECK_INTERVAL = 120  
TEMP_DIR = "temp"
OPENAI_MODEL = "gpt-3.5-turbo-1106"
SHEET_COLUMNS = [
    "Name", "Email", "Phone", "Education",
    "Domain", "Job History", "CV_URL", "Status"
]