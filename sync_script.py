import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import psycopg2

# ===============================
# GOOGLE SHEETS CONNECTION
# ===============================

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    "credentials.json", scope
)

client = gspread.authorize(creds)

sheet = client.open_by_url(
    "https://docs.google.com/spreadsheets/d/1L7WF5vrYjwOjPByWc0G59-xAzNc7Q-pfhbj91ytgBqA/edit#gid=0"
).worksheet("Clean_data")

data = sheet.get_all_records()
df = pd.DataFrame(data)

print("✅ Google Sheet Connected Successfully!")

# ===============================
# DATA CLEANING
# ===============================

# Clean percentage columns
percentage_columns = [
    'meallog_1d_pct',
    'meal_log_7d_pct',
    'gfy_7d_pct'
]

for col in percentage_columns:
    df[col] = df[col].astype(str).str.replace('%', '', regex=False)
    df[col] = df[col].str.strip()
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Convert numeric columns
numeric_columns = [
    'days',
    'days_no_chat',
    'start_hba1c',
    'last_hba1c',
    'weight_change'
]

for col in numeric_columns:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Convert date columns
date_columns = [
    'last_chat_sent_date',
    'last_meal_log_date'
]

for col in date_columns:
    df[col] = pd.to_datetime(df[col], errors='coerce')

print("✅ Data cleaned successfully!")

# ===============================
# POSTGRESQL CONNECTION
# ===============================

conn = psycopg2.connect(
    host="localhost",
    database="healthcare_db",
    user="postgres",
    password="Postgmk"
)

cur = conn.cursor()

print("✅ Connected to PostgreSQL!")

# ===============================
# SAFE INSERT (FINAL FIX)
# ===============================

for index, row in df.iterrows():

    # Convert pandas NaN / NaT to Python None explicitly
    values = [
        None if pd.isna(row['member_id']) else row['member_id'],
        None if pd.isna(row['gender']) else row['gender'],
        None if pd.isna(row['days']) else row['days'],
        None if pd.isna(row['coach']) else row['coach'],
        None if pd.isna(row['meallog_1d_pct']) else row['meallog_1d_pct'],
        None if pd.isna(row['days_no_chat']) else row['days_no_chat'],
        None if pd.isna(row['last_chat_sent_date']) else row['last_chat_sent_date'].to_pydatetime(),
        None if pd.isna(row['meal_log_7d_pct']) else row['meal_log_7d_pct'],
        None if pd.isna(row['gfy_7d_pct']) else row['gfy_7d_pct'],
        None if pd.isna(row['last_meal_log_date']) else row['last_meal_log_date'].to_pydatetime(),
        None if pd.isna(row['start_hba1c']) else row['start_hba1c'],
        None if pd.isna(row['last_hba1c']) else row['last_hba1c'],
        None if pd.isna(row['weight_change']) else row['weight_change'],
        None if pd.isna(row['weight_status']) else row['weight_status'],
        None if pd.isna(row['hba1c_status']) else row['hba1c_status'],
        None if pd.isna(row['meal_logging_level']) else row['meal_logging_level'],
        None if pd.isna(row['gfy_level']) else row['gfy_level']
    ]

    cur.execute("""
        INSERT INTO healthcare_data (
            member_id, gender, days, coach, meallog_1d_pct,
            days_no_chat, last_chat_sent_date,
            meal_log_7d_pct, gfy_7d_pct,
            last_meal_log_date,
            start_hba1c, last_hba1c,
            weight_change, weight_status,
            hba1c_status, meal_logging_level, gfy_level
        )
        VALUES (%s, %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                %s,
                %s, %s,
                %s, %s,
                %s, %s, %s)
        ON CONFLICT (member_id) DO NOTHING
    """, values)

conn.commit()

print("✅ Data inserted successfully!")

cur.close()
conn.close()

print("🚀 ETL Process Completed Successfully!")
