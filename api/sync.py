import os
import json
import gspread
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from google.oauth2.service_account import Credentials


def handler(request):
    try:
        # ===============================
        # GOOGLE SHEETS CONNECTION
        # ===============================

        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        # Load credentials from ENV variable (SAFER)
        credentials_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

        creds = Credentials.from_service_account_info(
            credentials_dict,
            scopes=scope
        )

        client = gspread.authorize(creds)

        sheet = client.open_by_url(
            "https://docs.google.com/spreadsheets/d/1L7WF5vrYjwOjPByWc0G59-xAzNc7Q-pfhbj91ytgBqA/edit"
        ).worksheet("Clean_data")

        data = sheet.get_all_records()
        df = pd.DataFrame(data)

        # ===============================
        # DATA CLEANING
        # ===============================

        percentage_columns = [
            'meallog_1d_pct',
            'meal_log_7d_pct',
            'gfy_7d_pct'
        ]

        for col in percentage_columns:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace('%', '', regex=False)
                    .str.strip()
                )
                df[col] = pd.to_numeric(df[col], errors='coerce')

        numeric_columns = [
            'days',
            'days_no_chat',
            'start_hba1c',
            'last_hba1c',
            'weight_change'
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        date_columns = [
            'last_chat_sent_date',
            'last_meal_log_date'
        ]

        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # ===============================
        # POSTGRESQL CONNECTION (CLOUD DB)
        # ===============================

        conn = psycopg2.connect(os.environ["DATABASE_URL"])
        cur = conn.cursor()

        # ===============================
        # PREPARE DATA FOR BATCH INSERT
        # ===============================

        insert_query = """
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
        """

        values_list = []

        for _, row in df.iterrows():
            values = [
                None if pd.isna(row.get('member_id')) else row.get('member_id'),
                None if pd.isna(row.get('gender')) else row.get('gender'),
                None if pd.isna(row.get('days')) else row.get('days'),
                None if pd.isna(row.get('coach')) else row.get('coach'),
                None if pd.isna(row.get('meallog_1d_pct')) else row.get('meallog_1d_pct'),
                None if pd.isna(row.get('days_no_chat')) else row.get('days_no_chat'),
                None if pd.isna(row.get('last_chat_sent_date')) else row.get('last_chat_sent_date').to_pydatetime(),
                None if pd.isna(row.get('meal_log_7d_pct')) else row.get('meal_log_7d_pct'),
                None if pd.isna(row.get('gfy_7d_pct')) else row.get('gfy_7d_pct'),
                None if pd.isna(row.get('last_meal_log_date')) else row.get('last_meal_log_date').to_pydatetime(),
                None if pd.isna(row.get('start_hba1c')) else row.get('start_hba1c'),
                None if pd.isna(row.get('last_hba1c')) else row.get('last_hba1c'),
                None if pd.isna(row.get('weight_change')) else row.get('weight_change'),
                None if pd.isna(row.get('weight_status')) else row.get('weight_status'),
                None if pd.isna(row.get('hba1c_status')) else row.get('hba1c_status'),
                None if pd.isna(row.get('meal_logging_level')) else row.get('meal_logging_level'),
                None if pd.isna(row.get('gfy_level')) else row.get('gfy_level')
            ]

            values_list.append(values)

        execute_batch(cur, insert_query, values_list)
        conn.commit()

        cur.close()
        conn.close()

        return {
            "statusCode": 200,
            "body": "🚀 ETL Process Completed Successfully!"
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }
