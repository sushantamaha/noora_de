import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from analysis import plot_outbound_message_statuses

if __name__ == "__main__":
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path=dotenv_path)
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")
    engine = create_engine(f'postgresql://{user}:{password}@localhost:5433/{db}')

    df = pd.read_sql_table('int_messages_with_status_history', engine, schema='public')
    df['message_created_at'] = pd.to_datetime(df['message_created_at'])
    df['sent_at'] = pd.to_datetime(df['sent_at'])
    df['read_at'] = pd.to_datetime(df['read_at'])

    plot_outbound_message_statuses(df)
