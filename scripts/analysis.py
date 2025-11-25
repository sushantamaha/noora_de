import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

def plot_user_trends(df: pd.DataFrame):
    """
    Generates and displays a line chart of total and active users over time.

    Args:
        df (pd.DataFrame): DataFrame containing message data.
    """
    df['week'] = df['message_created_at'].dt.to_period('W').apply(lambda r: r.start_time).dt.date

    # Total users
    total_users = df.groupby('week').agg({'masked_sender': 'nunique', 'masked_addressees': 'nunique'}).reset_index()
    total_users['total_users'] = total_users['masked_sender'] + total_users['masked_addressees']

    # Active users (inbound)
    active_users = df[df['direction'] == 'inbound'].groupby('week').agg({'masked_sender': 'nunique'}).reset_index()
    active_users.rename(columns={'masked_sender': 'active_users'}, inplace=True)

    user_trends = pd.merge(total_users, active_users, on='week', how='left').fillna(0)

    fig = px.line(user_trends, x='week', y=['total_users', 'active_users'], title='Total and Active Users Over Time')
    fig.show()

def get_read_fraction_text(df: pd.DataFrame) -> str:
    """
    Calculates the fraction of non-failed outbound messages that were read.

    Args:
        df (pd.DataFrame): DataFrame containing message data.

    Returns:
        str: A formatted string with the read fraction.
    """
    outbound_non_failed = df[(df['direction'] == 'outbound') & (df['failed_at'].isna())]
    read_fraction = outbound_non_failed['read_at'].notna().mean()
    return f"Fraction of non-failed outbound messages that were read: {read_fraction:.2%}"

def plot_time_to_read_distribution(df: pd.DataFrame):
    """
    Generates and displays a histogram of the time to read non-failed outbound messages.

    Args:
        df (pd.DataFrame): DataFrame containing message data, including 'direction', 
                           'failed_at', 'read_at', and 'sent_at' columns.
    """
    outbound_non_failed = df[(df['direction'] == 'outbound') & (df['failed_at'].isna())]
    read_times = outbound_non_failed[outbound_non_failed['read_at'].notna()].copy()
    
    if not read_times.empty:
        read_times['time_to_read_seconds'] = (read_times['read_at'] - read_times['sent_at']).dt.total_seconds()
        fig = px.histogram(read_times, x='time_to_read_seconds', title='Distribution of Time to Read Outbound Messages (Seconds)')
        fig.show()
    else:
        print("No messages with read times available to plot.")

def plot_outbound_message_statuses(df: pd.DataFrame):
    """
    Generates and displays a bar chart of outbound message statuses in the last week.

    Args:
        df (pd.DataFrame): DataFrame containing message data.
    """
    last_week = df['message_created_at'].max() - pd.Timedelta(days=7)
    last_week_outbound = df[(df['direction'] == 'outbound') & (df['message_created_at'] >= last_week)]

    status_counts = pd.DataFrame({
        'sent': last_week_outbound['sent_at'].notna().sum(),
        'delivered': last_week_outbound['delivered_at'].notna().sum(),
        'read': last_week_outbound['read_at'].notna().sum(),
        'failed': last_week_outbound['failed_at'].notna().sum()
    }, index=['count']).T.reset_index()

    fig = px.bar(status_counts, x='index', y='count', title='Outbound Message Statuses in the Last Week')
    fig.show()
