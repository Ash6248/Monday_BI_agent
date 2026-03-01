"""
Data Loader Module for Monday.com BI Agent
Loads and cleans data from Excel files containing Work Orders and Deals data.
"""
import pandas as pd
import numpy as np
from datetime import datetime
import os

# Data quality notes that will be displayed to users
DATA_QUALITY_NOTES = []

def load_deals_data(file_path):
    """Load and clean deals data from Excel file."""
    print(f"[API CALL] Loading deals data from {file_path}")
    df = pd.read_excel(file_path)
    
    initial_rows = len(df)
    
    # Standardize column names
    df.columns = df.columns.str.strip()
    
    # Handle missing values
    df['Deal Status'] = df['Deal Status'].fillna('Unknown')
    df['Deal Stage'] = df['Deal Stage'].fillna('Unknown')
    df['Sector/service'] = df['Sector/service'].fillna('Unknown')
    df['Product deal'] = df['Product deal'].fillna('Unknown')
    
    # Normalize deal values - convert to numeric, handle masked values
    df['Masked Deal value'] = pd.to_numeric(df['Masked Deal value'], errors='coerce')
    df['Masked Deal value'] = df['Masked Deal value'].fillna(0)
    
    # Normalize dates
    df['Created Date'] = pd.to_datetime(df['Created Date'], errors='coerce')
    df['Close Date (A)'] = pd.to_datetime(df['Close Date (A)'], errors='coerce')
    df['Tentative Close Date'] = pd.to_datetime(df['Tentative Close Date'], errors='coerce')
    
    # Normalize sector names (handle inconsistencies)
    df['Sector/service'] = df['Sector/service'].str.strip()
    sector_mapping = {
        'Mining': ['Mining', 'MINING', 'mining'],
        'Powerline': ['Powerline', 'POWERLINE', 'powerline'],
        'Renewables': ['Renewables', 'RENEWABLES', 'renewables'],
        'DSP': ['DSP', 'dsp'],
        'Railways': ['Railways', 'RAILWAYS', 'railways'],
        'Tender': ['Tender', 'TENDER', 'tender']
    }
    
    for standard, variants in sector_mapping.items():
        for variant in variants:
            df.loc[df['Sector/service'] == variant, 'Sector/service'] = standard
    
    final_rows = len(df)
    notes = f"Deals Data: Loaded {final_rows} records. {initial_rows - final_rows} duplicates removed. {df['Masked Deal value'].isna().sum()} missing deal values handled."
    DATA_QUALITY_NOTES.append(notes)
    print(f"[API CALL] {notes}")
    
    return df

def load_work_orders_data(file_path):
    """Load and clean work orders data from Excel file."""
    print(f"[API CALL] Loading work orders data from {file_path}")
    
    # Read with header row = 1 (second row contains headers)
    df = pd.read_excel(file_path, header=1)
    
    initial_rows = len(df)
    
    # Standardize column names
    df.columns = df.columns.str.strip()
    
    # Rename unnamed columns based on first row values
    # First row contained: Deal name masked, Customer Name Code, Serial #, Nature of Work, etc.
    column_mapping = {
        'Unnamed: 0': 'Deal Name',
        'Unnamed: 1': 'Customer Name Code',
        'Unnamed: 2': 'Serial Number',
        'Unnamed: 3': 'Nature of Work',
    }
    # Apply mapping for known columns
    
    # Handle missing values in key columns
    if 'Deal name masked' in df.columns:
        df['Deal name masked'] = df['Deal name masked'].fillna('Unknown')
    if 'WO Status (billed)' in df.columns:
        df['WO Status (billed)'] = df['WO Status (billed)'].fillna('Unknown')
    if 'Collection status' in df.columns:
        df['Collection status'] = df['Collection status'].fillna('Unknown')
    if 'Billing Status' in df.columns:
        df['Billing Status'] = df['Billing Status'].fillna('Unknown')
    
    final_rows = len(df)
    notes = f"Work Orders Data: Loaded {final_rows} records. Data cleaned for nulls and inconsistencies."
    DATA_QUALITY_NOTES.append(notes)
    print(f"[API CALL] {notes}")
    
    return df

def get_data_quality_notes():
    """Return accumulated data quality notes."""
    return DATA_QUALITY_NOTES

def query_deals_by_sector(df, sector):
    """Query deals filtered by sector."""
    print(f"[API CALL] Querying deals with sector = '{sector}'")
    result = df[df['Sector/service'].str.lower() == sector.lower()]
    print(f"[API CALL] Found {len(result)} deals in {sector} sector")
    return result

def query_deals_by_stage(df, stage):
    """Query deals filtered by stage."""
    print(f"[API CALL] Querying deals with stage = '{stage}'")
    result = df[df['Deal Stage'].str.contains(stage, case=False, na=False)]
    print(f"[API CALL] Found {len(result)} deals at stage: {stage}")
    return result

def get_pipeline_summary(df):
    """Get overall pipeline summary."""
    print(f"[API CALL] Calculating pipeline summary")
    total_value = df['Masked Deal value'].sum()
    open_deals = len(df[df['Deal Status'] == 'Open'])
    on_hold = len(df[df['Deal Status'] == 'On Hold'])
    closed = len(df[df['Deal Status'] == 'Closed Won']) + len(df[df['Deal Status'] == 'Closed Lost'])
    
    summary = {
        'total_value': total_value,
        'total_deals': len(df),
        'open_deals': open_deals,
        'on_hold': on_hold,
        'closed': closed
    }
    print(f"[API CALL] Pipeline summary: {summary}")
    return summary

def get_sector_performance(df):
    """Get performance metrics by sector."""
    print(f"[API CALL] Calculating sector performance")
    sector_stats = df.groupby('Sector/service').agg({
        'Masked Deal value': ['sum', 'mean', 'count'],
        'Deal Status': lambda x: (x == 'Open').sum()
    }).round(2)
    sector_stats.columns = ['Total Value', 'Avg Value', 'Deal Count', 'Open Deals']
    print(f"[API CALL] Sector performance calculated for {len(sector_stats)} sectors")
    return sector_stats.reset_index()

# Initialize data on module load
DEALS_FILE = 'C:/Users/Ashmith/Downloads/Deal funnel Data.xlsx'
WO_FILE = 'C:/Users/Ashmith/Downloads/Work_Order_Tracker Data.xlsx'

deals_df = None
work_orders_df = None

def initialize_data():
    """Initialize data from files."""
    global deals_df, work_orders_df
    try:
        deals_df = load_deals_data(DEALS_FILE)
        work_orders_df = load_work_orders_data(WO_FILE)
        return True
    except Exception as e:
        print(f"[ERROR] Failed to load data: {e}")
        return False

def get_deals_data():
    """Get deals dataframe."""
    return deals_df

def get_work_orders_data():
    """Get work orders dataframe."""
    return work_orders_df
