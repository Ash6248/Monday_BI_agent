"""
Data Loader Module for Monday.com BI Agent
Loads and cleans data from Excel files OR Monday.com API, with caching support.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import logging
import time
import hashlib
import threading
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

# Data quality notes that will be displayed to users
DATA_QUALITY_NOTES = []

# Cache storage
class DataCache:
    """Simple in-memory cache with TTL support."""
    
    def __init__(self, ttl=60):
        self._cache = {}
        self._timestamps = {}
        self._ttl = ttl
        self._lock = threading.Lock()
    
    def get(self, key):
        """Get value from cache if not expired."""
        with self._lock:
            if key in self._cache:
                age = time.time() - self._timestamps.get(key, 0)
                if age < self._ttl:
                    logger.debug(f"Cache hit for key: {key}")
                    return self._cache[key]
                else:
                    logger.debug(f"Cache expired for key: {key}")
                    del self._cache[key]
                    del self._timestamps[key]
            return None
    
    def set(self, key, value):
        """Set value in cache."""
        with self._lock:
            self._cache[key] = value
            self._timestamps[key] = time.time()
            logger.debug(f"Cache set for key: {key}")
    
    def clear(self):
        """Clear all cache."""
        with self._lock:
            self._cache.clear()
            self._timestamps.clear()
            logger.info("Cache cleared")
    
    def get_stats(self):
        """Get cache statistics."""
        with self._lock:
            return {
                'entries': len(self._cache),
                'ttl': self._ttl
            }

# Global cache instance
_data_cache = None

def get_cache(ttl=60):
    """Get or create the global cache instance."""
    global _data_cache
    if _data_cache is None:
        _data_cache = DataCache(ttl=ttl)
    return _data_cache


# File watcher for auto-reload
class FileWatcher:
    """Watch files for changes and trigger callbacks."""
    
    def __init__(self, files, callback, interval=5):
        self.files = files
        self.callback = callback
        self.interval = interval
        self._thread = None
        self._running = False
        self._last_modified = {}
    
    def start(self):
        """Start watching files."""
        if self._running:
            return
        
        # Initialize last modified times
        for f in self.files:
            if os.path.exists(f):
                self._last_modified[f] = os.path.getmtime(f)
        
        self._running = True
        self._thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._thread.start()
        logger.info(f"File watcher started for: {self.files}")
    
    def stop(self):
        """Stop watching files."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
        logger.info("File watcher stopped")
    
    def _watch_loop(self):
        """Watch loop that checks for file changes."""
        while self._running:
            try:
                for f in self.files:
                    if os.path.exists(f):
                        current_mtime = os.path.getmtime(f)
                        if f in self._last_modified:
                            if current_mtime > self._last_modified[f]:
                                logger.info(f"File changed: {f}")
                                self._last_modified[f] = current_mtime
                                self.callback()
                        else:
                            self._last_modified[f] = current_mtime
            except Exception as e:
                logger.error(f"Error in file watcher: {e}")
            
            time.sleep(self.interval)


# Global file watcher instance
_file_watcher = None


# Monday.com API integration
class MondayAPI:
    """Monday.com API client for live data."""
    
    def __init__(self, api_key, board_id):
        self.api_key = api_key
        self.board_id = board_id
        self.base_url = "https://api.monday.com/v2"
    
    def _make_request(self, query, variables=None):
        """Make API request to Monday.com."""
        import requests
        
        headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
            "API-Version": "2024-01"
        }
        
        body = {"query": query}
        if variables:
            body["variables"] = variables
        
        try:
            response = requests.post(self.base_url, json=body, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Monday.com API error: {e}")
            raise
    
    def fetch_items(self, board_id=None):
        """Fetch all items from a board."""
        board = board_id or self.board_id
        query = f"""
        {{
            boards(ids: [{board}]) {{
                items_page {{ items {{ id name }} }}
            }}
        }}
        """
        result = self._make_request(query)
        return result
    
    def fetch_columns(self, board_id=None):
        """Fetch column metadata from a board."""
        board = board_id or self.board_id
        query = f"""
        {{
            boards(ids: [{board}]) {{
                columns {{ id title type }}
            }}
        }}
        """
        result = self._make_request(query)
        return result


# Global API clients
_monday_deals_api = None
_monday_work_orders_api = None


def init_monday_api(api_key, deals_board_id, work_orders_board_id):
    """Initialize Monday.com API clients."""
    global _monday_deals_api, _monday_work_orders_api
    
    if api_key and deals_board_id:
        _monday_deals_api = MondayAPI(api_key, deals_board_id)
        logger.info("Monday.com Deals API initialized")
    
    if api_key and work_orders_board_id:
        _monday_work_orders_api = MondayAPI(api_key, work_orders_board_id)
        logger.info("Monday.com Work Orders API initialized")


def load_deals_from_api():
    """Load deals data from Monday.com API."""
    if not _monday_deals_api:
        raise RuntimeError("Monday.com Deals API not initialized")
    
    logger.info("Fetching deals from Monday.com API...")
    data = _monday_deals_api.fetch_items()
    
    # Transform API response to DataFrame
    items = data.get('data', {}).get('boards', [{}])[0].get('items_page', {}).get('items', [])
    
    records = []
    for item in items:
        records.append({
            'Deal Name': item.get('name', 'Unknown'),
            'Deal Status': 'Open',
            'Deal Stage': 'Unknown',
            'Sector/service': 'Unknown',
            'Product deal': 'Unknown',
            'Masked Deal value': 0,
            'Created Date': datetime.now()
        })
    
    df = pd.DataFrame(records)
    logger.info(f"Loaded {len(df)} deals from Monday.com API")
    return df


def load_deals_data(file_path, use_cache=True):
    """Load and clean deals data from Excel file."""
    cache = get_cache()
    cache_key = f"deals_data_{hashlib.md5(file_path.encode()).hexdigest()}"
    
    # Try cache first
    if use_cache:
        cached = cache.get(cache_key)
        if cached is not None:
            logger.debug(f"Using cached deals data")
            return cached
    
    logger.info(f"Loading deals data from {file_path}")
    
    try:
        df = pd.read_excel(file_path)
    except FileNotFoundError:
        logger.error(f"Deals file not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading deals file: {e}")
        raise
    
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
    logger.info(notes)
    
    # Cache the result
    if use_cache:
        cache.set(cache_key, df)
    
    return df

def load_work_orders_data(file_path, use_cache=True):
    """Load and clean work orders data from Excel file."""
    cache = get_cache()
    cache_key = f"work_orders_data_{hashlib.md5(file_path.encode()).hexdigest()}"
    
    # Try cache first
    if use_cache:
        cached = cache.get(cache_key)
        if cached is not None:
            logger.debug(f"Using cached work orders data")
            return cached
    
    logger.info(f"Loading work orders data from {file_path}")
    
    try:
        # Read with header row = 1 (second row contains headers)
        df = pd.read_excel(file_path, header=1)
    except FileNotFoundError:
        logger.error(f"Work orders file not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading work orders file: {e}")
        raise
    
    initial_rows = len(df)
    
    # Standardize column names
    df.columns = df.columns.str.strip()
    
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
    logger.info(notes)
    
    # Cache the result
    if use_cache:
        cache.set(cache_key, df)
    
    return df

def get_data_quality_notes():
    """Return accumulated data quality notes."""
    return DATA_QUALITY_NOTES

def query_deals_by_sector(df, sector):
    """Query deals filtered by sector."""
    logger.debug(f"Querying deals with sector = '{sector}'")
    result = df[df['Sector/service'].str.lower() == sector.lower()]
    logger.debug(f"Found {len(result)} deals in {sector} sector")
    return result

def query_deals_by_stage(df, stage):
    """Query deals filtered by stage."""
    logger.debug(f"Querying deals with stage = '{stage}'")
    result = df[df['Deal Stage'].str.contains(stage, case=False, na=False)]
    logger.debug(f"Found {len(result)} deals at stage: {stage}")
    return result

def get_pipeline_summary(df):
    """Get overall pipeline summary."""
    logger.debug("Calculating pipeline summary")
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
    logger.debug(f"Pipeline summary: {summary}")
    return summary

def get_sector_performance(df):
    """Get performance metrics by sector."""
    logger.debug("Calculating sector performance")
    sector_stats = df.groupby('Sector/service').agg({
        'Masked Deal value': ['sum', 'mean', 'count'],
        'Deal Status': lambda x: (x == 'Open').sum()
    }).round(2)
    sector_stats.columns = ['Total Value', 'Avg Value', 'Deal Count', 'Open Deals']
    logger.debug(f"Sector performance calculated for {len(sector_stats)} sectors")
    return sector_stats.reset_index()


# Global data references
deals_df = None
work_orders_df = None
_last_refresh = None

# Configuration storage for board links
_board_config = {}

# Configuration
DEALS_FILE = None
WO_FILE = None
USE_LIVE_API = False
DEMO_MODE = os.environ.get('DEMO_MODE', 'false').lower() == 'true'


def initialize_data(config=None):
    """Initialize data from files or API based on configuration."""
    global deals_df, work_orders_df, DEALS_FILE, WO_FILE, USE_LIVE_API, _file_watcher, _last_refresh, DEMO_MODE, _board_config
    
    # Load configuration
    if config:
        DEALS_FILE = config.DEALS_FILE
        WO_FILE = config.WORK_ORDERS_FILE
        USE_LIVE_API = config.USE_LIVE_API
        DEMO_MODE = os.environ.get('DEMO_MODE', 'false').lower() == 'true'
        
        # Initialize cache
        cache = get_cache(ttl=config.CACHE_TTL)
        
        # Initialize Monday.com API if enabled
        if config.USE_LIVE_API and config.MONDAY_API_KEY:
            init_monday_api(
                config.MONDAY_API_KEY,
                config.MONDAY_BOARD_ID_DEALS,
                config.MONDAY_BOARD_ID_WORK_ORDERS
            )
        
        # Start file watcher if enabled
        if config.ENABLE_FILE_WATCHER and not DEMO_MODE:
            files_to_watch = [DEALS_FILE, WO_FILE]
            _file_watcher = FileWatcher(
                files_to_watch,
                callback=lambda: refresh_data(),
                interval=config.AUTO_REFRESH_INTERVAL
            )
            _file_watcher.start()
    
    # Check for demo mode
    if DEMO_MODE:
        logger.info("Running in DEMO MODE - using sample data")
        try:
            from demo_data import DEMO_DEALS, DEMO_WORK_ORDERS
            deals_df = DEMO_DEALS
            work_orders_df = DEMO_WORK_ORDERS
            DATA_QUALITY_NOTES.append("Demo Mode: Using sample data for demonstration")
            _last_refresh = datetime.now()
            logger.info("Demo data loaded successfully")
            return True
        except ImportError:
            logger.warning("Demo data module not found, trying file-based data")
            DEMO_MODE = False
    
    try:
        if USE_LIVE_API and _monday_deals_api:
            deals_df = load_deals_from_api()
        else:
            deals_df = load_deals_data(DEALS_FILE)
        
        work_orders_df = load_work_orders_data(WO_FILE)
        
        _last_refresh = datetime.now()
        logger.info("Data initialization completed successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        # If in demo mode, create empty dataframes
        if DEMO_MODE:
            deals_df = pd.DataFrame()
            work_orders_df = pd.DataFrame()
            _last_refresh = datetime.now()
            return True
        return False


def refresh_data():
    """Refresh data from source."""
    global deals_df, work_orders_df, _last_refresh, DATA_QUALITY_NOTES
    
    logger.info("Refreshing data...")
    DATA_QUALITY_NOTES = []  # Reset notes
    
    try:
        # Clear cache
        cache = get_cache()
        cache.clear()
        
        # Reload data
        if USE_LIVE_API and _monday_deals_api:
            deals_df = load_deals_from_api()
        else:
            deals_df = load_deals_data(DEALS_FILE, use_cache=False)
        
        work_orders_df = load_work_orders_data(WO_FILE, use_cache=False)
        
        _last_refresh = datetime.now()
        logger.info("Data refresh completed successfully")
        return True
    except Exception as e:
        logger.error(f"Data refresh failed: {e}")
        return False


def get_last_refresh_time():
    """Get the last data refresh time."""
    return _last_refresh


def get_cache_stats():
    """Get cache statistics."""
    return get_cache().get_stats()


def start_file_watcher(config):
    """Start the file watcher with given configuration."""
    global _file_watcher
    
    if config.ENABLE_FILE_WATCHER:
        files_to_watch = [config.DEALS_FILE, config.WORK_ORDERS_FILE]
        _file_watcher = FileWatcher(
            files_to_watch,
            callback=lambda: refresh_data(),
            interval=config.AUTO_REFRESH_INTERVAL
        )
        _file_watcher.start()


def stop_file_watcher():
    """Stop the file watcher."""
    global _file_watcher
    
    if _file_watcher:
        _file_watcher.stop()


def get_deals_data():
    """Get deals dataframe."""
    return deals_df

def get_work_orders_data():
    """Get work orders dataframe."""
    return work_orders_df


def get_board_links(config):
    """Generate Monday.com board links based on configuration.
    
    Args:
        config: Configuration object with board settings
        
    Returns:
        Dictionary with board links information
    """
    global _board_config
    
    workspace = config.MONDAY_WORKSPACE_SUBDOMAIN
    deals_board_id = config.MONDAY_BOARD_ID_DEALS
    work_orders_board_id = config.MONDAY_BOARD_ID_WORK_ORDERS
    deals_name = config.DEALS_BOARD_NAME
    work_orders_name = config.WORK_ORDERS_BOARD_NAME
    
    board_links = {}
    
    if workspace and deals_board_id:
        board_links['deals'] = {
            'name': deals_name,
            'url': f"https://{workspace}.monday.com/boards/{deals_board_id}"
        }
    
    if workspace and work_orders_board_id:
        board_links['work_orders'] = {
            'name': work_orders_name,
            'url': f"https://{workspace}.monday.com/boards/{work_orders_board_id}"
        }
    
    # Store for later use
    _board_config = board_links
    
    return board_links


def get_stored_board_links():
    """Get the stored board links configuration."""
    return _board_config
