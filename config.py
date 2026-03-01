"""
Configuration Module for Monday.com BI Agent
Environment-based configuration management.
"""
import os
from datetime import timedelta
from pathlib import Path

# Get the base directory
BASE_DIR = Path(__file__).parent.absolute()


class Config:
    """Base configuration class."""
    
    # Flask Settings
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
    DEBUG = False
    TESTING = False
    
    # Data File Paths
    DEALS_FILE = os.environ.get('DEALS_FILE', 'C:/Users/Ashmith/Downloads/Deal funnel Data.xlsx')
    WORK_ORDERS_FILE = os.environ.get('WORK_ORDERS_FILE', 'C:/Users/Ashmith/Downloads/Work_Order_Tracker Data.xlsx')
    
    # Data Refresh Settings
    AUTO_REFRESH_INTERVAL = int(os.environ.get('AUTO_REFRESH_INTERVAL', 300))  # seconds
    ENABLE_FILE_WATCHER = os.environ.get('ENABLE_FILE_WATCHER', 'true').lower() == 'true'
    
    # Cache Settings
    ENABLE_CACHE = os.environ.get('ENABLE_CACHE', 'true').lower() == 'true'
    CACHE_TTL = int(os.environ.get('CACHE_TTL', 60))  # seconds
    
    # Rate Limiting
    RATELIMIT_ENABLED = os.environ.get('RATELIMIT_ENABLED', 'true').lower() == 'true'
    RATELIMIT_DEFAULT = "100 per minute"
    
    # CORS Settings
    CORS_ORIGINS = os.environ.get('CORS_ORIGINS', '*').split(',')
    
    # Monday.com API Settings (for live data)
    MONDAY_API_KEY = os.environ.get('MONDAY_API_KEY', '')
    MONDAY_BOARD_ID_DEALS = os.environ.get('MONDAY_BOARD_ID_DEALS', '')
    MONDAY_BOARD_ID_WORK_ORDERS = os.environ.get('MONDAY_BOARD_ID_WORK_ORDERS', '')
    USE_LIVE_API = os.environ.get('USE_LIVE_API', 'false').lower() == 'true'
    
    # Logging Settings
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FILE = os.environ.get('LOG_FILE', str(BASE_DIR / 'logs' / 'app.log'))
    
    # Session Settings
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    PERMANENT_SESSION_LIFETIME = timedelta(hours=24)


class DevelopmentConfig(Config):
    """Development configuration."""
    DEBUG = True
    LOG_LEVEL = 'DEBUG'
    RATELIMIT_ENABLED = False


class ProductionConfig(Config):
    """Production configuration."""
    DEBUG = False
    LOG_LEVEL = 'WARNING'
    RATELIMIT_ENABLED = True
    RATELIMIT_DEFAULT = "60 per minute"


class TestingConfig(Config):
    """Testing configuration."""
    TESTING = True
    DEBUG = True


# Configuration mapping
config_by_name = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}


def get_config(env=None):
    """Get configuration based on environment."""
    if env is None:
        env = os.environ.get('FLASK_ENV', 'development')
    return config_by_name.get(env, DevelopmentConfig)


# Data quality notes storage (for display)
DATA_QUALITY_NOTES = []
