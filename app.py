"""
Monday.com Business Intelligence Agent - Flask Web Application
A production-ready conversational interface for business intelligence queries.
"""
import os
import sys
import logging
from datetime import datetime
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from werkzeug.middleware.proxy_fix import ProxyFix

# Import project modules
from config import get_config
from data_loader import (
    initialize_data, 
    get_data_quality_notes, 
    refresh_data,
    get_last_refresh_time,
    get_cache_stats,
    stop_file_watcher
)
from query_processor import QueryProcessor

# Create Flask app
app = Flask(__name__)

# Load configuration
env = os.environ.get('FLASK_ENV', 'production')
config = get_config(env)
app.config.from_object(config)

# Setup logging
def setup_logging():
    """Configure application logging."""
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(config.LOG_FILE)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    
    # Configure logging format
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(config.LOG_FILE) if config.LOG_FILE else logging.NullHandler()
        ]
    )
    
    return logging.getLogger(__name__)

logger = setup_logging()

# Setup CORS
CORS(app, origins=config.CORS_ORIGINS)

# Setup rate limiting
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=[config.RATELIMIT_DEFAULT] if config.RATELIMIT_ENABLED else [],
    storage_uri="memory://"
)

# Setup proxy fix for production
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

# Initialize data and query processor
logger.info("Initializing Monday.com BI Agent...")
data_initialized = initialize_data(config)
logger.info(f"Data initialization: {'Success' if data_initialized else 'Failed'}")

processor = QueryProcessor()


@app.route('/')
def index():
    """Render the main chat interface."""
    return render_template('index.html')


@app.route('/api/query', methods=['POST'])
@limiter.limit(config.RATELIMIT_DEFAULT if config.RATELIMIT_ENABLED else "1000 per minute")
def handle_query():
    """Handle user queries via API."""
    try:
        # Validate request
        if not request.is_json:
            return jsonify({'error': 'Content-Type must be application/json'}), 400
        
        data = request.get_json()
        query = data.get('query', '').strip()
        
        if not query:
            return jsonify({'error': 'Please enter a query'}), 400
        
        if len(query) > 500:
            return jsonify({'error': 'Query too long (max 500 characters)'}), 400
        
        # Log the query
        logger.info(f"Query received: {query}")
        
        # Process the query
        result = processor.process_query(query)
        
        # Add data quality notes
        result['data_notes'] = get_data_quality_notes()
        
        # Add metadata
        result['metadata'] = {
            'timestamp': datetime.now().isoformat(),
            'data_refresh_time': get_last_refresh_time().isoformat() if get_last_refresh_time() else None
        }
        
        logger.info(f"Query processed successfully")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error processing query: {e}", exc_info=True)
        return jsonify({
            'error': 'An error occurred while processing your query. Please try again.',
            'actions': []
        }), 500


@app.route('/api/help', methods=['GET'])
def help():
    """Return help information."""
    try:
        return jsonify(processor.get_help())
    except Exception as e:
        logger.error(f"Error getting help: {e}")
        return jsonify({'error': 'Unable to retrieve help information'}), 500


@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint with detailed status."""
    try:
        last_refresh = get_last_refresh_time()
        cache_stats = get_cache_stats()
        
        return jsonify({
            'status': 'healthy' if data_initialized else 'degraded',
            'data_loaded': data_initialized,
            'last_refresh': last_refresh.isoformat() if last_refresh else None,
            'cache': cache_stats,
            'environment': env,
            'version': '1.0.0-production'
        })
    except Exception as e:
        logger.error(f"Error in health check: {e}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500


@app.route('/api/refresh', methods=['POST'])
@limiter.limit("10 per minute")
def refresh():
    """Manually refresh data from source."""
    try:
        logger.info("Manual data refresh requested")
        success = refresh_data()
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Data refreshed successfully',
                'refresh_time': get_last_refresh_time().isoformat()
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to refresh data'
            }), 500
            
    except Exception as e:
        logger.error(f"Error refreshing data: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/api/cache/clear', methods=['POST'])
@limiter.limit("10 per minute")
def clear_cache():
    """Clear the data cache."""
    try:
        from data_loader import get_cache
        cache = get_cache()
        cache.clear()
        
        logger.info("Cache cleared via API")
        return jsonify({
            'status': 'success',
            'message': 'Cache cleared successfully'
        })
    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/api/cache/stats', methods=['GET'])
def cache_stats():
    """Get cache statistics."""
    try:
        return jsonify(get_cache_stats())
    except Exception as e:
        logger.error(f"Error getting cache stats: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


# Error handlers
@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors."""
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors."""
    logger.error(f"Internal server error: {error}")
    return jsonify({'error': 'Internal server error'}), 500


@app.errorhandler(429)
def ratelimit_handler(error):
    """Handle rate limit errors."""
    return jsonify({
        'error': 'Rate limit exceeded. Please try again later.',
        'retry_after': error.description
    }), 429


# Cleanup handlers
def cleanup():
    """Cleanup resources on shutdown."""
    logger.info("Shutting down...")
    stop_file_watcher()
    logger.info("Cleanup complete")


if __name__ == '__main__':
    # Register cleanup
    import atexit
    atexit.register(cleanup)
    
    # Create necessary directories
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    
    # Print startup banner
    print("\n" + "="*60)
    print("Monday.com Business Intelligence Agent - Production")
    print("="*60)
    print(f"Environment: {env}")
    print(f"Debug: {config.DEBUG}")
    print(f"Rate Limiting: {config.RATELIMIT_ENABLED}")
    print(f"Cache: {config.ENABLE_CACHE}")
    print(f"File Watcher: {config.ENABLE_FILE_WATCHER}")
    print(f"Live API: {config.USE_LIVE_API}")
    print("="*60)
    print(f"Starting web server at http://{'0.0.0.0' if env == 'production' else 'localhost'}:5000")
    print("Press Ctrl+C to stop the server")
    print("="*60 + "\n")
    
    # Run the app
    app.run(
        debug=config.DEBUG,
        host='0.0.0.0' if env == 'production' else '127.0.0.1',
        port=int(os.environ.get('PORT', 5000)),
        threaded=True
    )
