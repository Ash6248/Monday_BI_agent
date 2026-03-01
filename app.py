"""
Monday.com Business Intelligence Agent - Flask Web Application
A conversational interface for business intelligence queries.
"""
from flask import Flask, render_template, request, jsonify
from data_loader import initialize_data, get_data_quality_notes
from query_processor import QueryProcessor
import os

app = Flask(__name__)
app.secret_key = 'monday-bi-agent-secret-key'

# Initialize data and query processor
print("Initializing Monday.com BI Agent...")
data_initialized = initialize_data()
print(f"Data initialization: {'Success' if data_initialized else 'Failed'}")

processor = QueryProcessor()

@app.route('/')
def index():
    """Render the main chat interface."""
    return render_template('index.html')

@app.route('/api/query', methods=['POST'])
def handle_query():
    """Handle user queries via API."""
    data = request.get_json()
    query = data.get('query', '').strip()
    
    if not query:
        return jsonify({'error': 'Please enter a query'})
    
    # Process the query
    result = processor.process_query(query)
    
    # Add data quality notes
    result['data_notes'] = get_data_quality_notes()
    
    return jsonify(result)

@app.route('/api/help', methods=['GET'])
def help():
    """Return help information."""
    return jsonify(processor.get_help())

@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({
        'status': 'running',
        'data_loaded': data_initialized
    })

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    
    # Run the app
    print("\n" + "="*50)
    print("Monday.com Business Intelligence Agent")
    print("="*50)
    print("Starting web server at http://localhost:5000")
    print("Press Ctrl+C to stop the server")
    print("="*50 + "\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
