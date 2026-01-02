from flask import Flask, jsonify, send_from_directory
from validation_ai import validation_ai_bp
from zoho_bp import zoho_bp
from daily_brief import daily_brief_bp
import os

app = Flask(__name__)
app.register_blueprint(validation_ai_bp)
app.register_blueprint(zoho_bp)
app.register_blueprint(daily_brief_bp)

# Add CORS headers
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

@app.route('/')
def index():
    return jsonify({"Choo Choo": "Welcome to your Flask app 🚅"})

@app.route('/charts')
def charts():
    """Serve the charts page"""
    return send_from_directory('app', 'index.html')

@app.route('/app/<path:filename>')
def serve_app_files(filename):
    """Serve static files from the app directory"""
    return send_from_directory('app', filename)