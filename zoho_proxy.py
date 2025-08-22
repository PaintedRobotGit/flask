from flask import Flask, request, jsonify
import requests
import json
from datetime import datetime, timedelta
import os

app = Flask(__name__)

# Zoho Creator Configuration
ZOHO_CONFIG = {
    'client_id': '1000.NHPZ1CTLITJBBNG4WQCL1OC8GOEFIG',
    'client_secret': '07dcee8c0291f26b1ae6ed6c6ee040a02e530fecc2',
    'refresh_token': '1000.2e9f82d7cd4485477685fc15398d0637.0f5e7a7f227a9a5a121fc0a626f7a272',  # Replace with your actual refresh token
    'app_owner_name': 'creator',  # Replace with your app owner name
    'app_name': 'precision-ops',  # Replace with your app name
    'base_url': 'https://creator.zoho.com'
}

# Cache for access token
access_token_cache = {
    'token': None,
    'expires_at': None
}

def get_access_token():
    """Get access token using refresh token with caching"""
    global access_token_cache
    
    # Check if we have a valid cached token
    if (access_token_cache['token'] and 
        access_token_cache['expires_at'] and 
        datetime.now() < access_token_cache['expires_at']):
        return access_token_cache['token']
    
    try:
        # Get new access token
        token_url = 'https://accounts.zoho.com/oauth/v2/token'
        token_data = {
            'refresh_token': ZOHO_CONFIG['refresh_token'],
            'client_id': ZOHO_CONFIG['client_id'],
            'client_secret': ZOHO_CONFIG['client_secret'],
            'grant_type': 'refresh_token'
        }
        
        response = requests.post(token_url, data=token_data)
        response.raise_for_status()
        
        token_info = response.json()
        access_token = token_info['access_token']
        expires_in = token_info.get('expires_in', 3600)  # Default to 1 hour
        
        # Cache the token (subtract 5 minutes for safety)
        access_token_cache = {
            'token': access_token,
            'expires_at': datetime.now() + timedelta(seconds=expires_in - 300)
        }
        
        print(f"New access token obtained, expires at {access_token_cache['expires_at']}")
        return access_token
        
    except requests.exceptions.RequestException as e:
        print(f"Error getting access token: {e}")
        raise

def call_zoho_api(endpoint, params=None):
    """Make API call to Zoho Creator"""
    try:
        access_token = get_access_token()
        
        url = f"{ZOHO_CONFIG['base_url']}/api/v2/{ZOHO_CONFIG['app_owner_name']}/{ZOHO_CONFIG['app_name']}{endpoint}"
        
        headers = {
            'Authorization': f'Zoho-oauthtoken {access_token}',
            'Content-Type': 'application/json'
        }
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error calling Zoho API: {e}")
        raise

@app.route('/api/zoho/reports/<report_name>', methods=['GET'])
def get_report_data(report_name):
    """Get data from a Zoho Creator report"""
    try:
        # Get criteria from query parameters
        criteria = request.args.get('criteria', '')
        
        # Build endpoint
        endpoint = f"/report/{report_name}"
        
        # Make API call
        data = call_zoho_api(endpoint, {'criteria': criteria} if criteria else None)
        
        return jsonify({
            'success': True,
            'data': data,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/zoho/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'config': {
            'app_owner': ZOHO_CONFIG['app_owner_name'],
            'app_name': ZOHO_CONFIG['app_name'],
            'has_refresh_token': bool(ZOHO_CONFIG['refresh_token'] != 'YOUR_REFRESH_TOKEN_HERE')
        }
    })

@app.route('/api/zoho/generate-refresh-token', methods=['POST'])
def generate_refresh_token():
    """Generate refresh token using authorization code"""
    try:
        data = request.get_json()
        auth_code = data.get('code')
        
        if not auth_code:
            return jsonify({
                'success': False,
                'error': 'Authorization code is required'
            }), 400
        
        # Exchange authorization code for refresh token
        token_url = 'https://accounts.zoho.com/oauth/v2/token'
        token_data = {
            'code': auth_code,
            'client_id': ZOHO_CONFIG['client_id'],
            'client_secret': ZOHO_CONFIG['client_secret'],
            'redirect_uri': 'http://localhost:5000/api/zoho/callback',  # Update this to your actual redirect URI
            'grant_type': 'authorization_code'
        }
        
        response = requests.post(token_url, data=token_data)
        response.raise_for_status()
        
        token_info = response.json()
        
        if 'refresh_token' in token_info:
            return jsonify({
                'success': True,
                'refresh_token': token_info['refresh_token'],
                'access_token': token_info.get('access_token'),
                'expires_in': token_info.get('expires_in'),
                'message': 'Copy the refresh_token to your ZOHO_CONFIG'
            })
        else:
            return jsonify({
                'success': False,
                'error': 'No refresh token in response',
                'response': token_info
            }), 400
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/zoho/auth-url', methods=['GET'])
def get_auth_url():
    """Get authorization URL for generating refresh token"""
    auth_url = (f"https://accounts.zoho.com/oauth/v2/auth?"
                f"response_type=code&"
                f"client_id={ZOHO_CONFIG['client_id']}&"
                f"scope=ZohoCreator.reports.READ&"
                f"redirect_uri=http://localhost:5000/api/zoho/callback&"
                f"access_type=offline")
    
    return jsonify({
        'auth_url': auth_url,
        'instructions': [
            '1. Open the auth_url in your browser',
            '2. Authorize the application',
            '3. Copy the "code" parameter from the redirect URL',
            '4. Use POST /api/zoho/generate-refresh-token with the code'
        ]
    })

# Add CORS headers to all responses
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

if __name__ == '__main__':
    app.run(debug=True, port=5000)
