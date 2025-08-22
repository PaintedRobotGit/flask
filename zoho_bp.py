from flask import Blueprint, request, jsonify
import requests
from datetime import datetime, timedelta
import os
from config import ZOHO_CONFIG

# Create blueprint
zoho_bp = Blueprint('zoho', __name__, url_prefix='/api/zoho')

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

@zoho_bp.route('/reports/<report_name>', methods=['GET'])
def get_report_data(report_name):
    """Get data from a Zoho Creator report"""
    try:
        # Get criteria from query parameters
        criteria = request.args.get('criteria', '')
        
        # Build endpoint
        endpoint = f"/report/{report_name}"
        
        print(f"Attempting to fetch report: {report_name}")
        print(f"Full URL will be: {ZOHO_CONFIG['base_url']}/api/v2/{ZOHO_CONFIG['app_owner_name']}/{ZOHO_CONFIG['app_name']}{endpoint}")
        
        # Make API call
        data = call_zoho_api(endpoint, {'criteria': criteria} if criteria else None)
        
        print(f"Successfully fetched data for report: {report_name}")
        
        return jsonify({
            'success': True,
            'data': data,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"Error fetching report {report_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@zoho_bp.route('/config', methods=['GET'])
def get_config():
    """Get configuration for frontend"""
    return jsonify({
        'clientId': ZOHO_CONFIG['client_id'],
        'clientSecret': ZOHO_CONFIG['client_secret'],
        'refreshToken': ZOHO_CONFIG['refresh_token'],
        'appOwnerName': ZOHO_CONFIG['app_owner_name'],
        'appName': ZOHO_CONFIG['app_name'],
        'baseUrl': ZOHO_CONFIG['base_url'],
        'railwayUrl': ZOHO_CONFIG['railway_url']
    })

@zoho_bp.route('/health', methods=['GET'])
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

@zoho_bp.route('/generate-refresh-token', methods=['POST'])
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
            'redirect_uri': 'https://flask-production-aff4.up.railway.app/api/zoho/callback',
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

@zoho_bp.route('/auth-url', methods=['GET'])
def get_auth_url():
    """Get authorization URL for generating refresh token"""
    auth_url = (f"https://accounts.zoho.com/oauth/v2/auth?"
                f"response_type=code&"
                f"client_id={ZOHO_CONFIG['client_id']}&"
                f"scope=ZohoCreator.reports.READ&"
                f"redirect_uri=https://flask-production-aff4.up.railway.app/api/zoho/callback&"
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

@zoho_bp.route('/callback', methods=['GET'])
def oauth_callback():
    """OAuth callback endpoint"""
    code = request.args.get('code')
    if code:
        return jsonify({
            'success': True,
            'message': 'Authorization successful! Copy the code parameter and use it with the generate-refresh-token endpoint.',
            'code': code
        })
    else:
        return jsonify({
            'success': False,
            'error': 'No authorization code received'
        }), 400

@zoho_bp.route('/test-connection', methods=['GET'])
def test_connection():
    """Test Zoho API connection and list available reports"""
    try:
        print("Testing Zoho API connection...")
        
        # Test access token
        access_token = get_access_token()
        print(f"Access token obtained: {access_token[:20]}...")
        
        # Test basic API call to list reports
        url = f"{ZOHO_CONFIG['base_url']}/api/v2/{ZOHO_CONFIG['app_owner_name']}/{ZOHO_CONFIG['app_name']}/report"
        
        headers = {
            'Authorization': f'Zoho-oauthtoken {access_token}',
            'Content-Type': 'application/json'
        }
        
        print(f"Testing URL: {url}")
        response = requests.get(url, headers=headers)
        
        if response.ok:
            data = response.json()
            return jsonify({
                'success': True,
                'message': 'Zoho API connection successful',
                'reports': data.get('reports', []),
                'config': {
                    'app_owner': ZOHO_CONFIG['app_owner_name'],
                    'app_name': ZOHO_CONFIG['app_name'],
                    'base_url': ZOHO_CONFIG['base_url']
                }
            })
        else:
            return jsonify({
                'success': False,
                'error': f'Zoho API returned {response.status_code}',
                'response_text': response.text,
                'config': {
                    'app_owner': ZOHO_CONFIG['app_owner_name'],
                    'app_name': ZOHO_CONFIG['app_name'],
                    'base_url': ZOHO_CONFIG['base_url']
                }
            }), 500
            
    except Exception as e:
        print(f"Error testing connection: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return jsonify({
            'success': False,
            'error': str(e),
            'config': {
                'app_owner': ZOHO_CONFIG['app_owner_name'],
                'app_name': ZOHO_CONFIG['app_name'],
                'base_url': ZOHO_CONFIG['base_url']
            }
        }), 500
