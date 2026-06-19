from flask import Blueprint, request, redirect
from urllib.parse import urlencode
import os
import html
import requests


google_ads_oauth_bp = Blueprint("google_ads_oauth", __name__)

GOOGLE_AUTH_URI = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"
ADS_SCOPE = "https://www.googleapis.com/auth/adwords"


def _client_config():
    client_id = (os.environ.get("GOOGLE_ADS_CLIENT_ID") or "").strip()
    client_secret = (os.environ.get("GOOGLE_ADS_CLIENT_SECRET") or "").strip()
    return client_id, client_secret


def _redirect_uri():
    # Must EXACTLY match an Authorized redirect URI on the OAuth client in Cloud Console.
    configured = (os.environ.get("GOOGLE_ADS_OAUTH_REDIRECT_URI") or "").strip()
    if configured:
        return configured
    # Fall back to building it from the request (force https for proxied hosts like Railway).
    base = request.url_root.replace("http://", "https://")
    return base + "google_ads_oauth/callback"


def _page(title, body_html):
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        f"<title>{html.escape(title)}</title>"
        "<style>"
        "body{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;"
        "background:#0f1115;color:#e6e8eb;margin:0;padding:40px;line-height:1.5;}"
        ".card{max-width:680px;margin:0 auto;background:#171a21;border:1px solid #262b34;"
        "border-radius:14px;padding:32px;}"
        "h1{font-size:20px;margin:0 0 8px;} p{color:#aab1bd;}"
        "a.btn,button.btn{display:inline-block;background:#3b82f6;color:#fff;border:0;"
        "padding:12px 20px;border-radius:8px;font-size:15px;text-decoration:none;cursor:pointer;}"
        "textarea{width:100%;box-sizing:border-box;background:#0f1115;color:#e6e8eb;"
        "border:1px solid #2b313b;border-radius:8px;padding:12px;font-family:monospace;"
        "font-size:13px;margin-top:8px;}"
        ".label{font-size:12px;text-transform:uppercase;letter-spacing:.04em;color:#7d8694;"
        "margin-top:20px;}"
        ".err{background:#2a1416;border:1px solid #5b2327;color:#f7b4b8;padding:14px;"
        "border-radius:8px;white-space:pre-wrap;font-family:monospace;font-size:12px;}"
        "</style></head><body><div class='card'>" + body_html + "</div></body></html>"
    )


@google_ads_oauth_bp.route("/google_ads_oauth", methods=["GET"])
def google_ads_oauth_start():
    client_id, client_secret = _client_config()
    if not client_id or not client_secret:
        return _page(
            "Configuration needed",
            "<h1>OAuth not configured</h1>"
            "<p>Set <code>GOOGLE_ADS_CLIENT_ID</code> and "
            "<code>GOOGLE_ADS_CLIENT_SECRET</code> as environment variables on the server, "
            "then reload this page.</p>",
        ), 500

    params = {
        "client_id": client_id,
        "redirect_uri": _redirect_uri(),
        "response_type": "code",
        "scope": ADS_SCOPE,
        "access_type": "offline",
        # Force a consent prompt so Google always returns a refresh_token,
        # even if this account has authorized before.
        "prompt": "consent",
        "include_granted_scopes": "true",
    }
    auth_url = f"{GOOGLE_AUTH_URI}?{urlencode(params)}"

    body = (
        "<h1>Authorize Google Ads access</h1>"
        "<p>You're granting Painted Robot read access to Google Ads reporting data. "
        "Sign in with the Google account that can see the Ads accounts, then approve. "
        "On the next screen you'll get a token to copy back.</p>"
        f"<p style='margin-top:24px'><a class='btn' href='{html.escape(auth_url)}'>"
        "Authorize with Google</a></p>"
    )
    return _page("Authorize Google Ads access", body)


@google_ads_oauth_bp.route("/google_ads_oauth/callback", methods=["GET"])
def google_ads_oauth_callback():
    error = request.args.get("error")
    if error:
        return _page(
            "Authorization failed",
            "<h1>Authorization was cancelled</h1>"
            f"<div class='err'>{html.escape(error)}</div>"
            "<p><a class='btn' href='/google_ads_oauth' style='margin-top:20px'>Try again</a></p>",
        ), 400

    code = request.args.get("code", "")
    if not code:
        return _page(
            "Missing code",
            "<h1>No authorization code returned</h1>"
            "<p><a class='btn' href='/google_ads_oauth'>Start over</a></p>",
        ), 400

    client_id, client_secret = _client_config()
    try:
        response = requests.post(
            GOOGLE_TOKEN_URI,
            data={
                "code": code,
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": _redirect_uri(),
                "grant_type": "authorization_code",
            },
            timeout=(10, 30),
        )
        response.raise_for_status()
        token_json = response.json()
    except requests.HTTPError as http_error:
        detail = ""
        if http_error.response is not None:
            try:
                detail = json_pretty(http_error.response.json())
            except Exception:
                detail = http_error.response.text
        return _page(
            "Token exchange failed",
            "<h1>Could not exchange the code for tokens</h1>"
            f"<div class='err'>{html.escape(detail or str(http_error))}</div>",
        ), 400
    except requests.RequestException as request_error:
        return _page(
            "Token exchange failed",
            "<h1>Request to Google failed</h1>"
            f"<div class='err'>{html.escape(str(request_error))}</div>",
        ), 502

    refresh_token = token_json.get("refresh_token")
    if not refresh_token:
        return _page(
            "No refresh token",
            "<h1>Google did not return a refresh token</h1>"
            "<p>This usually means the account already authorized this app. "
            "Remove access at "
            "<a href='https://myaccount.google.com/permissions' style='color:#7db1ff'>"
            "myaccount.google.com/permissions</a> and try again.</p>"
            "<p><a class='btn' href='/google_ads_oauth'>Start over</a></p>",
        ), 400

    safe_token = html.escape(refresh_token)
    body = (
        "<h1>Success — copy this refresh token</h1>"
        "<p>Send this value back. It's the only piece needed to finish setup. "
        "Keep it secret — it grants ongoing read access.</p>"
        "<div class='label'>Refresh token</div>"
        f"<textarea id='rt' rows='3' readonly onclick='this.select()'>{safe_token}</textarea>"
        "<p style='margin-top:16px'>"
        "<button class='btn' onclick=\"navigator.clipboard.writeText("
        "document.getElementById('rt').value);this.textContent='Copied';\">"
        "Copy to clipboard</button></p>"
    )
    return _page("Refresh token", body)


def json_pretty(obj):
    import json
    return json.dumps(obj, indent=2)
