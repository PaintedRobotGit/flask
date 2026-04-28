from flask import Blueprint, jsonify, request
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import service_account
import requests


google_analytics_bp = Blueprint("google_analytics", __name__)

GA_SCOPE = "https://www.googleapis.com/auth/analytics.readonly"
GA_API_BASE = "https://analyticsdata.googleapis.com/v1beta"


@google_analytics_bp.route("/google_analytics_report", methods=["POST"])
def google_analytics_report():
    payload = request.get_json(silent=True) or {}

    client_email = str(payload.get("clientEmail", "")).strip()
    private_key = str(payload.get("privateKey", "")).strip()
    property_id = str(payload.get("propertyID", "")).strip()
    report_type = str(payload.get("report_type", "")).strip().lower()

    missing_fields = [
        field_name
        for field_name, value in (
            ("clientEmail", client_email),
            ("privateKey", private_key),
            ("propertyID", property_id),
            ("report_type", report_type),
        )
        if not value
    ]
    if missing_fields:
        return jsonify(
            {
                "status": "error",
                "message": "Missing required fields",
                "missing_fields": missing_fields,
            }
        ), 400

    try:
        access_token = _get_google_access_token(
            client_email=client_email, private_key=private_key
        )
        report_request = _build_report_request(report_type)
    except ValueError as value_error:
        return jsonify({"status": "error", "message": str(value_error)}), 400
    except Exception as auth_error:
        return jsonify(
            {
                "status": "error",
                "message": "Failed to authenticate with Google",
                "details": str(auth_error),
            }
        ), 400

    try:
        response = requests.post(
            f"{GA_API_BASE}/properties/{property_id}:runReport",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json=report_request,
            timeout=(10, 45),
        )
        response.raise_for_status()
    except requests.HTTPError as http_error:
        error_body = None
        if http_error.response is not None:
            try:
                error_body = http_error.response.json()
            except Exception:
                error_body = http_error.response.text
        return jsonify(
            {
                "status": "error",
                "message": "Google Analytics API HTTP error",
                "details": str(http_error),
                "response": error_body,
            }
        ), http_error.response.status_code if http_error.response else 502
    except requests.RequestException as request_error:
        return jsonify(
            {
                "status": "error",
                "message": "Google Analytics API request failed",
                "details": str(request_error),
            }
        ), 502

    return jsonify(
        {
            "status": "ok",
            "propertyID": property_id,
            "report_type": report_type,
            "data": response.json(),
        }
    ), 200


def _get_google_access_token(*, client_email: str, private_key: str) -> str:
    normalized_private_key = private_key.replace("\\n", "\n")
    credentials = service_account.Credentials.from_service_account_info(
        {
            "type": "service_account",
            "client_email": client_email,
            "private_key": normalized_private_key,
            "token_uri": "https://oauth2.googleapis.com/token",
        },
        scopes=[GA_SCOPE],
    )
    credentials.refresh(GoogleAuthRequest())
    if not credentials.token:
        raise ValueError("Failed to obtain Google access token")
    return credentials.token


def _build_report_request(report_type: str):
    reports = {
        "overview": {
            "dateRanges": [{"startDate": "7daysAgo", "endDate": "yesterday"}],
            "dimensions": [{"name": "date"}],
            "metrics": [
                {"name": "activeUsers"},
                {"name": "newUsers"},
                {"name": "sessions"},
                {"name": "screenPageViews"},
            ],
            "limit": 1000,
        },
        "traffic_sources": {
            "dateRanges": [{"startDate": "30daysAgo", "endDate": "yesterday"}],
            "dimensions": [{"name": "sessionSourceMedium"}],
            "metrics": [
                {"name": "sessions"},
                {"name": "engagedSessions"},
                {"name": "conversions"},
            ],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": 100,
        },
        "top_pages": {
            "dateRanges": [{"startDate": "30daysAgo", "endDate": "yesterday"}],
            "dimensions": [{"name": "pagePath"}],
            "metrics": [
                {"name": "screenPageViews"},
                {"name": "averageSessionDuration"},
                {"name": "bounceRate"},
            ],
            "orderBys": [
                {"metric": {"metricName": "screenPageViews"}, "desc": True}
            ],
            "limit": 100,
        },
    }

    report_request = reports.get(report_type)
    if report_request is None:
        allowed = ", ".join(sorted(reports.keys()))
        raise ValueError(
            f"Unsupported report_type '{report_type}'. Supported values: {allowed}"
        )

    return report_request
