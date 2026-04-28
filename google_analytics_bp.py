from flask import Blueprint, jsonify, request
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import service_account
import json
import requests


google_analytics_bp = Blueprint("google_analytics", __name__)

GA_SCOPE = "https://www.googleapis.com/auth/analytics.readonly"
GA_API_BASE = "https://analyticsdata.googleapis.com/v1beta"


@google_analytics_bp.route("/google_analytics_report", methods=["POST"])
def google_analytics_report():
    payload = _extract_payload()

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
        report_request = _build_report_request(report_type, payload.get("query"))
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


def _extract_payload():
    # Prefer JSON payloads, but allow form-encoded bodies from tools like Deluge.
    json_payload = request.get_json(silent=True)
    if isinstance(json_payload, dict):
        return json_payload

    form_payload = request.form.to_dict(flat=True)
    if form_payload:
        return form_payload

    raw_body = request.get_data(cache=False, as_text=True) or ""
    if raw_body:
        try:
            parsed = json.loads(raw_body)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

    return {}


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


def _build_report_request(report_type: str, query=None):
    last_30_days = [{"startDate": "30daysAgo", "endDate": "yesterday"}]

    reports = {
        "overview": {
            "dateRanges": last_30_days,
            "dimensions": [{"name": "date"}],
            "metrics": [
                {"name": "activeUsers"},
                {"name": "newUsers"},
                {"name": "sessions"},
                {"name": "screenPageViews"},
                {"name": "conversions"},
            ],
            "limit": 1000,
        },
        "traffic_sources": {
            "dateRanges": last_30_days,
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
            "dateRanges": last_30_days,
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
        "landing_pages": {
            "dateRanges": last_30_days,
            "dimensions": [{"name": "landingPage"}],
            "metrics": [
                {"name": "sessions"},
                {"name": "engagedSessions"},
                {"name": "conversions"},
                {"name": "bounceRate"},
            ],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": 100,
        },
        "geo_countries": {
            "dateRanges": last_30_days,
            "dimensions": [{"name": "country"}],
            "metrics": [
                {"name": "activeUsers"},
                {"name": "sessions"},
                {"name": "conversions"},
            ],
            "orderBys": [{"metric": {"metricName": "activeUsers"}, "desc": True}],
            "limit": 100,
        },
        "geo_cities": {
            "dateRanges": last_30_days,
            "dimensions": [{"name": "city"}],
            "metrics": [
                {"name": "activeUsers"},
                {"name": "sessions"},
                {"name": "conversions"},
            ],
            "orderBys": [{"metric": {"metricName": "activeUsers"}, "desc": True}],
            "limit": 100,
        },
        "devices": {
            "dateRanges": last_30_days,
            "dimensions": [{"name": "deviceCategory"}],
            "metrics": [
                {"name": "activeUsers"},
                {"name": "sessions"},
                {"name": "engagedSessions"},
            ],
            "orderBys": [{"metric": {"metricName": "activeUsers"}, "desc": True}],
            "limit": 10,
        },
        "channels": {
            "dateRanges": last_30_days,
            "dimensions": [{"name": "sessionDefaultChannelGroup"}],
            "metrics": [
                {"name": "sessions"},
                {"name": "engagedSessions"},
                {"name": "conversions"},
                {"name": "totalRevenue"},
            ],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": 50,
        },
        "campaigns": {
            "dateRanges": last_30_days,
            "dimensions": [{"name": "sessionCampaignName"}],
            "metrics": [
                {"name": "sessions"},
                {"name": "engagedSessions"},
                {"name": "conversions"},
                {"name": "totalRevenue"},
            ],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": 100,
        },
        "events": {
            "dateRanges": last_30_days,
            "dimensions": [{"name": "eventName"}],
            "metrics": [
                {"name": "eventCount"},
                {"name": "totalUsers"},
                {"name": "eventCountPerUser"},
            ],
            "orderBys": [{"metric": {"metricName": "eventCount"}, "desc": True}],
            "limit": 100,
        },
        "hourly_trend": {
            "dateRanges": last_30_days,
            "dimensions": [{"name": "hour"}],
            "metrics": [{"name": "activeUsers"}, {"name": "sessions"}],
            "orderBys": [{"dimension": {"dimensionName": "hour"}}],
            "limit": 24,
        },
        "daily_trend": {
            "dateRanges": last_30_days,
            "dimensions": [{"name": "date"}],
            "metrics": [
                {"name": "activeUsers"},
                {"name": "sessions"},
                {"name": "conversions"},
                {"name": "totalRevenue"},
            ],
            "orderBys": [{"dimension": {"dimensionName": "date"}}],
            "limit": 1000,
        },
    }

    if report_type == "custom":
        custom_query = _parse_custom_query(query)
        if not custom_query:
            raise ValueError(
                "For report_type 'custom', provide optional field 'query' as a JSON object or JSON string."
            )
        return custom_query

    report_request = reports.get(report_type)
    if report_request is None:
        allowed = ", ".join(sorted(list(reports.keys()) + ["custom"]))
        raise ValueError(
            f"Unsupported report_type '{report_type}'. Supported values: {allowed}"
        )

    return report_request


def _parse_custom_query(query):
    if query is None:
        return None

    if isinstance(query, dict):
        return query

    if isinstance(query, str):
        query_text = query.strip()
        if not query_text:
            return None
        try:
            parsed = json.loads(query_text)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

    raise ValueError(
        "Invalid 'query' field. For custom reports, provide 'query' as a JSON object or JSON string."
    )
