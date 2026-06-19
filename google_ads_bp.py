from flask import Blueprint, jsonify, request
import json
import requests


google_ads_bp = Blueprint("google_ads", __name__)

# Google Ads API version is part of the URL and is deprecated roughly yearly.
# Bump this when Google retires the version.
GOOGLE_ADS_API_VERSION = "v24"
GOOGLE_ADS_API_BASE = f"https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}"
GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"


@google_ads_bp.route("/google_ads_report", methods=["POST"])
def google_ads_report():
    payload = _extract_payload()

    client_id = str(payload.get("clientId", "")).strip()
    client_secret = str(payload.get("clientSecret", "")).strip()
    developer_token = str(payload.get("developerToken", "")).strip()
    refresh_token = str(payload.get("refreshToken", "")).strip()
    customer_id = _normalize_customer_id(payload.get("customerId", ""))
    login_customer_id = _normalize_customer_id(payload.get("loginCustomerId", ""))
    report_type = str(payload.get("report_type", "")).strip().lower()

    missing_fields = [
        field_name
        for field_name, value in (
            ("clientId", client_id),
            ("clientSecret", client_secret),
            ("developerToken", developer_token),
            ("refreshToken", refresh_token),
            ("customerId", customer_id),
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
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
        )
        gaql_query = _build_gaql_query(report_type, payload.get("query"))
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

    headers = {
        "Authorization": f"Bearer {access_token}",
        "developer-token": developer_token,
        "Content-Type": "application/json",
    }
    # Required when a manager (MCC) account calls on behalf of a client account.
    if login_customer_id:
        headers["login-customer-id"] = login_customer_id

    try:
        response_json = _fetch_all_search_rows(
            headers=headers,
            customer_id=customer_id,
            gaql_query=gaql_query,
        )
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
                "message": "Google Ads API HTTP error",
                "details": str(http_error),
                "response": error_body,
            }
        ), http_error.response.status_code if http_error.response else 502
    except requests.RequestException as request_error:
        return jsonify(
            {
                "status": "error",
                "message": "Google Ads API request failed",
                "details": str(request_error),
            }
        ), 502

    return jsonify(
        {
            "status": "ok",
            "customerID": customer_id,
            "report_type": report_type,
            "data": response_json,
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


def _normalize_customer_id(value):
    # Google Ads customer IDs are 10 digits; the API wants them without dashes/spaces.
    return "".join(ch for ch in str(value) if ch.isdigit())


def _get_google_access_token(*, client_id: str, client_secret: str, refresh_token: str) -> str:
    response = requests.post(
        GOOGLE_TOKEN_URI,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=(10, 30),
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise ValueError("Failed to obtain Google access token")
    return token


def _build_gaql_query(report_type: str, query=None):
    if report_type == "custom":
        custom_query = _parse_custom_query(query)
        if not custom_query:
            raise ValueError(
                "For report_type 'custom', provide field 'query' as a GAQL string."
            )
        return custom_query

    reports = {
        "account_overview": (
            "SELECT customer.id, customer.descriptive_name, customer.currency_code, "
            "metrics.impressions, metrics.clicks, metrics.ctr, metrics.average_cpc, "
            "metrics.cost_micros, metrics.conversions, metrics.conversions_value "
            "FROM customer WHERE segments.date DURING LAST_MONTH"
        ),
        "campaign_performance": (
            "SELECT campaign.id, campaign.name, campaign.status, "
            "campaign.advertising_channel_type, metrics.impressions, metrics.clicks, "
            "metrics.ctr, metrics.average_cpc, metrics.cost_micros, metrics.conversions, "
            "metrics.conversions_value, metrics.cost_per_conversion "
            "FROM campaign WHERE segments.date DURING LAST_MONTH "
            "ORDER BY metrics.cost_micros DESC"
        ),
        "ad_group_performance": (
            "SELECT campaign.name, ad_group.id, ad_group.name, ad_group.status, "
            "metrics.impressions, metrics.clicks, metrics.ctr, metrics.cost_micros, "
            "metrics.conversions, metrics.conversions_value "
            "FROM ad_group WHERE segments.date DURING LAST_MONTH "
            "ORDER BY metrics.cost_micros DESC"
        ),
        "keyword_performance": (
            "SELECT campaign.name, ad_group.name, ad_group_criterion.keyword.text, "
            "ad_group_criterion.keyword.match_type, metrics.impressions, metrics.clicks, "
            "metrics.ctr, metrics.average_cpc, metrics.cost_micros, metrics.conversions, "
            "metrics.conversions_value "
            "FROM keyword_view WHERE segments.date DURING LAST_MONTH "
            "ORDER BY metrics.cost_micros DESC"
        ),
        "search_terms": (
            "SELECT campaign.name, ad_group.name, search_term_view.search_term, "
            "metrics.impressions, metrics.clicks, metrics.ctr, metrics.cost_micros, "
            "metrics.conversions "
            "FROM search_term_view WHERE segments.date DURING LAST_MONTH "
            "ORDER BY metrics.impressions DESC"
        ),
        "ad_performance": (
            "SELECT campaign.name, ad_group.name, ad_group_ad.ad.id, "
            "ad_group_ad.ad.name, ad_group_ad.status, metrics.impressions, "
            "metrics.clicks, metrics.ctr, metrics.cost_micros, metrics.conversions "
            "FROM ad_group_ad WHERE segments.date DURING LAST_MONTH "
            "ORDER BY metrics.impressions DESC"
        ),
        "daily_performance": (
            "SELECT segments.date, metrics.impressions, metrics.clicks, "
            "metrics.cost_micros, metrics.conversions, metrics.conversions_value "
            "FROM customer WHERE segments.date DURING LAST_MONTH "
            "ORDER BY segments.date"
        ),
        "device_performance": (
            "SELECT segments.device, metrics.impressions, metrics.clicks, "
            "metrics.cost_micros, metrics.conversions, metrics.conversions_value "
            "FROM customer WHERE segments.date DURING LAST_MONTH"
        ),
        "geo_performance": (
            "SELECT campaign.name, segments.geo_target_region, metrics.impressions, "
            "metrics.clicks, metrics.cost_micros, metrics.conversions "
            "FROM geographic_view WHERE segments.date DURING LAST_MONTH "
            "ORDER BY metrics.impressions DESC"
        ),
        "conversion_actions": (
            "SELECT segments.conversion_action_name, metrics.conversions, "
            "metrics.conversions_value, metrics.all_conversions "
            "FROM customer WHERE segments.date DURING LAST_MONTH"
        ),
    }

    gaql_query = reports.get(report_type)
    if gaql_query is None:
        allowed = ", ".join(sorted(list(reports.keys()) + ["custom"]))
        raise ValueError(
            f"Unsupported report_type '{report_type}'. Supported values: {allowed}"
        )

    return gaql_query


def _parse_custom_query(query):
    if query is None:
        return None

    if isinstance(query, str):
        query_text = query.strip()
        return query_text or None

    # Allow a JSON object wrapper like {"query": "SELECT ..."} for convenience.
    if isinstance(query, dict):
        inner = query.get("query")
        if isinstance(inner, str) and inner.strip():
            return inner.strip()

    raise ValueError(
        "Invalid 'query' field. For custom reports, provide 'query' as a GAQL string."
    )


def _fetch_all_search_rows(*, headers, customer_id, gaql_query):
    url = f"{GOOGLE_ADS_API_BASE}/customers/{customer_id}/googleAds:search"
    all_results = []
    field_mask = None
    page_token = None

    while True:
        body = {"query": gaql_query}
        if page_token:
            body["pageToken"] = page_token

        response = requests.post(url, headers=headers, json=body, timeout=(10, 60))
        response.raise_for_status()
        page_json = response.json()

        all_results.extend(page_json.get("results", []))
        if field_mask is None:
            field_mask = page_json.get("fieldMask")

        page_token = page_json.get("nextPageToken")
        if not page_token:
            break

    return {
        "results": all_results,
        "fieldMask": field_mask,
        "rowCount": len(all_results),
    }
