from flask import Blueprint, jsonify, request
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import service_account
from datetime import date, timedelta
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
        api_method = "batchRunReports" if _is_batch_report(report_request) else "runReport"
        if report_type == "site_journey_flow_deep" and api_method == "runReport":
            response_json = _fetch_all_run_report_rows(
                access_token=access_token,
                property_id=property_id,
                report_request=report_request,
            )
        else:
            response = requests.post(
                f"{GA_API_BASE}/properties/{property_id}:{api_method}",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                json=report_request,
                timeout=(10, 45),
            )
            response.raise_for_status()
            response_json = response.json()
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

    if report_type in ("site_journey_flow", "site_journey_flow_deep"):
        depth = _to_int(payload.get("journey_depth")) or 4
        top_paths = 250
        journey_paths = _build_deep_journey_paths(
            response_json=response_json,
            depth=max(2, min(depth, 8)),
            top_paths=max(10, min(top_paths, 250)),
        )
        result = {
            "status": "ok",
            "propertyID": property_id,
            "report_type": report_type,
            "journey_paths": journey_paths,
            "journey_meta": {
                "depth": max(2, min(depth, 8)),
                "top_paths": max(10, min(top_paths, 250)),
                "fetched_row_count": response_json.get("fetchedRowCount")
                or len(response_json.get("rows", [])),
            },
        }
        result = _fit_journey_payload_size(result=result, char_limit=65535)
    else:
        result = {
            "status": "ok",
            "propertyID": property_id,
            "report_type": report_type,
            "data": response_json,
        }

    return jsonify(result), 200


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
    last_month_start, last_month_end = _get_last_month_range()
    last_month_range = [
        {
            "startDate": last_month_start.isoformat(),
            "endDate": last_month_end.isoformat(),
        }
    ]
    prior_year_start = last_month_start.replace(year=last_month_start.year - 1)
    prior_year_end = last_month_end.replace(year=last_month_end.year - 1)

    reports = {
        "overview": {
            "dateRanges": last_month_range,
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
            "dateRanges": last_month_range,
            "dimensions": [{"name": "sessionSourceMedium"}],
            "metrics": [
                {"name": "sessions"},
                {"name": "engagedSessions"},
                {"name": "conversions"},
            ],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": 100,
        },
        "acquisition_overview": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "sessionDefaultChannelGroup"}],
            "metrics": [
                {"name": "sessions"},
                {"name": "engagedSessions"},
            ],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": 100,
        },
        "acquisition_overview_active_new_users": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "date"}],
            "metrics": [
                {"name": "activeUsers"},
                {"name": "newUsers"},
            ],
            "orderBys": [{"dimension": {"dimensionName": "date"}}],
            "limit": 1000,
        },
        "acquisition_overview_new_users_by_primary_channel": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "firstUserPrimaryChannelGroup"}],
            "metrics": [
                {"name": "newUsers"},
            ],
            "orderBys": [{"metric": {"metricName": "newUsers"}, "desc": True}],
            "limit": 200,
        },
        "acquisition_overview_sessions_by_default_channel": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "sessionDefaultChannelGroup"}],
            "metrics": [
                {"name": "sessions"},
            ],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": 200,
        },
        "acquisition_overview_sessions_by_google_ads_campaign": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "sessionGoogleAdsCampaignName"}],
            "metrics": [
                {"name": "sessions"},
            ],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": 200,
        },
        "acquisition_overview_average_120d_value": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "date"}],
            "metrics": [
                {"name": "average120dValue"},
            ],
            "orderBys": [{"dimension": {"dimensionName": "date"}}],
            "limit": 1000,
        },
        "acquisition_overview_organic_impressions_by_landing_page": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "landingPagePlusQueryString"}],
            "metrics": [
                {"name": "organicGoogleSearchImpressions"},
            ],
            "orderBys": [
                {"metric": {"metricName": "organicGoogleSearchImpressions"}, "desc": True}
            ],
            "limit": 200,
        },
        "acquisition_overview_organic_clicks_by_search_query": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "organicGoogleSearchQuery"}],
            "metrics": [
                {"name": "organicGoogleSearchClicks"},
            ],
            "orderBys": [
                {"metric": {"metricName": "organicGoogleSearchClicks"}, "desc": True}
            ],
            "limit": 200,
        },
        "user_acquisition_by_source": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "firstUserPrimaryChannelGroup"}],
            "metrics": [
                {"name": "newUsers"},
            ],
            "orderBys": [{"metric": {"metricName": "newUsers"}, "desc": True}],
            "limit": 200,
        },
        "sessions_by_google_ads_campaign": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "sessionGoogleAdsCampaignName"}],
            "metrics": [
                {"name": "sessions"},
            ],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": 200,
        },
        "google_organic_search_traffic": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "landingPagePlusQueryString"}],
            "metrics": [
                {"name": "organicGoogleSearchClicks"},
            ],
            "dimensionFilter": {
                "andGroup": {
                    "expressions": [
                        {
                            "filter": {
                                "fieldName": "sessionSource",
                                "stringFilter": {
                                    "matchType": "EXACT",
                                    "value": "google",
                                    "caseSensitive": False,
                                },
                            }
                        },
                        {
                            "filter": {
                                "fieldName": "sessionMedium",
                                "stringFilter": {
                                    "matchType": "EXACT",
                                    "value": "organic",
                                    "caseSensitive": False,
                                },
                            }
                        },
                    ]
                }
            },
            "orderBys": [
                {"metric": {"metricName": "organicGoogleSearchClicks"}, "desc": True}
            ],
            "limit": 200,
        },
        "organic_google_search_queries": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "organicGoogleSearchQuery"}],
            "metrics": [
                {"name": "organicGoogleSearchClicks"},
            ],
            "orderBys": [
                {"metric": {"metricName": "organicGoogleSearchClicks"}, "desc": True}
            ],
            "limit": 200,
        },
        "acquisition_queries": {
            "dateRanges": last_month_range,
            "dimensions": [
                {"name": "sessionManualTerm"},
                {"name": "sessionSourceMedium"},
            ],
            "metrics": [
                {"name": "sessions"},
                {"name": "engagedSessions"},
                {"name": "conversions"},
            ],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": 200,
        },
        "acquition_queries": {
            "dateRanges": last_month_range,
            "dimensions": [
                {"name": "sessionManualTerm"},
                {"name": "sessionSourceMedium"},
            ],
            "metrics": [
                {"name": "sessions"},
                {"name": "engagedSessions"},
                {"name": "conversions"},
            ],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": 200,
        },
        "lead_acquisition": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "sessionDefaultChannelGroup"}],
            "metrics": [{"name": "conversions"}, {"name": "sessions"}],
            "metricFilter": {
                "filter": {
                    "fieldName": "conversions",
                    "numericFilter": {
                        "operation": "GREATER_THAN",
                        "value": {"doubleValue": 0},
                    },
                }
            },
            "orderBys": [{"metric": {"metricName": "conversions"}, "desc": True}],
            "limit": 100,
        },
        "non_google_campaign": {
            "dateRanges": last_month_range,
            "dimensions": [
                {"name": "sessionSource"},
                {"name": "sessionMedium"},
                {"name": "sessionCampaignName"},
            ],
            "metrics": [
                {"name": "sessions"},
                {"name": "conversions"},
                {"name": "totalRevenue"},
            ],
            "dimensionFilter": {
                "notExpression": {
                    "filter": {
                        "fieldName": "sessionSource",
                        "stringFilter": {
                            "matchType": "EXACT",
                            "value": "google",
                            "caseSensitive": False,
                        },
                    }
                }
            },
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": 200,
        },
        "engagement_overview": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "date"}],
            "metrics": [
                {"name": "engagedSessions"},
                {"name": "engagementRate"},
                {"name": "averageSessionDuration"},
                {"name": "eventCount"},
            ],
            "orderBys": [{"dimension": {"dimensionName": "date"}}],
            "limit": 1000,
        },
        "engagement_events": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "eventName"}],
            "metrics": [
                {"name": "eventCount"},
                {"name": "eventCountPerUser"},
                {"name": "totalUsers"},
            ],
            "orderBys": [{"metric": {"metricName": "eventCount"}, "desc": True}],
            "limit": 200,
        },
        "pages_and_screens": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "pagePath"}, {"name": "pageTitle"}],
            "metrics": [
                {"name": "screenPageViews"},
                {"name": "totalUsers"},
                {"name": "averageSessionDuration"},
            ],
            "orderBys": [
                {"metric": {"metricName": "screenPageViews"}, "desc": True}
            ],
            "limit": 200,
        },
        "site_journey_flow": {
            "dateRanges": last_month_range,
            "dimensions": [
                {"name": "eventName"},
                {"name": "pageReferrer"},
                {"name": "pagePath"},
                {"name": "pageTitle"},
            ],
            "metrics": [{"name": "activeUsers"}, {"name": "screenPageViews"}],
            "orderBys": [{"metric": {"metricName": "activeUsers"}, "desc": True}],
            "limit": 1000,
        },
        "site_journey_flow_deep": {
            "dateRanges": last_month_range,
            "dimensions": [
                {"name": "eventName"},
                {"name": "pageReferrer"},
                {"name": "pagePath"},
                {"name": "pageTitle"},
            ],
            "metrics": [{"name": "activeUsers"}, {"name": "screenPageViews"}],
            "orderBys": [{"metric": {"metricName": "activeUsers"}, "desc": True}],
            "limit": 1000,
        },
        "landing_page": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "landingPage"}],
            "metrics": [
                {"name": "sessions"},
                {"name": "engagedSessions"},
                {"name": "conversions"},
                {"name": "bounceRate"},
            ],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": 200,
        },
        "user_overview": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "date"}],
            "metrics": [
                {"name": "totalUsers"},
                {"name": "newUsers"},
                {"name": "activeUsers"},
                {"name": "returningUsers"},
            ],
            "orderBys": [{"dimension": {"dimensionName": "date"}}],
            "limit": 1000,
        },
        "audience": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "country"}, {"name": "language"}],
            "metrics": [{"name": "totalUsers"}, {"name": "newUsers"}],
            "orderBys": [{"metric": {"metricName": "totalUsers"}, "desc": True}],
            "limit": 200,
        },
        "demographic_details": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "country"}, {"name": "city"}],
            "metrics": [{"name": "totalUsers"}, {"name": "newUsers"}],
            "orderBys": [{"metric": {"metricName": "totalUsers"}, "desc": True}],
            "limit": 200,
        },
        "user_queries": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "sessionManualTerm"}],
            "metrics": [
                {"name": "sessions"},
                {"name": "engagedSessions"},
                {"name": "conversions"},
            ],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": 200,
        },
        "tech_details": {
            "dateRanges": last_month_range,
            "dimensions": [
                {"name": "deviceCategory"},
                {"name": "operatingSystem"},
                {"name": "browser"},
            ],
            "metrics": [{"name": "totalUsers"}, {"name": "sessions"}],
            "orderBys": [{"metric": {"metricName": "totalUsers"}, "desc": True}],
            "limit": 200,
        },
        "conversions_overview": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "date"}],
            "metrics": [
                {"name": "conversions"},
                {"name": "totalRevenue"},
                {"name": "purchaseRevenue"},
            ],
            "orderBys": [{"dimension": {"dimensionName": "date"}}],
            "limit": 1000,
        },
        "conversion_events": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "eventName"}],
            "metrics": [{"name": "conversions"}, {"name": "eventCount"}],
            "orderBys": [{"metric": {"metricName": "conversions"}, "desc": True}],
            "limit": 200,
        },
        "ecommerce_purchases": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "itemName"}],
            "metrics": [
                {"name": "itemPurchaseQuantity"},
                {"name": "itemRevenue"},
                {"name": "purchases"},
            ],
            "orderBys": [{"metric": {"metricName": "itemRevenue"}, "desc": True}],
            "limit": 200,
        },
        "purchase_journey": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "sessionDefaultChannelGroup"}],
            "metrics": [
                {"name": "addToCarts"},
                {"name": "checkouts"},
                {"name": "purchases"},
            ],
            "orderBys": [{"metric": {"metricName": "purchases"}, "desc": True}],
            "limit": 100,
        },
        "checkout_journey": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "deviceCategory"}],
            "metrics": [{"name": "checkouts"}, {"name": "purchases"}],
            "orderBys": [{"metric": {"metricName": "checkouts"}, "desc": True}],
            "limit": 50,
        },
        "retention_overview": {
            "dateRanges": last_month_range,
            "dimensions": [{"name": "date"}],
            "metrics": [{"name": "activeUsers"}, {"name": "returningUsers"}],
            "orderBys": [{"dimension": {"dimensionName": "date"}}],
            "limit": 1000,
        },
        "top_pages": {
            "dateRanges": last_month_range,
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
            "dateRanges": last_month_range,
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
            "dateRanges": last_month_range,
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
            "dateRanges": last_month_range,
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
            "dateRanges": last_month_range,
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
            "dateRanges": last_month_range,
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
            "dateRanges": last_month_range,
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
            "dateRanges": last_month_range,
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
            "dateRanges": last_month_range,
            "dimensions": [{"name": "hour"}],
            "metrics": [{"name": "activeUsers"}, {"name": "sessions"}],
            "orderBys": [{"dimension": {"dimensionName": "hour"}}],
            "limit": 24,
        },
        "daily_trend": {
            "dateRanges": last_month_range,
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
        "monthly_channel_breakdown": {
            "requests": [
                {
                    "dateRanges": [
                        {
                            "startDate": last_month_start.isoformat(),
                            "endDate": last_month_end.isoformat(),
                            "name": "current",
                        }
                    ],
                    "dimensions": [
                        {"name": "date"},
                        {"name": "sessionDefaultChannelGroup"},
                    ],
                    "metrics": [
                        {"name": "sessions"},
                        {"name": "totalUsers"},
                        {"name": "newUsers"},
                        {"name": "engagedSessions"},
                        {"name": "engagementRate"},
                        {"name": "averageSessionDuration"},
                    ],
                    "orderBys": [
                        {"dimension": {"dimensionName": "date"}, "desc": False}
                    ],
                    "limit": 10000,
                    "returnPropertyQuota": True,
                },
                {
                    "dateRanges": [
                        {
                            "startDate": last_month_start.isoformat(),
                            "endDate": last_month_end.isoformat(),
                            "name": "current",
                        },
                        {
                            "startDate": prior_year_start.isoformat(),
                            "endDate": prior_year_end.isoformat(),
                            "name": "prior_year",
                        },
                    ],
                    "dimensions": [
                        {"name": "sessionSource"},
                        {"name": "sessionMedium"},
                        {"name": "sessionCampaignName"},
                        {"name": "sessionDefaultChannelGroup"},
                    ],
                    "metrics": [
                        {"name": "sessions"},
                        {"name": "totalUsers"},
                        {"name": "newUsers"},
                        {"name": "engagedSessions"},
                        {"name": "engagementRate"},
                        {"name": "averageSessionDuration"},
                    ],
                    "orderBys": [
                        {"metric": {"metricName": "sessions"}, "desc": True}
                    ],
                    "limit": 100,
                },
            ]
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


def _is_batch_report(report_request):
    return isinstance(report_request, dict) and isinstance(
        report_request.get("requests"), list
    )


def _get_last_month_range():
    today = date.today()
    first_of_current_month = today.replace(day=1)
    last_of_previous_month = first_of_current_month - timedelta(days=1)
    first_of_previous_month = last_of_previous_month.replace(day=1)
    return first_of_previous_month, last_of_previous_month


def _to_int(value):
    try:
        return int(value)
    except Exception:
        return None


def _to_float(value):
    try:
        return float(value)
    except Exception:
        return None


def _build_deep_journey_paths(*, response_json, depth, top_paths):
    rows = response_json.get("rows", [])
    dimension_headers = [h.get("name", "") for h in response_json.get("dimensionHeaders", [])]
    metric_headers = [h.get("name", "") for h in response_json.get("metricHeaders", [])]

    idx_event = _header_index(dimension_headers, "eventName")
    idx_referrer = _header_index(dimension_headers, "pageReferrer")
    idx_path = _header_index(dimension_headers, "pagePath")
    idx_title = _header_index(dimension_headers, "pageTitle")
    idx_users = _header_index(metric_headers, "activeUsers")
    if idx_users is None:
        idx_users = 0

    edges = {}
    outgoing_totals = {}
    entry_targets = {}
    page_labels = {}
    for row in rows:
        dvals = [v.get("value", "") for v in row.get("dimensionValues", [])]
        mvals = [v.get("value", "") for v in row.get("metricValues", [])]

        event_name = dvals[idx_event] if idx_event is not None and idx_event < len(dvals) else ""
        referrer = _extract_path(dvals[idx_referrer]) if idx_referrer is not None and idx_referrer < len(dvals) else "(entry)"
        path = _normalize_path(dvals[idx_path]) if idx_path is not None and idx_path < len(dvals) else ""
        title = dvals[idx_title] if idx_title is not None and idx_title < len(dvals) else ""
        users = _to_float(mvals[idx_users]) if idx_users < len(mvals) else 0.0
        if not path or users is None or users <= 0:
            continue

        source = _normalize_path(referrer or "(entry)")
        target = path
        if target not in page_labels:
            page_labels[target] = title.strip() or target

        key = (source, target)
        edges[key] = edges.get(key, 0.0) + users
        outgoing_totals[source] = outgoing_totals.get(source, 0.0) + users
        if event_name == "session_start":
            entry_targets[target] = entry_targets.get(target, 0.0) + users

    adjacency = {}
    for (source, target), users in edges.items():
        prob = users / outgoing_totals[source] if outgoing_totals.get(source) else 0.0
        adjacency.setdefault(source, []).append(
            {"target": target, "users": users, "transition_probability": prob}
        )

    for source in adjacency:
        adjacency[source].sort(key=lambda x: x["users"], reverse=True)
        adjacency[source] = adjacency[source][:25]

    start_node = "(entry)"
    initial_edges = []
    if entry_targets:
        total_entry_users = sum(entry_targets.values())
        if total_entry_users > 0:
            for target, users in entry_targets.items():
                initial_edges.append(
                    {
                        "target": target,
                        "users": users,
                        "transition_probability": users / total_entry_users,
                    }
                )
            initial_edges.sort(key=lambda x: x["users"], reverse=True)
            initial_edges = initial_edges[:25]
    if not initial_edges:
        initial_edges = adjacency.get(start_node, [])
    if not initial_edges:
        initial_edges = []
        for source, transitions in adjacency.items():
            if source.startswith("http"):
                initial_edges.extend(transitions)
        initial_edges.sort(key=lambda x: x["users"], reverse=True)
        initial_edges = initial_edges[:25]

    paths = []
    for edge in initial_edges:
        first_label = page_labels.get(edge["target"], edge["target"])
        _dfs_paths(
            adjacency=adjacency,
            current_node=edge["target"],
            current_path=[first_label],
            cumulative_users=edge["users"],
            cumulative_prob=edge["transition_probability"],
            max_depth=depth,
            paths=paths,
            visited={edge["target"]},
            page_labels=page_labels,
        )

    paths.sort(key=lambda x: (x["est_users"], x["prob"]), reverse=True)
    return paths[:top_paths]


def _dfs_paths(
    *,
    adjacency,
    current_node,
    current_path,
    cumulative_users,
    cumulative_prob,
    max_depth,
    paths,
    visited,
    page_labels,
):
    paths.append(
        {
            "p": current_path.copy(),
            "est_users": round(cumulative_users, 2),
            "prob": round(cumulative_prob, 6),
            "d": len(current_path),
        }
    )

    if len(current_path) >= max_depth:
        return

    for edge in adjacency.get(current_node, []):
        next_node = edge["target"]
        if next_node in visited:
            continue
        next_label = page_labels.get(next_node, next_node)
        _dfs_paths(
            adjacency=adjacency,
            current_node=next_node,
            current_path=current_path + [next_label],
            cumulative_users=min(cumulative_users, edge["users"]),
            cumulative_prob=cumulative_prob * edge["transition_probability"],
            max_depth=max_depth,
            paths=paths,
            visited=visited | {next_node},
            page_labels=page_labels,
        )


def _header_index(headers, name):
    for i, header in enumerate(headers):
        if header == name:
            return i
    return None


def _extract_path(referrer):
    if not referrer:
        return "(entry)"
    value = str(referrer).strip()
    if not value:
        return "(entry)"

    marker = "://"
    idx = value.find(marker)
    if idx >= 0:
        slash_idx = value.find("/", idx + len(marker))
        if slash_idx >= 0:
            value = value[slash_idx:]
        else:
            value = "/"

    q_idx = value.find("?")
    if q_idx >= 0:
        value = value[:q_idx]

    return value or "(entry)"


def _normalize_path(value):
    if not value:
        return "(entry)"

    parsed = _extract_path(str(value))
    parsed = parsed.strip()
    if not parsed:
        return "(entry)"
    if not parsed.startswith("/"):
        if parsed == "(entry)":
            return parsed
        parsed = "/" + parsed
    return parsed


def _fit_journey_payload_size(*, result, char_limit):
    paths = result.get("journey_paths") or []
    meta = result.setdefault("journey_meta", {})
    current_size = _json_char_count(result)
    if current_size <= char_limit:
        meta["returned_paths"] = len(paths)
        meta["trimmed_for_size"] = False
        meta["char_count"] = current_size
        meta["char_limit"] = char_limit
        return result

    low = 0
    high = len(paths)
    best_count = 0
    best_size = None

    while low <= high:
        mid = (low + high) // 2
        candidate = dict(result)
        candidate["journey_paths"] = paths[:mid]
        size = _json_char_count(candidate)
        if size <= char_limit:
            best_count = mid
            best_size = size
            low = mid + 1
        else:
            high = mid - 1

    result["journey_paths"] = paths[:best_count]
    final_size = best_size if best_size is not None else _json_char_count(result)
    meta["returned_paths"] = best_count
    meta["trimmed_for_size"] = best_count < len(paths)
    meta["char_count"] = final_size
    meta["char_limit"] = char_limit
    return result


def _json_char_count(value):
    return len(json.dumps(value, separators=(",", ":"), ensure_ascii=False))


def _fetch_all_run_report_rows(*, access_token, property_id, report_request):
    limit = _to_int(report_request.get("limit")) or 1000
    limit = max(100, min(limit, 100000))
    offset = 0
    all_rows = []
    base_response = None

    while True:
        paged_request = dict(report_request)
        paged_request["limit"] = limit
        paged_request["offset"] = offset

        response = requests.post(
            f"{GA_API_BASE}/properties/{property_id}:runReport",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json=paged_request,
            timeout=(10, 45),
        )
        response.raise_for_status()
        page_json = response.json()

        if base_response is None:
            base_response = page_json.copy()
            base_response["rows"] = []

        page_rows = page_json.get("rows", [])
        all_rows.extend(page_rows)

        row_count = _to_int(page_json.get("rowCount"))
        fetched_count = len(all_rows)
        if not page_rows:
            break
        if row_count is not None and fetched_count >= row_count:
            break
        if len(page_rows) < limit:
            break

        offset += len(page_rows)

    if base_response is None:
        return {"rows": []}

    base_response["rows"] = all_rows
    base_response["fetchedRowCount"] = len(all_rows)
    return base_response
