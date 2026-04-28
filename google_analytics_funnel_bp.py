from flask import Blueprint, jsonify, request
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import service_account
from datetime import date, timedelta
import json
import requests


google_analytics_funnel_bp = Blueprint("google_analytics_funnel", __name__)

GA_SCOPE = "https://www.googleapis.com/auth/analytics.readonly"
GA_ALPHA_API_BASE = "https://analyticsdata.googleapis.com/v1alpha"


@google_analytics_funnel_bp.route("/google_analytics_funnel_report", methods=["POST"])
def google_analytics_funnel_report():
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
        funnel_query = _build_funnel_request(report_type, payload.get("query"))
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
            f"{GA_ALPHA_API_BASE}/properties/{property_id}:runFunnelReport",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json=funnel_query,
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
                "message": "Google Analytics Funnel API HTTP error",
                "details": str(http_error),
                "response": error_body,
            }
        ), http_error.response.status_code if http_error.response else 502
    except requests.RequestException as request_error:
        return jsonify(
            {
                "status": "error",
                "message": "Google Analytics Funnel API request failed",
                "details": str(request_error),
            }
        ), 502

    return jsonify(
        {
            "status": "ok",
            "propertyID": property_id,
            "endpoint": "runFunnelReport",
            "report_type": report_type or "custom",
            "data": response.json(),
            "normalized_funnel_rows": _normalize_funnel_rows(
                response.json(),
                payload.get("funnel_name") or report_type or "custom_funnel",
                payload.get("source_medium"),
            ),
        }
    ), 200


def _extract_payload():
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
        "Invalid 'query' field. For funnel reports, provide 'query' as a JSON object or JSON string."
    )


def _build_funnel_request(report_type: str, query):
    custom_query = _parse_custom_query(query)
    if report_type in ("", "custom"):
        if not custom_query:
            raise ValueError(
                "Provide 'query' for custom funnel requests, or set a preset 'report_type'."
            )
        return custom_query

    last_month_start, last_month_end = _get_last_month_range()
    current_month_range = [
        {
            "startDate": last_month_start.isoformat(),
            "endDate": last_month_end.isoformat(),
            "name": "last_month",
        }
    ]

    presets = {
        "path_exploration": {
            "dateRanges": current_month_range,
            "funnel": {
                "isOpenFunnel": True,
                "steps": [
                    {
                        "name": "Landing page view",
                        "filterExpression": {
                            "funnelEventFilter": {"eventName": "page_view"}
                        },
                    },
                    {
                        "name": "Engaged session",
                        "filterExpression": {
                            "funnelEventFilter": {"eventName": "user_engagement"}
                        },
                    },
                    {
                        "name": "Key action",
                        "filterExpression": {
                            "orGroup": {
                                "expressions": [
                                    {
                                        "funnelEventFilter": {
                                            "eventName": "generate_lead"
                                        }
                                    },
                                    {
                                        "funnelEventFilter": {"eventName": "purchase"}
                                    },
                                ]
                            }
                        },
                    },
                ],
            },
            "funnelVisualizationType": "STANDARD_FUNNEL",
            "returnPropertyQuota": True,
        },
        "conversion_journey": {
            "dateRanges": current_month_range,
            "funnel": {
                "isOpenFunnel": False,
                "steps": [
                    {
                        "name": "Session start",
                        "filterExpression": {
                            "funnelEventFilter": {"eventName": "session_start"}
                        },
                    },
                    {
                        "name": "View item",
                        "filterExpression": {
                            "funnelEventFilter": {"eventName": "view_item"}
                        },
                    },
                    {
                        "name": "Add to cart",
                        "filterExpression": {
                            "funnelEventFilter": {"eventName": "add_to_cart"}
                        },
                    },
                    {
                        "name": "Begin checkout",
                        "filterExpression": {
                            "funnelEventFilter": {"eventName": "begin_checkout"}
                        },
                    },
                    {
                        "name": "Purchase",
                        "filterExpression": {
                            "funnelEventFilter": {"eventName": "purchase"}
                        },
                    },
                ],
            },
            "funnelVisualizationType": "STANDARD_FUNNEL",
            "returnPropertyQuota": True,
        },
        "lead_conversion_journey": {
            "dateRanges": current_month_range,
            "funnel": {
                "isOpenFunnel": False,
                "steps": [
                    {
                        "name": "Session start",
                        "filterExpression": {
                            "funnelEventFilter": {"eventName": "session_start"}
                        },
                    },
                    {
                        "name": "View lead form",
                        "filterExpression": {
                            "funnelEventFilter": {"eventName": "view_item"}
                        },
                    },
                    {
                        "name": "Submit form",
                        "filterExpression": {
                            "orGroup": {
                                "expressions": [
                                    {
                                        "funnelEventFilter": {
                                            "eventName": "generate_lead"
                                        }
                                    },
                                    {
                                        "funnelEventFilter": {"eventName": "form_submit"}
                                    },
                                ]
                            }
                        },
                    },
                ],
            },
            "funnelVisualizationType": "STANDARD_FUNNEL",
            "returnPropertyQuota": True,
        },
        "checkout_journey": {
            "dateRanges": current_month_range,
            "funnel": {
                "isOpenFunnel": False,
                "steps": [
                    {
                        "name": "View item",
                        "filterExpression": {
                            "funnelEventFilter": {"eventName": "view_item"}
                        },
                    },
                    {
                        "name": "Add to cart",
                        "filterExpression": {
                            "funnelEventFilter": {"eventName": "add_to_cart"}
                        },
                    },
                    {
                        "name": "Begin checkout",
                        "filterExpression": {
                            "funnelEventFilter": {"eventName": "begin_checkout"}
                        },
                    },
                    {
                        "name": "Add payment info",
                        "filterExpression": {
                            "funnelEventFilter": {"eventName": "add_payment_info"}
                        },
                    },
                    {
                        "name": "Purchase",
                        "filterExpression": {
                            "funnelEventFilter": {"eventName": "purchase"}
                        },
                    },
                ],
            },
            "funnelVisualizationType": "STANDARD_FUNNEL",
            "returnPropertyQuota": True,
        },
    }

    preset = presets.get(report_type)
    if preset is None:
        allowed = ", ".join(sorted(list(presets.keys()) + ["custom"]))
        raise ValueError(
            f"Unsupported funnel report_type '{report_type}'. Supported values: {allowed}"
        )

    return preset


def _get_last_month_range():
    today = date.today()
    first_of_current_month = today.replace(day=1)
    last_of_previous_month = first_of_current_month - timedelta(days=1)
    first_of_previous_month = last_of_previous_month.replace(day=1)
    return first_of_previous_month, last_of_previous_month


def _normalize_funnel_rows(report_json, funnel_name, source_medium):
    funnel_table = report_json.get("funnelTable") or {}
    dimension_headers = [h.get("name", "") for h in funnel_table.get("dimensionHeaders", [])]
    metric_headers = [h.get("name", "") for h in funnel_table.get("metricHeaders", [])]
    rows = funnel_table.get("rows", [])

    normalized = []
    stage1_users = None
    previous_stage_users = None

    for row in rows:
        row_dimensions = _extract_value_list(row.get("dimensionValues", []))
        row_metrics = _extract_value_list(row.get("metricValues", []))
        row_dimension_map = {
            name: row_dimensions[i] if i < len(row_dimensions) else ""
            for i, name in enumerate(dimension_headers)
        }
        row_metric_map = _build_metric_map(metric_headers, row_metrics)

        stage_number = _to_int(
            row_dimension_map.get("funnelStepIndex")
            or row_dimension_map.get("step")
            or row_dimension_map.get("stepIndex")
        )
        stage_name = (
            row_dimension_map.get("funnelStepName")
            or row_dimension_map.get("stepName")
            or row_dimension_map.get("funnelStep")
            or f"Stage {stage_number if stage_number is not None else len(normalized) + 1}"
        )
        users = _first_numeric_metric(
            row_metric_map,
            (
                "activeUsers",
                "totalUsers",
                "funnelStepUsers",
                "users",
            ),
        )

        if stage_number is None:
            stage_number = len(normalized) + 1

        if stage1_users is None and users is not None:
            stage1_users = users
        if previous_stage_users is None and users is not None:
            previous_stage_users = users

        conversion_rate = None
        if stage1_users and users is not None:
            conversion_rate = round((users / stage1_users) * 100.0, 4)

        step_drop_off = None
        if previous_stage_users is not None and users is not None and previous_stage_users > 0:
            step_drop_off = round(((previous_stage_users - users) / previous_stage_users) * 100.0, 4)

        normalized.append(
            {
                "funnel_name": funnel_name,
                "stage_number": stage_number,
                "stage_name": stage_name,
                "users": users,
                "conversion_rate": conversion_rate,
                "step_drop_off": step_drop_off,
                "source_medium": source_medium or row_dimension_map.get("sessionSourceMedium", ""),
                "dimensions": row_dimension_map,
                "metrics": row_metric_map,
            }
        )

        if users is not None:
            previous_stage_users = users

    return normalized


def _extract_value_list(values):
    extracted = []
    for value in values:
        if isinstance(value, dict):
            extracted.append(value.get("value", ""))
        else:
            extracted.append(value)
    return extracted


def _to_int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def _to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _first_numeric_metric(metric_map, preferred_keys):
    for key in preferred_keys:
        metric_value = _to_float(metric_map.get(key))
        if metric_value is not None:
            return metric_value

    for metric_value_raw in metric_map.values():
        metric_value = _to_float(metric_value_raw)
        if metric_value is not None:
            return metric_value

    return None


def _build_metric_map(metric_headers, row_metrics):
    metric_map = {}
    for i, name in enumerate(metric_headers):
        value = row_metrics[i] if i < len(row_metrics) else ""
        existing = metric_map.get(name)

        # Preserve first meaningful metric value when headers repeat.
        if existing not in (None, "") and value in (None, ""):
            continue
        if existing not in (None, "") and value not in (None, ""):
            continue

        metric_map[name] = value

    return metric_map
