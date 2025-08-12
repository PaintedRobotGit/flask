from flask import Blueprint, request, jsonify
import requests
from typing import Any, Dict, Tuple, Optional
import json
import time


validation_ai_bp = Blueprint("validation_ai", __name__)


@validation_ai_bp.route("/validation_ai", methods=["POST"])
def validate_ai_payload():
    payload = request.get_json(silent=True) or {}

    required_keys = ["OpenAI_Key", "Gemini_Key", "data"]
    missing_keys = [key for key in required_keys if key not in payload]

    if missing_keys:
        return (
            jsonify({
                "status": "error",
                "message": "Missing required keys",
                "missing": missing_keys,
            }),
            400,
        )

    gemini_key: str = str(payload.get("Gemini_Key", ""))
    user_data: Any = payload.get("data")
    model: str = "gemini-2.5-flash"
    # Allow multi-minute operations
    read_timeout_seconds: int = int(payload.get("Timeout_Seconds", 300))  # default 5 min
    connect_timeout_seconds: int = int(payload.get("Connect_Timeout_Seconds", 60))
    if read_timeout_seconds < 60:
        read_timeout_seconds = 60
    if connect_timeout_seconds < 10:
        connect_timeout_seconds = 10

    # Input may contain seed hints like company name/domain; external research is handled by Gemini tools

    # Build prompts for Gemini: system instruction + user prompt
    try:
        system_instruction_text, user_prompt_text = _build_ad_agency_prompts(user_data)
    except ValueError as err:
        return jsonify({
            "status": "error",
            "message": str(err),
        }), 400

    try:
        completion_text, vendor_response = _call_gemini_generate_content(
            api_key=gemini_key,
            model=model,
            prompt=user_prompt_text,
            system_instruction=system_instruction_text,
            read_timeout_seconds=read_timeout_seconds,
            connect_timeout_seconds=connect_timeout_seconds,
        )
    except requests.HTTPError as http_err:
        return jsonify({
            "status": "error",
            "message": "Gemini API HTTP error",
            "details": str(http_err),
            "response": getattr(http_err, "response", None).text if getattr(http_err, "response", None) else None,
        }), 502
    except requests.RequestException as req_err:
        return jsonify({
            "status": "error",
            "message": "Gemini API request failed",
            "details": str(req_err),
        }), 502

    # Parse the model output as strict JSON object
    try:
        parsed = _parse_strict_json_object(completion_text)
    except ValueError as parse_err:
        return jsonify({
            "status": "error",
            "message": "Model output was not valid JSON object",
            "details": str(parse_err),
            "raw_output": completion_text,
        }), 502

    return jsonify({
        "status": "ok",
        "model": model,
        "output": parsed,
    })


def _call_gemini_generate_content(*, api_key: str, model: str, prompt: str, system_instruction: Optional[str] = None, read_timeout_seconds: int = 300, connect_timeout_seconds: int = 60) -> Tuple[str, Dict[str, Any]]:
    """Call Gemini's REST API generateContent endpoint and return the text output.

    Returns a tuple of (text, full_response_dict).
    """
    if not api_key:
        raise requests.RequestException("Missing Gemini API key")

    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    headers = {
        "x-goog-api-key": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload: Dict[str, Any] = {
        "contents": [
            {
                "parts": [{"text": prompt}],
            }
        ],
        "tools": [
            {"google_search": {}}
        ]
    }
    if system_instruction:
        payload["systemInstruction"] = {
            "parts": [{"text": system_instruction}]
        }

    # Retry on read timeouts with simple exponential backoff
    max_attempts = 2
    attempt = 0
    last_exc: Optional[Exception] = None
    while attempt < max_attempts:
        try:
            response = requests.post(
                endpoint,
                headers=headers,
                json=payload,
                timeout=(connect_timeout_seconds, read_timeout_seconds),  # (connect timeout, read timeout)
            )
            response.raise_for_status()
            data: Dict[str, Any] = response.json()
            break
        except requests.Timeout as e:
            last_exc = e
            attempt += 1
            if attempt >= max_attempts:
                raise
            time.sleep(1.5 ** attempt)
        except Exception:
            raise

    # Extract first candidate text if available
    text_output = ""
    try:
        candidates = data.get("candidates") or []
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            text_output = "".join(part.get("text", "") for part in parts if isinstance(part, dict))
    except Exception:
        # Keep empty text_output on parsing issues; caller handles
        pass

    return text_output, data


def _build_ad_agency_prompts(user_data: Any) -> Tuple[str, str]:
    """Build system instruction and user prompt tailored for an ad-agency assistant.

    Accepts either a string or any JSON-serializable structure in user_data.
    Returns (system_instruction_text, user_prompt_text).
    """
    # Normalize input into a text block to embed verbatim for analysis
    if isinstance(user_data, str):
        data_block = user_data
    else:
        try:
            import json as _json
            data_block = _json.dumps(user_data, ensure_ascii=False, indent=2)
        except Exception:
            raise ValueError("Unsupported 'data' format. Provide a string or JSON-serializable object.")

    system_text = (
        "You are an elite advertising agency assistant. Your job is to research companies on the public web using Google Search "
        "and compile accurate, marketing-relevant intelligence. Treat the input as seed hints (e.g., company name, domain, notes). "
        "Prioritize official and authoritative sources. Do not fabricate or guess; if data is unavailable, leave fields empty or null. "
        "Normalize and deduplicate all outputs."
    )

    # Request structured JSON to make downstream usage easy
    user_text = (
        "Research the company using Google Search. Use the following seed hints to guide your search.\n"
        "Return a single JSON object with this schema: \n"
        "{\n"
        "  \"company_name\": string | null,\n"
        "  \"known_domains\": string[],\n"
        "  \"social_media\": {\n"
        "    \"linkedin\": string | null,\n"
        "    \"twitter\": string | null,\n"
        "    \"facebook\": string | null,\n"
        "    \"instagram\": string | null,\n"
        "    \"tiktok\": string | null,\n"
        "    \"youtube\": string | null,\n"
        "    \"github\": string | null,\n"
        "    \"other\": string[]\n"
        "  },\n"
        "  \"contact\": {\n"
        "    \"emails\": string[],\n"
        "    \"phones\": string[],\n"
        "    \"addresses\": string[]\n"
        "  },\n"
        "  \"industry\": string | null,\n"
        "  \"size_employees\": number | null,\n"
        "  \"locations\": string[],\n"
        "  \"key_personnel\": [{ \"name\": string, \"title\": string | null, \"link\": string | null }],\n"
        "  \"products_services\": string[],\n"
        "  \"value_proposition\": string | null,\n"
        "  \"marketing_insights\": {\n"
        "    \"audience\": string | null,\n"
        "    \"tone_style\": string | null,\n"
        "    \"differentiators\": string[],\n"
        "    \"competitors\": string[]\n"
        "  },\n"
        "  \"suggested_pitch_points\": string[],\n"
        "  \"missing_information\": string[],\n"
        "  \"confidence\": number,  // 0.0 - 1.0\n"
        "  \"sources\": string[]   // distinct URLs that substantiate the data\n"
        "}\n\n"
        "Rules:\n"
        "- Perform web research with Google Search. Prefer official sites and verified profiles.\n"
        "- Do not invent URLs, emails, or names.\n"
        "- Normalize URLs (include https scheme). Deduplicate lists.\n"
        "- If multiple candidates exist, pick the most authoritative or include the top 3.\n"
        "- Output only the JSON object, with no prose.\n\n"
        f"Seed hints:\n{data_block}"
    )

    return system_text, user_text


def _parse_strict_json_object(text: str) -> Dict[str, Any]:
    """Parse a string as a strict JSON object (dict). Reject code fences and arrays.

    Raises ValueError on any issues.
    """
    if not isinstance(text, str) or not text.strip():
        raise ValueError("Empty model output")

    cleaned = text.strip()
    # Remove common code fence wrappers if the model added them despite instructions
    if cleaned.startswith("```"):
        # Strip the first fence line and the trailing fence if present
        lines = cleaned.splitlines()
        # Drop first line
        lines = lines[1:]
        # Drop trailing fence line if present
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON decode error: {e}")

    if not isinstance(parsed, dict):
        raise ValueError("Expected a JSON object at top level")

    return parsed


