from flask import Blueprint, request, jsonify
import requests
from typing import Any, Dict, Tuple, Optional
import json
import time
import threading
import os


validation_ai_bp = Blueprint("validation_ai", __name__)


@validation_ai_bp.route("/validation_ai", methods=["POST"])
def validate_ai_payload():
    payload = request.get_json(silent=True) or {}

    # New inputs
    record_id = payload.get("Record_ID")
    debug_mode: bool = bool(payload.get("DEBUG", False))

    # Keys for processing
    user_data: Any = payload.get("data")
    gemini_key_from_payload: str = str(payload.get("Gemini_Key", "")).strip()
    gemini_key_env: str = os.getenv("GEMINI_KEY", "").strip()
    gemini_key: str = gemini_key_from_payload or gemini_key_env

    model: str = "gemini-2.5-flash"
    # Allow multi-minute operations
    read_timeout_seconds: int = int(payload.get("Timeout_Seconds", 300))  # default 5 min
    connect_timeout_seconds: int = int(payload.get("Connect_Timeout_Seconds", 60))
    if read_timeout_seconds < 60:
        read_timeout_seconds = 60
    if connect_timeout_seconds < 10:
        connect_timeout_seconds = 10

    # When DEBUG=true, behave as before: run and return the data to the caller
    if debug_mode:
        missing_keys = []
        if not gemini_key:
            missing_keys.append("Gemini_Key")
        if user_data is None:
            missing_keys.append("data")
        if missing_keys:
            return (
                jsonify({
                    "status": "error",
                    "message": "Missing required keys",
                    "missing": missing_keys,
                }),
                400,
            )

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

    # Non-debug mode: validate inputs, return immediately, process in background and POST to Zoho
    zoho_return_url = "https://www.zohoapis.com/creator/custom/paintedrobot/NSM_AI_Validate_Return?publickey=AhhvXBa53te27Zp0pzx1Jqz6D"

    missing_keys = []
    if record_id in (None, ""):
        missing_keys.append("Record_ID")
    if user_data is None:
        missing_keys.append("data")
    if not gemini_key:
        missing_keys.append("Gemini_Key")

    if missing_keys:
        return (
            jsonify({
                "status": "error",
                "message": "Missing required keys",
                "missing": missing_keys,
            }),
            400,
        )

    def _background_worker(record_id_value: Any, gemini_api_key: str, payload_data: Any, model_name: str, rt_seconds: int, ct_seconds: int, callback_url: str) -> None:
        try:
            system_instruction_text, user_prompt_text = _build_ad_agency_prompts(payload_data)
            completion_text, vendor_response = _call_gemini_generate_content(
                api_key=gemini_api_key,
                model=model_name,
                prompt=user_prompt_text,
                system_instruction=system_instruction_text,
                read_timeout_seconds=rt_seconds,
                connect_timeout_seconds=ct_seconds,
            )
            parsed = _parse_strict_json_object(completion_text)
            result_body: Dict[str, Any] = {
                "status": "ok",
                "Record_ID": record_id_value,
                "model": model_name,
                "output": parsed,
            }
        except requests.HTTPError as http_err:
            result_body = {
                "status": "error",
                "Record_ID": record_id_value,
                "message": "Gemini API HTTP error",
                "details": str(http_err),
                "response": getattr(http_err, "response", None).text if getattr(http_err, "response", None) else None,
            }
        except requests.RequestException as req_err:
            result_body = {
                "status": "error",
                "Record_ID": record_id_value,
                "message": "Gemini API request failed",
                "details": str(req_err),
            }
        except ValueError as parse_err:
            result_body = {
                "status": "error",
                "Record_ID": record_id_value,
                "message": "Model output was not valid JSON object",
                "details": str(parse_err),
            }
        except Exception as e:
            result_body = {
                "status": "error",
                "Record_ID": record_id_value,
                "message": "Unexpected processing error",
                "details": str(e),
            }

        try:
            requests.post(
                callback_url,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                json={"data": result_body},
                timeout=(10, 60),
            )
        except Exception:
            # Swallow callback errors to avoid crashing the worker thread
            pass

    worker = threading.Thread(
        target=_background_worker,
        args=(record_id, gemini_key, user_data, model, read_timeout_seconds, connect_timeout_seconds, zoho_return_url),
        daemon=True,
    )
    worker.start()

    return jsonify({
        "status": "accepted",
        "message": "Processing started",
        "Record_ID": record_id,
    }), 202


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
        "Your task is to find comprehensive information about a company based on the provided seed hints. "
        "Follow these steps sequentially to ensure all data points are covered:\n\n"
        "1. **Identify the official company website and domain.** Use a targeted Google Search query like '[company name] official website' or a search on the provided domain.\n"
        "2. **Find social media profiles.** For each of the following platforms, perform a dedicated, targeted search. **IMPORTANT:** If you do not find a direct, verifiable, and authoritative URL for a social media profile, you **MUST** return `null` for that field. Do not guess or fabricate URLs.\n"
        "   - **LinkedIn**: Search for `site:linkedin.com \"[Company Name]\"` and return the URL of the official company page.\n"
        "   - **Facebook**: Search for `site:facebook.com \"[Company Name]\"` and return the URL of the official page.\n"
        "   - **Instagram**: Search for `site:instagram.com \"[Company Name]\"` and return the URL of the official page.\n"
        "   - **Twitter**: Search for `site:twitter.com \"[Company Name]\"` and return the URL of the official profile.\n"
        "   - **YouTube**: Search for `site:youtube.com \"[Company Name]\"` and return the URL of the official channel.\n"
        "   - **TikTok**: Search for `site:tiktok.com \"[Company Name]\"` and return the URL of the official profile.\n"
        "   - **GitHub**: Search for `site:github.com \"[Company Name]\"` and return the URL of the official organization or profile.\n"
        "3. **Gather other key information.** Use a series of broad and specific searches to fill out the remaining fields in the JSON schema, such as industry, key personnel, and competitors.\n\n"
        "Return a single JSON object with the following schema and rules:\n"
        "{\n"
        "  \"company_name\": string | null,\n"
        "  \"known_domains\": string[],\n"
        "  \"social_media\": {\n"
        "    \"linkedin\": string | null,\n"
        "    \"twitter\": string | null,\n"
        "    \"facebook\": string | null,\n"
        "    \"instagram\": string | null,\n"
        "    \"tiktok\": string | null,\n"
        "    \"youtube\": string | null,\n"
        "    \"other\": string[]\n"
        "  },\n"
        "  \"contact\": {\n"
        "    \"emails\": string[],\n"
        "    \"phones\": string[],\n"
        "    \"addresses\": string[]\n"
        "  },\n"
        "  \"industry\": string | null,\n"
        "  \"size_employees\": number | null,\n"
        "  \"annual_revenue\": number | null,\n"
        "  \"locations\": string[],\n"
        "  \"key_personnel\": [{ \"name\": string, \"title\": string | null, \"linkedin_profile\": string | null, \"email\": string | null, \"phone\": string | null }],\n"
        "  \"products_services\": string[],\n"
        "  \"value_proposition\": string | null,\n"
        "  \"marketing_insights\": {\n"
        "    \"audience\": string | null,\n"
        "    \"tone_style\": string | null,\n"
        "    \"differentiators\": string[],\n"
        "    \"competitors\": string[]\n"
        "    \"digital_marketing_opportunities\": string[]\n"
        "  },\n"
        "  \"suggested_pitch_points\": string[],\n"
        "  \"missing_information\": string[],\n"
        "  \"website_audit\": string[],\n"
        "  \"confidence\": number,  // 0.0 - 1.0\n"
        "  \"sources\": string[]   // distinct URLs that substantiate the data\n"
        "}\n\n"
        "Rules:\n"
        "- Do not invent URLs, emails, or names.\n"
        "- Normalize all URLs to include `https://`.\n"
        "- Deduplicate all lists (e.g., `known_domains`, `sources`).\n"
        "- **All phone numbers must be formatted with the correct country calling code, for example, `+1 (555) 555-5555` for US/Canada numbers.**\n"
        "- If multiple candidates exist for a field, pick the most authoritative or include the top 3.\n"
        "- Output only the JSON object, with no prose or explanation outside of it.\n\n"
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

    # Helper: try parse JSON string strictly as dict
    def _try_parse_object(candidate: str) -> Optional[Dict[str, Any]]:
        try:
            parsed_candidate = json.loads(candidate)
            if isinstance(parsed_candidate, dict):
                return parsed_candidate
            return None
        except json.JSONDecodeError:
            return None

    # 1) Fast path: if the whole thing is JSON
    parsed = _try_parse_object(cleaned)
    if parsed is not None:
        return parsed

    # 2) If there is a fenced block anywhere, try the first fenced content
    if "```" in cleaned:
        parts = cleaned.split("```")
        # parts alternates: [before, maybe lang, content, maybe lang, content, ...]
        for idx in range(1, len(parts), 2):
            fenced_block = parts[idx]
            # If language tag present, drop first line
            fenced_lines = fenced_block.splitlines()
            if fenced_lines:
                first_line = fenced_lines[0].strip().lower()
                if first_line in ("json", "js", "javascript"):  # common tags
                    fenced_lines = fenced_lines[1:]
            candidate = "\n".join(fenced_lines).strip()
            parsed = _try_parse_object(candidate)
            if parsed is not None:
                return parsed

    # 3) Fallback: extract the first balanced {...} block and parse it
    start = cleaned.find("{")
    while start != -1:
        i = start
        depth = 0
        in_string = False
        escape = False
        for i, ch in enumerate(cleaned[start:], start):
            if in_string:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == '"':
                    in_string = False
                continue
            else:
                if ch == '"':
                    in_string = True
                elif ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        candidate = cleaned[start:i+1]
                        parsed = _try_parse_object(candidate)
                        if parsed is not None:
                            return parsed
                        break
        # Look for next '{' and try again
        start = cleaned.find("{", start + 1)

    raise ValueError("JSON decode error: Could not extract a valid JSON object from model output")


