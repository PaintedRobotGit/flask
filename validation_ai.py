from flask import Blueprint, request, jsonify
import requests
from typing import Any, Dict, Tuple, Optional
import json
import logging
import time
import threading
import os


validation_ai_bp = Blueprint("validation_ai", __name__)
logger = logging.getLogger("validation_ai")


@validation_ai_bp.route("/validation_ai", methods=["POST"])
def validate_ai_payload():
    payload = request.get_json(silent=True) or {}

    # New inputs
    record_id = payload.get("Record_ID")
    debug_mode: bool = bool(payload.get("DEBUG", False))
    customer_type: str = str(payload.get("customer_type", "")).strip().lower()

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
        logger.info("validation_ai: DEBUG request record_id=%s customer_type=%s data_keys=%s", record_id, customer_type, list(user_data.keys()) if isinstance(user_data, dict) else type(user_data).__name__)
        missing_keys = []
        if not gemini_key:
            missing_keys.append("Gemini_Key")
        if user_data is None:
            missing_keys.append("data")
        if missing_keys:
            logger.warning("validation_ai: missing keys %s", missing_keys)
            return (
                jsonify({
                    "status": "error",
                    "message": "Missing required keys",
                    "missing": missing_keys,
                }),
                400,
            )

        vendor_response = None
        try:
            # Primary call - common fields for all customer types
            system_instruction_text, user_prompt_text = _build_ad_agency_prompts_primary(user_data)
            completion_text, vendor_response = _call_gemini_generate_content(
                api_key=gemini_key,
                model=model,
                prompt=user_prompt_text,
                system_instruction=system_instruction_text,
                read_timeout_seconds=read_timeout_seconds,
                connect_timeout_seconds=connect_timeout_seconds,
            )
        except ValueError as err:
            return jsonify({
                "status": "error",
                "message": str(err),
            }), 400
        except requests.HTTPError as http_err:
            return jsonify({
                "status": "error",
                "message": "Gemini API HTTP error (primary call)",
                "details": str(http_err),
                "response": getattr(http_err, "response", None).text if getattr(http_err, "response", None) else None,
            }), 502
        except requests.RequestException as req_err:
            return jsonify({
                "status": "error",
                "message": "Gemini API request failed (primary call)",
                "details": str(req_err),
                "gemini_response": vendor_response,
            }), 502

        # Check for empty output before parsing
        if not completion_text or not completion_text.strip():
            return jsonify({
                "status": "error",
                "message": "Gemini API returned empty output (primary call)",
                "details": "The API response contained no text content",
                "gemini_response": vendor_response,
            }), 502

        try:
            parsed = _parse_strict_json_object(completion_text)
        except ValueError as parse_err:
            return jsonify({
                "status": "error",
                "message": "Model output was not valid JSON object (primary call)",
                "details": str(parse_err),
                "raw_output": completion_text,
                "gemini_response": vendor_response,
            }), 502

        _normalize_primary_output(parsed)
        _ensure_ecommerce_keys(parsed)
        known = parsed.get("known_domains") or []
        logger.info("validation_ai: primary call ok known_domains=%s company_name=%s", len(known), parsed.get("company_name"))

        # Website tech call (third call) – four booleans: Google Ads, Meta Ads, LinkedIn Ads, Tag Manager
        parsed["google_ads"] = False
        parsed["meta_ads"] = False
        parsed["linkedin_ads"] = False
        parsed["tag_manager"] = False
        website_url = _get_website_url_for_calls(parsed, user_data)
        if not website_url:
            logger.warning("validation_ai: no website_url (known_domains empty and no domain in data); skipping tech and ecommerce calls")
        if website_url:
            logger.info("validation_ai: website_url=%s fetching HTML", website_url)
            try:
                html_snippet = _fetch_page_html(website_url)
                if html_snippet:
                    logger.info("validation_ai: HTML fetched len=%s", len(html_snippet))
                else:
                    logger.warning("validation_ai: HTML fetch failed or empty for %s", website_url)
                # Tag Manager: server-side detection from HTML (more reliable than AI)
                parsed["tag_manager"] = _detect_tag_manager_in_html(html_snippet) if html_snippet else False
                # Website/ecommerce platform: server-side detection from HTML (overrides AI after ecommerce merge)
                detected_website_platform = _detect_website_platform_in_html(html_snippet) if html_snippet else None
                detected_ecommerce_platform = _detect_ecommerce_platform_in_html(html_snippet) if html_snippet else None
                logger.info("validation_ai: platform detection from HTML website=%s ecommerce=%s tag_manager=%s", detected_website_platform, detected_ecommerce_platform, parsed["tag_manager"])
                system_instruction_tech, user_prompt_tech = _build_ad_agency_prompts_website_tech(website_url, html_snippet)
                completion_tech, _ = _call_gemini_generate_content(
                    api_key=gemini_key,
                    model=model,
                    prompt=user_prompt_tech,
                    system_instruction=system_instruction_tech,
                    read_timeout_seconds=read_timeout_seconds,
                    connect_timeout_seconds=connect_timeout_seconds,
                )
                if completion_tech and completion_tech.strip():
                    parsed_tech = _parse_strict_json_object(completion_tech)
                    parsed["google_ads"] = bool(parsed_tech.get("google_ads"))
                    parsed["meta_ads"] = bool(parsed_tech.get("meta_ads"))
                    parsed["linkedin_ads"] = bool(parsed_tech.get("linkedin_ads"))
                    logger.info("validation_ai: website_tech call ok google_ads=%s meta_ads=%s linkedin_ads=%s", parsed["google_ads"], parsed["meta_ads"], parsed["linkedin_ads"])
                else:
                    logger.warning("validation_ai: website_tech call empty or failed")
            except (requests.HTTPError, requests.RequestException, ValueError) as e:
                logger.warning("validation_ai: website_tech call error %s", e)

            # Ecommerce call (run when we have a URL; use same fetched HTML for reliable extraction)
            vendor_response_ecom = None
            try:
                system_instruction_text_ecom, user_prompt_text_ecom = _build_ad_agency_prompts_ecommerce(
                    user_data, website_url=website_url, html_snippet=html_snippet
                )
                completion_text_ecom, vendor_response_ecom = _call_gemini_generate_content(
                    api_key=gemini_key,
                    model=model,
                    prompt=user_prompt_text_ecom,
                    system_instruction=system_instruction_text_ecom,
                    read_timeout_seconds=read_timeout_seconds,
                    connect_timeout_seconds=connect_timeout_seconds,
                )
                if not completion_text_ecom or not completion_text_ecom.strip():
                    logger.warning("validation_ai: ecommerce call returned empty output")
                    if debug_mode:
                        parsed["_ecommerce_call_error"] = {
                            "message": "Gemini API returned empty output (ecommerce call)",
                            "details": "The API response contained no text content",
                            "gemini_response": vendor_response_ecom,
                        }
                else:
                    parsed_ecom = _parse_strict_json_object(completion_text_ecom)
                    parsed.update(parsed_ecom)
                    _normalize_ecommerce_output(parsed)
                    logger.info("validation_ai: ecommerce call ok catalogue_size=%s ecommerce_platform=%s", parsed.get("catalogue_size"), parsed.get("ecommerce_platform"))
            except requests.HTTPError as http_err:
                if debug_mode:
                    parsed["_ecommerce_call_error"] = {
                        "message": "Gemini API HTTP error (ecommerce call)",
                        "details": str(http_err),
                        "response": getattr(http_err, "response", None).text if getattr(http_err, "response", None) else None,
                    }
            except requests.RequestException as req_err:
                if debug_mode:
                    parsed["_ecommerce_call_error"] = {
                        "message": "Gemini API request failed (ecommerce call)",
                        "details": str(req_err),
                        "gemini_response": vendor_response_ecom,
                    }
            except ValueError as parse_err:
                if debug_mode:
                    parsed["_ecommerce_call_error"] = {
                        "message": "Model output was not valid JSON object (ecommerce call)",
                        "details": str(parse_err),
                        "raw_output": completion_text_ecom if "completion_text_ecom" in locals() else None,
                        "gemini_response": vendor_response_ecom,
                    }
            # Apply server-side platform detection when we have a match (even if ecommerce call failed)
            if detected_website_platform is not None:
                parsed["website_platform"] = detected_website_platform
            if detected_ecommerce_platform is not None:
                parsed["ecommerce_platform"] = detected_ecommerce_platform

        _infer_sales_type(parsed)
        logger.info("validation_ai: sales_type=%s returning output keys=%s", parsed.get("sales_type"), list(parsed.keys()))

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

    def _background_worker(record_id_value: Any, gemini_api_key: str, payload_data: Any, model_name: str, rt_seconds: int, ct_seconds: int, callback_url: str, customer_type_value: str) -> None:
        logger.info("validation_ai: background worker started Record_ID=%s", record_id_value)
        try:
            # Primary call - common fields for all customer types
            system_instruction_text, user_prompt_text = _build_ad_agency_prompts_primary(payload_data)
            completion_text, vendor_response = _call_gemini_generate_content(
                api_key=gemini_api_key,
                model=model_name,
                prompt=user_prompt_text,
                system_instruction=system_instruction_text,
                read_timeout_seconds=rt_seconds,
                connect_timeout_seconds=ct_seconds,
            )
            parsed = _parse_strict_json_object(completion_text)
            _normalize_primary_output(parsed)
            _ensure_ecommerce_keys(parsed)
            known = parsed.get("known_domains") or []
            logger.info("validation_ai: [%s] primary ok known_domains=%s company_name=%s", record_id_value, len(known), parsed.get("company_name"))

            # Website tech call (third call) – four booleans: Google Ads, Meta Ads, LinkedIn Ads, Tag Manager
            parsed["google_ads"] = False
            parsed["meta_ads"] = False
            parsed["linkedin_ads"] = False
            parsed["tag_manager"] = False
            website_url = _get_website_url_for_calls(parsed, payload_data)
            if not website_url:
                logger.warning("validation_ai: [%s] no website_url; skipping tech and ecommerce", record_id_value)
            if website_url:
                logger.info("validation_ai: [%s] website_url=%s", record_id_value, website_url)
                try:
                    html_snippet = _fetch_page_html(website_url)
                    # Tag Manager: server-side detection from HTML (more reliable than AI)
                    parsed["tag_manager"] = _detect_tag_manager_in_html(html_snippet) if html_snippet else False
                    # Website/ecommerce platform: server-side detection from HTML
                    detected_website_platform = _detect_website_platform_in_html(html_snippet) if html_snippet else None
                    detected_ecommerce_platform = _detect_ecommerce_platform_in_html(html_snippet) if html_snippet else None
                    system_instruction_tech, user_prompt_tech = _build_ad_agency_prompts_website_tech(website_url, html_snippet)
                    completion_tech, _ = _call_gemini_generate_content(
                        api_key=gemini_api_key,
                        model=model_name,
                        prompt=user_prompt_tech,
                        system_instruction=system_instruction_tech,
                        read_timeout_seconds=rt_seconds,
                        connect_timeout_seconds=ct_seconds,
                    )
                    if completion_tech and completion_tech.strip():
                        parsed_tech = _parse_strict_json_object(completion_tech)
                        parsed["google_ads"] = bool(parsed_tech.get("google_ads"))
                        parsed["meta_ads"] = bool(parsed_tech.get("meta_ads"))
                        parsed["linkedin_ads"] = bool(parsed_tech.get("linkedin_ads"))
                except (requests.HTTPError, requests.RequestException, ValueError):
                    pass  # keep defaults false

                # Ecommerce call (run when we have a URL; use same fetched HTML)
                try:
                    system_instruction_text_ecom, user_prompt_text_ecom = _build_ad_agency_prompts_ecommerce(
                        payload_data, website_url=website_url, html_snippet=html_snippet
                    )
                    completion_text_ecom, _ = _call_gemini_generate_content(
                        api_key=gemini_api_key,
                        model=model_name,
                        prompt=user_prompt_text_ecom,
                        system_instruction=system_instruction_text_ecom,
                        read_timeout_seconds=rt_seconds,
                        connect_timeout_seconds=ct_seconds,
                    )
                    if completion_text_ecom and completion_text_ecom.strip():
                        parsed_ecom = _parse_strict_json_object(completion_text_ecom)
                        parsed.update(parsed_ecom)
                        _normalize_ecommerce_output(parsed)
                except (requests.HTTPError, requests.RequestException, ValueError):
                    pass
                # Apply server-side platform detection when we have a match
                if detected_website_platform is not None:
                    parsed["website_platform"] = detected_website_platform
                if detected_ecommerce_platform is not None:
                    parsed["ecommerce_platform"] = detected_ecommerce_platform

            _infer_sales_type(parsed)
            logger.info("validation_ai: [%s] sales_type=%s posting to Zoho", record_id_value, parsed.get("sales_type"))

            result_body: Dict[str, Any] = {
                "status": "ok",
                "Record_ID": record_id_value,
                "model": model_name,
                "output": parsed,
            }
        except requests.HTTPError as http_err:
            logger.warning("validation_ai: [%s] HTTP error %s", record_id_value, http_err)
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
            logger.warning("validation_ai: [%s] parse error %s", record_id_value, parse_err)
            result_body = {
                "status": "error",
                "Record_ID": record_id_value,
                "message": "Model output was not valid JSON object",
                "details": str(parse_err),
            }
        except Exception as e:
            logger.exception("validation_ai: [%s] unexpected error %s", record_id_value, e)
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
        args=(record_id, gemini_key, user_data, model, read_timeout_seconds, connect_timeout_seconds, zoho_return_url, customer_type),
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
            candidate = candidates[0]
            # Check for blocking reasons
            finish_reason = candidate.get("finishReason", "")
            if finish_reason in ("SAFETY", "RECITATION", "OTHER"):
                blocking_reason = candidate.get("safetyRatings") or candidate.get("blockReason") or finish_reason
                raise requests.RequestException(f"Gemini API blocked response: {finish_reason}. Details: {blocking_reason}")
            
            parts = candidate.get("content", {}).get("parts", [])
            text_output = "".join(part.get("text", "") for part in parts if isinstance(part, dict))
            
            # If we have a candidate but no text, check for finish reason
            if not text_output and finish_reason:
                raise requests.RequestException(f"Gemini API returned empty content. Finish reason: {finish_reason}")
    except requests.RequestException:
        # Re-raise request exceptions (blocking reasons, etc.)
        raise
    except Exception:
        # Keep empty text_output on parsing issues; caller handles
        pass

    return text_output, data


def _get_website_url_for_calls(parsed_primary: Dict[str, Any], user_data: Any) -> Optional[str]:
    """Extract a single website URL from primary call result or user_data for use in website-tech and ecommerce calls."""
    domains = parsed_primary.get("known_domains") or []
    if domains and isinstance(domains[0], str) and domains[0].strip():
        d = domains[0].strip()
        if not d.startswith("http"):
            d = "https://" + d
        return d
    if isinstance(user_data, dict):
        for key in (
            "website", "domain", "url", "website_url", "domain_url",
            "Company_Website", "Web_Address", "Website", "Domain", "Company_Domain",
        ):
            v = user_data.get(key)
            if v and isinstance(v, str) and v.strip():
                v = v.strip()
                if not v.startswith("http"):
                    v = "https://" + v
                return v
    return None


# Max characters of fetched HTML to send to the model (keeps context within limits; head + early body usually has the tags)
_FETCHED_HTML_MAX_CHARS = 70000

# Common patterns for Google Tag Manager / gtag in page source (used for server-side detection)
_TAG_MANAGER_PATTERNS = (
    "googletagmanager.com/gtag/js",  # exact script src e.g. .../gtag/js?id=AW-...
    "googletagmanager.com/gtag",
    "googletagmanager.com/gtm.js",
    "googletagmanager.com/gtm.",
    "GTM-",
    "Google tag (gtag.js)",
    "Google Tag Manager",
)


def _detect_tag_manager_in_html(html: str) -> bool:
    """Return True if the HTML contains common Google Tag Manager / gtag.js indicators. Case-insensitive."""
    if not html or not isinstance(html, str):
        return False
    lower = html.lower()
    return any(
        p.lower() in lower
        for p in _TAG_MANAGER_PATTERNS
    )


# (pattern, platform_name) — first match wins. Order by specificity / reliability.
_WEBSITE_PLATFORM_PATTERNS: Tuple[Tuple[str, str], ...] = (
    ("wp-content", "WordPress"),
    ("wp-includes", "WordPress"),
    ("wp-json", "WordPress"),
    ("/wp-admin/", "WordPress"),
    ("wixstatic.com", "Wix"),
    ("wix.com", "Wix"),
    ("wixsite.com", "Wix"),
    ("squarespace.com", "Squarespace"),
    ("static1.squarespace.com", "Squarespace"),
    ("drupal.org", "Drupal"),
    ("sites/default/files", "Drupal"),
    ("/media/jui/", "Joomla"),
    ("joomla", "Joomla"),
    ("webflow.com", "Webflow"),
    ("cdn.webflow.com", "Webflow"),
    ("squarespace", "Squarespace"),
    ("drupal", "Drupal"),
)
_ECOMMERCE_PLATFORM_PATTERNS: Tuple[Tuple[str, str], ...] = (
    ("cdn.shopify.com", "Shopify"),
    ("myshopify.com", "Shopify"),
    ("shopify.theme", "Shopify"),
    ("shopify.com/shopify", "Shopify"),
    ("wp-content/plugins/woocommerce", "WooCommerce"),
    ("woocommerce", "WooCommerce"),
    ("wc-api", "WooCommerce"),
    ("mage/", "Magento"),
    ("magento", "Magento"),
    ("static/version", "Magento"),
    ("cdn.bigcommerce.com", "BigCommerce"),
    ("mybigcommerce.com", "BigCommerce"),
    ("bigcommerce.com", "BigCommerce"),
    ("demandware.net", "Salesforce Commerce Cloud"),
    ("commercecloud.salesforce.com", "Salesforce Commerce Cloud"),
    ("demandware", "Salesforce Commerce Cloud"),
)


def _detect_website_platform_in_html(html: str) -> Optional[str]:
    """Return detected website/CMS platform name from HTML (e.g. WordPress, Wix), or None. Case-insensitive."""
    if not html or not isinstance(html, str):
        return None
    lower = html.lower()
    for pattern, name in _WEBSITE_PLATFORM_PATTERNS:
        if pattern.lower() in lower:
            return name
    return None


def _detect_ecommerce_platform_in_html(html: str) -> Optional[str]:
    """Return detected ecommerce platform name from HTML (e.g. Shopify, WooCommerce), or None. Case-insensitive."""
    if not html or not isinstance(html, str):
        return None
    lower = html.lower()
    for pattern, name in _ECOMMERCE_PLATFORM_PATTERNS:
        if pattern.lower() in lower:
            return name
    return None


# Browser-like User-Agent so servers return the same HTML as when viewing source (avoids bot-only pages missing scripts)
_FETCH_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _fetch_page_html(url: str, timeout_seconds: int = 15) -> Optional[str]:
    """Fetch the raw HTML of a URL and return the first _FETCHED_HTML_MAX_CHARS characters, or None on failure."""
    if not url or not url.strip().startswith("http"):
        return None
    try:
        resp = requests.get(
            url,
            timeout=timeout_seconds,
            headers={
                "User-Agent": _FETCH_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            allow_redirects=True,
        )
        resp.raise_for_status()
        text = resp.text
        if not text:
            return None
        if len(text) > _FETCHED_HTML_MAX_CHARS:
            text = text[:_FETCHED_HTML_MAX_CHARS] + "\n\n[... HTML truncated ...]"
        return text
    except Exception:
        return None


def _build_ad_agency_prompts_website_tech(website_url: str, html_snippet: Optional[str] = None) -> Tuple[str, str]:
    """Build system instruction and user prompt for ad-platform detection (three booleans only).

    Tag Manager is detected server-side via _detect_tag_manager_in_html; it is not asked of the AI.
    Returns (system_instruction_text, user_prompt_text).
    Output schema: google_ads, meta_ads, linkedin_ads (all boolean).
    """
    system_text = (
        "You are a technical analyst. Your only job is to determine whether a website uses specific "
        "advertising technologies (Google Ads, Meta/Facebook Ads, LinkedIn Ads) by inspecting the page source. "
        "Search the provided content for the exact script URLs and code patterns listed. "
        "Return true if you find evidence; return false only when you have searched and found no such indicators. "
        "Do not guess from company type."
    )
    if html_snippet:
        user_text = (
            "Below is the raw HTML (and script content) of the website's page. Use it as the primary source to detect "
            "each ad technology. Search for the exact strings and patterns described.\n\n"
            "**URL (for reference):** " + website_url + "\n\n"
            "**Technologies to detect (set true only if you find evidence in the source):**\n"
            "1. **Google Ads** – Set true if you find ANY of: gtag or gtag/js, aw- (e.g. aw-123456789), googleadservices.com, "
            "doubleclick, googletagmanager.com/gtag with Google Ads, conversion_id, gclid, google.com/pagead/, dc.js, or 'Google Ads' in script/dataLayer.\n"
            "2. **Meta Ads (Facebook/Instagram)** – Set true if you find: fbq(, fbevents.js, facebook.net, connect.facebook.net, "
            "fbevents, Meta Pixel, or 'facebook' pixel in script/source.\n"
            "3. **LinkedIn Ads** – Set true if you find: li.lms-analytics, lintracker, linkedin.com/li.lms-analytics, "
            "snap.licdn.com, LinkedIn Insight Tag, or 'linkedin' tracking in script/source.\n\n"
            "**Page HTML/source (search below for the patterns above):**\n"
            "---BEGIN PAGE SOURCE---\n" + html_snippet + "\n---END PAGE SOURCE---\n\n"
            "Return a single JSON object with exactly these three boolean fields:\n"
            "{\n"
            "  \"google_ads\": boolean,\n"
            "  \"meta_ads\": boolean,\n"
            "  \"linkedin_ads\": boolean\n"
            "}\n\n"
            "Output only the JSON object, with no prose or explanation outside of it."
        )
    else:
        user_text = (
            "Visit this website URL and inspect the page source (HTML, script tags). "
            "For each technology below, set true if you find evidence; set false only if you find no indicators.\n\n"
            "**URL to inspect:** " + website_url + "\n\n"
            "**Technologies to detect:**\n"
            "1. **Google Ads** – gtag/aw-*, googleadservices.com, doubleclick, googletagmanager.com/gtag, conversion_id, gclid, dc.js.\n"
            "2. **Meta Ads** – fbq(, fbevents.js, facebook.net, connect.facebook.net, Meta Pixel.\n"
            "3. **LinkedIn Ads** – li.lms-analytics, lintracker, linkedin.com/li.lms-analytics, snap.licdn.com.\n\n"
            "Return a single JSON object with exactly these three boolean fields:\n"
            "{\n"
            "  \"google_ads\": boolean,\n"
            "  \"meta_ads\": boolean,\n"
            "  \"linkedin_ads\": boolean\n"
            "}\n\n"
            "Output only the JSON object, with no prose or explanation outside of it."
        )
    return system_text, user_text


def _build_ad_agency_prompts_primary(user_data: Any) -> Tuple[str, str]:
    """Build system instruction and user prompt for primary/common fields (all customer types).

    Accepts either a string or any JSON-serializable structure in user_data.
    Returns (system_instruction_text, user_prompt_text).
    This excludes ecommerce-specific fields like catalogue_size.
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
        "and compile accurate, marketing-relevant intelligence for client qualification and sales prospecting. "
        "You are researching these companies as POTENTIAL CLIENTS to pitch your agency's services TO them, not as existing clients. "
        "Treat the input as seed hints (e.g., company name, domain, notes). "
        "Prioritize official and authoritative sources. Do not fabricate or guess; if data is unavailable, leave fields empty or null. "
        "Normalize and deduplicate all outputs."
    )

    # Request structured JSON to make downstream usage easy
    user_text = (
        "Your task is to find comprehensive information about a company based on the provided seed hints. "
        "**IMPORTANT CONTEXT:** You are researching this company as a POTENTIAL CLIENT for an advertising agency. "
        "The agency wants to qualify and pitch their services TO this company. "
        "Fields like `suggested_pitch_points` and `digital_marketing_opportunities` should be written from the AGENCY'S perspective "
        "(what the agency would say to the company in a pitch), not from the company's own perspective. "
        "**NOTE:** Companies may be ecommerce (selling products) or service-based (offering services). "
        "The `products_services` field should contain products for ecommerce companies or services for service companies. "
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
        "3. **Gather other key information.** Use a series of broad and specific searches to fill out the remaining fields:\n"
        "   - **Industry**: Search for '[company name] industry' or '[company name] sector'. Use official company descriptions, LinkedIn, or industry classifications.\n"
        "   - **Competitors**: Search for '[company name] competitors' or '[company name] vs competitors'. Look for industry reports, analyst comparisons, or company mentions of competitors. Only include direct competitors in the same market segment.\n"
        "   - **Annual Revenue**: Search for '[company name] revenue', '[company name] annual report', '[company name] financials'. Prioritize official SEC filings, annual reports, or verified financial databases.\n"
        "   - **Size (Employees)**: Search for '[company name] employees', '[company name] workforce size', '[company name] company size'. Check LinkedIn company page, company website, or industry databases.\n"
        "   - **Locations**: Search for '[company name] locations', '[company name] offices', '[company name] headquarters'. Check company website, LinkedIn, or press releases.\n\n"
        "**CRITICAL: You MUST return a single JSON object that includes EVERY key listed in the schema below. Do not omit any key. Use null for missing single values and [] or {} for missing lists/objects. The response must have all of: company_name, known_domains, social_media (with linkedin, twitter, facebook, instagram, tiktok, youtube, other inside it), contact (with emails, phones, addresses), industry, size_employees, annual_revenue, locations, products_services, value_proposition, marketing_insights (with audience, tone_style, differentiators, competitors, digital_marketing_opportunities), suggested_pitch_points, missing_information, website_audit, confidence, sources.**\n\n"
        "Return a single JSON object with the following schema and rules:\n"
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
        "    \"other\": string[]\n"
        "  },\n"
        "  \"contact\": {\n"
        "    \"emails\": string[],\n"
        "    \"phones\": string[],\n"
        "    \"addresses\": string[]\n"
        "  },\n"
        "  \"industry\": string | null,\n"
        "  \"size_employees\": number | null,\n"
        "  \"annual_revenue\": number | null,\n"
        "  \"locations\": string[],\n"
        "  \"products_services\": string[],  // Products (for ecommerce) or services (for service companies) offered by the company\n"
        "  \"value_proposition\": string | null,  // The company's value proposition TO THEIR CUSTOMERS (what they offer/sell)\n"
        "  \"marketing_insights\": {\n"
        "    \"audience\": string | null,  // The company's target audience/customers\n"
        "    \"tone_style\": string | null,  // The company's current brand tone and style\n"
        "    \"differentiators\": string[],  // What differentiates this company from competitors\n"
        "    \"competitors\": string[],\n"
        "    \"digital_marketing_opportunities\": string[]  // Opportunities the agency could help with (e.g., 'SEO improvements needed', 'Social media engagement is low')\n"
        "  },\n"
        "  \"suggested_pitch_points\": string[],  // Points the agency should use when pitching services TO this company (e.g., 'Their social media presence is weak', 'Website lacks SEO optimization')\n"
        "  \"missing_information\": string[],\n"
        "  \"website_audit\": string[],  // Issues or opportunities found on the company's website from the agency's perspective (e.g., 'Missing meta descriptions', 'No mobile optimization')\n"
        "  \"confidence\": number,  // 0.0 - 1.0\n"
        "  \"sources\": string[]   // distinct URLs that substantiate the data\n"
        "}\n\n"
        "Rules:\n"
        "- **PERSPECTIVE:** Remember you are an agency researching a POTENTIAL CLIENT. Fields like `suggested_pitch_points`, `digital_marketing_opportunities`, and `website_audit` should be written from the AGENCY'S perspective (what you would tell them in a pitch), not from the company's own perspective.\n"
        "- **ACCURACY:** Do not invent, fabricate, or guess any information. If you cannot find reliable, verifiable information, use `null` for single fields or empty arrays `[]` for list fields.\n"
        "- **SOURCES:** The `sources` array must contain ONLY actual URLs that you accessed during your research. Do not include URLs you did not visit. Each source should be a distinct URL that substantiates the data you found.\n"
        "- **CONFIDENCE:** Set `confidence` to reflect how much of the data was found from authoritative sources vs inferred. Use 0.9+ only if most data came from official sources (company website, SEC filings, LinkedIn). Use 0.7-0.8 if some data was inferred or from secondary sources. Use 0.5-0.6 if significant data is missing or uncertain.\n"
        "- **COMPETITORS:** Only include direct competitors in the same market segment. Do not include suppliers, customers, or companies in adjacent markets.\n"
        "- **ANNUAL REVENUE:** Only include if found in official sources (SEC filings, annual reports, verified financial databases). If only estimates are available, use `null`.\n"
        "- Do not invent URLs, emails, or names.\n"
        "- Normalize all URLs to include `https://`.\n"
        "- Deduplicate all lists (e.g., `known_domains`, `sources`).\n"
        "- **All phone numbers must be formatted with the correct country calling code, for example, `+1 (555) 555-5555` for US/Canada numbers.**\n"
        "- If multiple candidates exist for a field, pick the most authoritative or include the top 3.\n"
        "- **SCHEMA COMPLETENESS:** Your response must contain every key in the schema. Do not return a partial object. Missing data must be null or empty arrays/objects, not omitted keys.\n"
        "- Output only the JSON object, with no prose or explanation outside of it.\n\n"
        f"Seed hints:\n{data_block}"
    )

    return system_text, user_text


def _build_ad_agency_prompts_ecommerce(
    user_data: Any,
    website_url: Optional[str] = None,
    html_snippet: Optional[str] = None,
) -> Tuple[str, str]:
    """Build system instruction and user prompt for ecommerce-specific fields only.

    If html_snippet is provided, the model will use the page source as the primary source for extraction.
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
        "You are an elite advertising agency assistant. Your job is to extract ecommerce-specific information "
        "from the provided data (page source and/or seed hints). Use the page HTML when provided as the primary "
        "source: look for platform in script/footer, payment/shipping in page text or footer, categories in nav. "
        "Do not fabricate; use null or [] when data is unavailable."
    )

    if html_snippet and website_url:
        intro = (
            "**Primary source:** The raw HTML of the company's website is provided below. Use it to extract ecommerce data. "
            "Search the HTML for: ecommerce platform (Shopify, WooCommerce, etc. in script/footer), website platform (WordPress, Wix in meta/source), "
            "payment methods (Visa, PayPal, etc. in footer/checkout), shipping methods (text or links), top-level product categories (nav menus, links). "
            "Also use Google Search with the seed hints for catalogue size, mobile app, subscription, international shipping if not visible in the HTML.\n\n"
            "**URL:** " + website_url + "\n\n"
            "**Page HTML/source (extract from this):**\n"
            "---BEGIN PAGE SOURCE---\n" + html_snippet + "\n---END PAGE SOURCE---\n\n"
            "**Seed hints (for search when needed):**\n" + data_block + "\n\n"
        )
    else:
        intro = (
            "Your task is to find ecommerce-specific information about a company based on the provided seed hints. "
            "Use Google Search and website exploration."
            + ("\n\n**Company website (inspect this URL):** " + website_url if website_url else "")
            + "\n\n**SEARCH STRATEGY:** Use multiple targeted Google Search queries and website exploration:\n\n"
        )

    # Request structured JSON focused only on ecommerce-specific fields
    user_text = (
        intro
        +
        "**1. Catalog Size (catalogue_size):**\n"
        "   - Search for '[company name] SKU count' or '[company name] number of SKUs'\n"
        "   - Search for '[company name] product catalog size' or '[company name] total products'\n"
        "   - Search for '[company name] annual report' or '[company name] investor relations' and look for catalog/product count mentions\n"
        "   - For B2B distributors/industrial companies, search for '[company name] parts catalog' or '[company name] inventory size'\n"
        "   - Explore the company's website directly - look for product sitemaps, category pages, or 'shop all' pages that might show product counts\n"
        "   - Check press releases, news articles, or industry publications\n\n"
        "**2. Product Categories Count (product_categories_count):**\n"
        "   **IMPORTANT: Count ALL top-level product categories. Be thorough and accurate.**\n"
        "   **Where to find categories:**\n"
        "   - Look for 'Shop by Category', 'Categories', 'Products', 'Shop', or similar navigation menus\n"
        "   - Check the main navigation bar, sidebar menus, or dropdown menus\n"
        "   - Look for category listings on the homepage or product pages\n"
        "   - Check footer links that might list product categories\n"
        "   - Look for category filters on product listing pages\n"
        "   **What counts as a category:**\n"
        "   - Each top-level category/department in the navigation (e.g., 'Accessories', 'Bulk Bins', 'Clamshells')\n"
        "   - Do NOT count subcategories - only count the main/top-level categories\n"
        "   - Do NOT count non-product pages (e.g., 'About', 'Contact', 'Blog')\n"
        "   - Count all categories shown in menus, even if some are less prominent\n"
        "   **Verification:**\n"
        "   - Count categories in multiple places on the site to ensure accuracy\n"
        "   - If you see a 'Shop by Category' menu, count every item listed there\n"
        "   - If categories are split across different menus, count all of them\n"
        "   - Be precise - if you see 10 categories, return 10, not an estimate\n\n"
        "**3. Ecommerce Platform (ecommerce_platform):**\n"
        "   **CRITICAL: Only return a platform name if you find DEFINITIVE, VERIFIABLE evidence. Do NOT guess based on appearance or assumptions.**\n"
        "   **REQUIRED EVIDENCE - You must find at least ONE of these definitive indicators:**\n"
        "   - **Shopify**: Look for '*.myshopify.com' in the URL, 'Powered by Shopify' in footer/source, 'cdn.shopify.com' in page source, or 'Shopify.theme' in JavaScript\n"
        "   - **Magento**: Look for 'Magento' in page source, '/static/version' in URLs, 'mage/' in JavaScript paths, or 'Magento' in meta tags\n"
        "   - **WooCommerce**: Look for 'WooCommerce' in page source, WordPress indicators (wp-content, wp-includes), or 'woocommerce' in JavaScript/CSS paths\n"
        "   - **BigCommerce**: Look for '*.mybigcommerce.com' in URL, 'BigCommerce' in page source, or 'cdn.bigcommerce.com' in resources\n"
        "   - **Salesforce Commerce Cloud**: Look for 'demandware.net' or 'commercecloud.salesforce.com' in URLs/resources, or 'Demandware' in source\n"
        "   - **Custom-built**: Only if you find explicit statements that it's custom-built, or if you cannot identify any known platform indicators\n"
        "   **Search methods:**\n"
        "   - View the website's HTML source code (look for platform-specific JavaScript, CSS, or meta tags)\n"
        "   - Check the website footer for 'Powered by' notices\n"
        "   - Examine URLs for platform-specific domains or paths\n"
        "   - Search for '[company name] ecommerce platform' or '[company name] shopping cart software' in news/articles\n"
        "   - Check job postings that explicitly mention the platform\n"
        "   **If you cannot find definitive evidence after thorough searching, return `null`. Do NOT guess or infer based on website appearance, design patterns, or payment methods alone.**\n\n"
        "**4. Website Platform (website_platform):**\n"
        "   - The general CMS or site builder the website is built on, if identifiable (e.g. WordPress, Wix, Squarespace, custom). This is distinct from ecommerce_platform (the shopping cart). Check page source, footer, or meta generator tags.\n\n"
        "**5. Shipping Methods (shipping_methods):**\n"
        "   - Check the company's website shipping/FAQ page\n"
        "   - Look for shipping options during checkout (if accessible)\n"
        "   - Search for '[company name] shipping options' or '[company name] delivery methods'\n\n"
        "**6. Payment Methods (payment_methods):**\n"
        "   - Check the company's website checkout page or payment information\n"
        "   - Look for payment logos/icons (Visa, Mastercard, PayPal, Apple Pay, etc.)\n"
        "   - Search for '[company name] payment methods' or '[company name] accepted payments'\n\n"
        "**7. Mobile App (has_mobile_app):**\n"
        "   - Search for '[company name] mobile app' or '[company name] iOS app' or '[company name] Android app'\n"
        "   - Check App Store or Google Play Store listings\n"
        "   - Look for app download links on the company website\n\n"
        "**8. Subscription Model (has_subscription_model):**\n"
        "   - Look for subscription options on the website\n"
        "   - Search for '[company name] subscription' or '[company name] recurring orders'\n"
        "   - Check if they offer subscription boxes, recurring deliveries, or membership programs\n\n"
        "**9. International Shipping (international_shipping):**\n"
        "   - Check shipping information on the website\n"
        "   - Look for country selection or international shipping options\n"
        "   - Search for '[company name] international shipping' or '[company name] ships to'\n\n"
        "**NOTE:** For B2B companies (distributors, industrial suppliers), catalog size may be referred to as:\n"
        "- SKU count (Stock Keeping Units)\n"
        "- Product lines or product families\n"
        "- Catalog items or parts\n"
        "- Inventory size\n"
        "All of these are valid measures of catalog size.\n\n"
        "Return a single JSON object with the following schema:\n"
        "{\n"
        "  \"catalogue_size\": number | null,  // The approximate number of products/SKUs (integer)\n"
        "  \"product_categories_count\": number | null,  // The number of distinct top-level product categories (integer)\n"
        "  \"ecommerce_platform\": string | null,  // Shopping cart platform (e.g. 'Shopify', 'Magento', 'WooCommerce', 'BigCommerce', 'custom-built')\n"
        "  \"website_platform\": string | null,  // General CMS/site builder if identifiable (e.g. 'WordPress', 'Wix', 'custom')\n"
        "  \"shipping_methods\": string[],  // Available shipping options\n"
        "  \"payment_methods\": string[],  // Accepted payment methods\n"
        "  \"has_mobile_app\": boolean | null,\n"
        "  \"has_subscription_model\": boolean | null,\n"
        "  \"international_shipping\": boolean | null\n"
        "}\n\n"
        "Rules:\n"
        "- Do not invent or guess information. If you cannot find reliable information after searching, return `null` for single fields or empty arrays `[]` for list fields.\n"
        "- For catalogue_size: If you can only find a range, use the midpoint or lower bound. For very large catalogs (10,000+), round to the nearest thousand if exact number isn't available.\n"
        "- For product_categories_count: **ACCURACY REQUIRED** - Count ALL top-level categories you can see on the website. Look for 'Shop by Category' menus, navigation bars, and category listings. Count every distinct top-level category - do not estimate or round. If you see 10 categories, return 10. Only count product categories, not non-product pages (About, Contact, etc.).\n"
        "- For ecommerce_platform: **STRICT RULE** - Only return a platform name if you find definitive technical evidence (platform-specific URLs, source code indicators, 'Powered by' text, or explicit company statements). Do NOT guess based on website appearance, design patterns, payment methods, or assumptions. If you cannot find verifiable evidence, return `null`.\n"
        "- For boolean fields: Return `true` only if you find clear evidence. Return `false` if you find evidence they don't have it. Return `null` if you cannot determine.\n"
        "- Output only the JSON object, with no prose or explanation outside of it.\n\n"
        f"Seed hints:\n{data_block}"
    )

    return system_text, user_text


def _normalize_primary_output(parsed: Dict[str, Any]) -> None:
    """Ensure primary-call parsed has full schema. Fix malformed responses (e.g. social_media keys at top level) and fill missing keys."""
    # If model returned social_media fields at top level instead of under "social_media", fix it
    social_keys = ("linkedin", "twitter", "facebook", "instagram", "tiktok", "youtube", "other")
    if "social_media" not in parsed and any(k in parsed for k in social_keys):
        parsed["social_media"] = {
            "linkedin": parsed.pop("linkedin", None),
            "twitter": parsed.pop("twitter", None),
            "facebook": parsed.pop("facebook", None),
            "instagram": parsed.pop("instagram", None),
            "tiktok": parsed.pop("tiktok", None),
            "youtube": parsed.pop("youtube", None),
            "other": parsed.pop("other", []) or [],
        }
    # Ensure social_media exists and has all keys
    sm = parsed.get("social_media")
    if not isinstance(sm, dict):
        parsed["social_media"] = {}
    sm = parsed["social_media"]
    for k in social_keys:
        if k not in sm:
            sm[k] = [] if k == "other" else None
    # Required top-level keys and defaults (so Zoho always gets full structure)
    defaults: Dict[str, Any] = {
        "company_name": None,
        "known_domains": [],
        "contact": {"emails": [], "phones": [], "addresses": []},
        "industry": None,
        "size_employees": None,
        "annual_revenue": None,
        "locations": [],
        "products_services": [],
        "value_proposition": None,
        "marketing_insights": {
            "audience": None,
            "tone_style": None,
            "differentiators": [],
            "competitors": [],
            "digital_marketing_opportunities": [],
        },
        "suggested_pitch_points": [],
        "missing_information": [],
        "website_audit": [],
        "confidence": 0.0,
        "sources": [],
    }
    for key, default in defaults.items():
        if key not in parsed:
            parsed[key] = default
        elif key == "contact" and isinstance(parsed[key], dict):
            for sub in ("emails", "phones", "addresses"):
                if sub not in parsed[key]:
                    parsed[key][sub] = []
        elif key == "marketing_insights" and isinstance(parsed[key], dict):
            for sub in ("audience", "tone_style", "differentiators", "competitors", "digital_marketing_opportunities"):
                if sub not in parsed[key]:
                    parsed[key][sub] = [] if sub in ("differentiators", "competitors", "digital_marketing_opportunities") else None


def _ensure_ecommerce_keys(parsed: Dict[str, Any]) -> None:
    """Ensure all ecommerce-related keys exist in parsed with null/empty defaults so Zoho always gets the same schema."""
    defaults: Dict[str, Any] = {
        "catalogue_size": None,
        "product_categories_count": None,
        "categories_count": None,
        "ecommerce_platform": None,
        "website_platform": None,
        "shipping_methods": [],
        "payment_methods": [],
        "payment_methods_str": "",
        "shipping_methods_str": "",
        "has_mobile_app": None,
        "has_subscription_model": None,
        "international_shipping": None,
        "sales_type": None,
    }
    for key, default in defaults.items():
        if key not in parsed:
            parsed[key] = default


def _infer_sales_type(parsed: Dict[str, Any]) -> None:
    """Set parsed['sales_type'] to 'E-Commerce', 'Lead Gen', or 'Other' from validation data and website technology."""
    # E-Commerce: ecommerce platform detected, or catalogue/categories present
    ecom_platform = (parsed.get("ecommerce_platform") or "").strip() if isinstance(parsed.get("ecommerce_platform"), str) else None
    catalogue_size = parsed.get("catalogue_size")
    categories_count = parsed.get("product_categories_count") or parsed.get("categories_count")
    if ecom_platform:
        parsed["sales_type"] = "E-Commerce"
        return
    if catalogue_size is not None and (isinstance(catalogue_size, (int, float)) and catalogue_size > 0):
        parsed["sales_type"] = "E-Commerce"
        return
    if categories_count is not None and (isinstance(categories_count, (int, float)) and categories_count > 0):
        parsed["sales_type"] = "E-Commerce"
        return
    # Lead Gen: we have website/domain but no ecommerce signals (marketing/service site)
    has_site = bool(parsed.get("known_domains")) or bool((parsed.get("website_platform") or "").strip())
    if has_site:
        parsed["sales_type"] = "Lead Gen"
        return
    parsed["sales_type"] = "Other"


def _normalize_ecommerce_output(parsed: Dict[str, Any]) -> None:
    """Normalize ecommerce-related keys in parsed for Zoho: string forms, categories_count alias, catalogue_size as int."""
    # Zoho-friendly string forms for payment/shipping (comma-separated)
    pm = parsed.get("payment_methods")
    parsed["payment_methods_str"] = ", ".join(pm) if isinstance(pm, list) and pm else (pm if isinstance(pm, str) else "")
    sm = parsed.get("shipping_methods")
    parsed["shipping_methods_str"] = ", ".join(sm) if isinstance(sm, list) and sm else (sm if isinstance(sm, str) else "")
    # Alias for Zoho 'Categories Count (int)'
    pc = parsed.get("product_categories_count")
    parsed["categories_count"] = int(pc) if pc is not None and isinstance(pc, (int, float)) else None
    # Ensure catalogue_size is int when present
    cs = parsed.get("catalogue_size")
    if cs is not None and isinstance(cs, (int, float)):
        parsed["catalogue_size"] = int(cs)


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


