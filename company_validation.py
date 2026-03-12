"""
Company validation: parse website HTML for tech stack, ecommerce stack, and ad/tag
checkboxes; run AI for ecommerce-specific fields (catalogue, subscription, shipping, etc.).
Fully self-contained — no dependency on validation_ai.
validation_ai owns Marketing Insights / Pitch Data; this module owns hard boolean/info returns.
"""
from flask import Blueprint, request, jsonify
from typing import Any, Dict, Optional, Tuple
import re
import json
import logging
import time
import os
import requests
import threading

company_validation_bp = Blueprint("company_validation", __name__)

# Zoho Creator webhook: we POST company_validation result here after processing (same pattern as validation_ai).
ZOHO_COMPANY_VALIDATION_RETURN_URL = "https://www.zohoapis.com/creator/custom/paintedrobot/NSM_Company_Validate_Return?publickey=5Bmx5hxpCDWfO4H0TDVFNJA6P"

logger = logging.getLogger("company_validation")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    logger.addHandler(_h)


# ----- URL from payload -----
def _get_website_url_from_payload(payload: Dict[str, Any]) -> Optional[str]:
    """Extract a single website URL from payload (website_url or data keys)."""
    url = payload.get("website_url") or payload.get("url")
    if url and isinstance(url, str) and url.strip():
        u = url.strip()
        if not u.startswith("http"):
            u = "https://" + u
        return u
    user_data = payload.get("data")
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


# ----- HTML fetch -----
# Use full response for GTM/ads detection (scripts can be anywhere). For platform detection use only the head.
_FETCHED_HTML_MAX_CHARS = 5_000_000
# Only the first N chars are used for website_platform and ecommerce_platform (avoids matching third-party script URLs).
_HTML_HEAD_FOR_PLATFORM_CHARS = 120_000
_FETCH_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _fetch_page_html(url: str, timeout_seconds: int = 15) -> Optional[str]:
    """Fetch the raw HTML of a URL; return up to _FETCHED_HTML_MAX_CHARS (full page for detection)."""
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


# ----- Ad/tag and platform detection (patterns + functions) -----
# Explicit GTM/gtag script and container indicators (order: most specific first).
_TAG_MANAGER_PATTERNS = (
    "googletagmanager.com/gtag/js",   # gtag.js script src (e.g. id=G-... or id=AW-...)
    "googletagmanager.com/gtm.js",    # GTM container script
    "googletagmanager.com/gtm.",      # GTM container (e.g. gtm.js?id=GTM-XXX)
    "googletagmanager.com/ns.html",   # GTM noscript iframe
    "googletagmanager.com/gtag",      # gtag config/call
    "googletagmanager.com",           # any GTM domain reference
    "GTM-",                           # container ID in script
    "Google tag (gtag.js)",
    "Google Tag Manager",
)

_GOOGLE_ADS_PATTERNS = (
    "googleadservices.com",
    "doubleclick.net",
    "dc.js",
    "google.com/pagead",
)
_GOOGLE_ADS_AW_REGEX = re.compile(r"aw-\d+", re.IGNORECASE)

_META_ADS_PATTERNS = (
    "fbevents.js",
    "connect.facebook.net",
    "facebook.net/en_us/fbevents",
    "fbq(",
    "fbq('init'",
    "meta pixel",
)

_LINKEDIN_ADS_PATTERNS = (
    "li.lms-analytics",
    "lintracker",
    "linkedin.com/li.lms-analytics",
    "snap.licdn.com",
    "linkedin insight tag",
)

# Explicit self-identification first (meta generator, "This is X", "Powered by X") — checked only in document head.
_WEBSITE_PLATFORM_SELF_ID: Tuple[Tuple[str, str], ...] = (
    ("this is squarespace", "Squarespace"),
    ("powered by squarespace", "Squarespace"),
    ("generator.*squarespace", "Squarespace"),
    ("powered by wordpress", "WordPress"),
    ("generator.*wordpress", "WordPress"),
    ("powered by wix", "Wix"),
    ("powered by webflow", "Webflow"),
)
# CMS/site-builder markers (only searched in document head to avoid third-party script URL false positives).
_WEBSITE_PLATFORM_PATTERNS: Tuple[Tuple[str, str], ...] = (
    ("/wp-content/", "WordPress"),
    ("/wp-includes/", "WordPress"),
    ("/wp-json/", "WordPress"),
    ("/wp-admin/", "WordPress"),
    ("wixstatic.com", "Wix"),
    ("parastorage.com", "Wix"),
    ("wixsite.com", "Wix"),
    ("squarespace.com", "Squarespace"),
    ("static1.squarespace.com", "Squarespace"),
    ("static.squarespace.com", "Squarespace"),
    ("drupal.org", "Drupal"),
    ("sites/default/files/", "Drupal"),
    ("/sites/default/", "Drupal"),
    ("/media/jui/", "Joomla"),
    ("webflow.com", "Webflow"),
    ("cdn.webflow.com", "Webflow"),
)

# Ecommerce self-identification (only in document head).
_ECOMMERCE_PLATFORM_SELF_ID: Tuple[Tuple[str, str], ...] = (
    ("powered by shopify", "Shopify"),
)
# Ecommerce platform markers (only in document head to avoid third-party script false positives).
_ECOMMERCE_PLATFORM_PATTERNS: Tuple[Tuple[str, str], ...] = (
    ("cdn.shopify.com", "Shopify"),
    ("myshopify.com", "Shopify"),
    ("shopify.theme", "Shopify"),
    ("shopify.com/shopify", "Shopify"),
    ("/plugins/woocommerce/", "WooCommerce"),
    ("wp-content/plugins/woocommerce", "WooCommerce"),
    ("/woocommerce/", "WooCommerce"),
    ("wc-api", "WooCommerce"),
    ("/static/frontend/Magento/", "Magento"),
    ("magento/theme", "Magento"),
    ("Magento_", "Magento"),
    ("cdn.bigcommerce.com", "BigCommerce"),
    ("mybigcommerce.com", "BigCommerce"),
    ("bigcommerce.com", "BigCommerce"),
    ("demandware.net", "Salesforce Commerce Cloud"),
    ("commercecloud.salesforce.com", "Salesforce Commerce Cloud"),
)


def _detect_tag_manager_in_html(html: str) -> bool:
    if not html or not isinstance(html, str):
        return False
    lower = html.lower()
    return any(p.lower() in lower for p in _TAG_MANAGER_PATTERNS)


def _detect_google_ads_in_html(html: str) -> bool:
    if not html or not isinstance(html, str):
        return False
    lower = html.lower()
    if any(p.lower() in lower for p in _GOOGLE_ADS_PATTERNS):
        return True
    if _GOOGLE_ADS_AW_REGEX.search(html):
        return True
    return False


def _detect_meta_ads_in_html(html: str) -> bool:
    if not html or not isinstance(html, str):
        return False
    lower = html.lower()
    return any(p.lower() in lower for p in _META_ADS_PATTERNS)


def _detect_linkedin_ads_in_html(html: str) -> bool:
    if not html or not isinstance(html, str):
        return False
    lower = html.lower()
    return any(p.lower() in lower for p in _LINKEDIN_ADS_PATTERNS)


def _platform_head_only(html: str) -> str:
    """Return only the document head / start of body for platform detection (avoids third-party script URLs)."""
    if not html or not isinstance(html, str):
        return ""
    return html[:_HTML_HEAD_FOR_PLATFORM_CHARS] if len(html) > _HTML_HEAD_FOR_PLATFORM_CHARS else html


def _detect_website_platform_in_html(html: str) -> Optional[str]:
    """Detect CMS/site builder from HTML. Uses only the first _HTML_HEAD_FOR_PLATFORM_CHARS to avoid false positives from third-party script URLs."""
    head = _platform_head_only(html)
    if not head:
        return None
    lower = head.lower()
    # 1) Explicit self-identification first (e.g. "This is Squarespace", generator meta).
    for pattern, name in _WEBSITE_PLATFORM_SELF_ID:
        if ".*" in pattern:
            if re.search(pattern, lower):
                return name
        elif pattern.lower() in lower:
            return name
    # 2) Structural markers (same-origin paths / same-origin script URLs live in head).
    for pattern, name in _WEBSITE_PLATFORM_PATTERNS:
        if pattern.lower() in lower:
            return name
    return None


def _detect_ecommerce_platform_in_html(html: str) -> Optional[str]:
    """Detect ecommerce platform from HTML. Uses only the first _HTML_HEAD_FOR_PLATFORM_CHARS to avoid false positives from third-party embeds."""
    head = _platform_head_only(html)
    if not head:
        return None
    lower = head.lower()
    for pattern, name in _ECOMMERCE_PLATFORM_SELF_ID:
        if pattern.lower() in lower:
            return name
    for pattern, name in _ECOMMERCE_PLATFORM_PATTERNS:
        if pattern.lower() in lower:
            return name
    return None


# ----- Gemini API and JSON parsing (used only for ecommerce AI) -----
def _call_gemini_generate_content(
    *,
    api_key: str,
    model: str,
    prompt: str,
    system_instruction: Optional[str] = None,
    read_timeout_seconds: int = 300,
    connect_timeout_seconds: int = 60,
) -> Tuple[str, Dict[str, Any]]:
    if not api_key:
        raise requests.RequestException("Missing Gemini API key")
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    headers = {
        "x-goog-api-key": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload: Dict[str, Any] = {
        "contents": [{"parts": [{"text": prompt}]}],
        "tools": [{"google_search": {}}],
    }
    if system_instruction:
        payload["systemInstruction"] = {"parts": [{"text": system_instruction}]}

    max_attempts = 2
    attempt = 0
    last_exc: Optional[Exception] = None
    while attempt < max_attempts:
        try:
            response = requests.post(
                endpoint,
                headers=headers,
                json=payload,
                timeout=(connect_timeout_seconds, read_timeout_seconds),
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

    text_output = ""
    try:
        candidates = data.get("candidates") or []
        if candidates:
            candidate = candidates[0]
            finish_reason = candidate.get("finishReason", "")
            if finish_reason in ("SAFETY", "RECITATION", "OTHER"):
                blocking_reason = candidate.get("safetyRatings") or candidate.get("blockReason") or finish_reason
                raise requests.RequestException(f"Gemini API blocked response: {finish_reason}. Details: {blocking_reason}")
            parts = candidate.get("content", {}).get("parts", [])
            text_output = "".join(part.get("text", "") for part in parts if isinstance(part, dict))
            if not text_output and finish_reason:
                raise requests.RequestException(f"Gemini API returned empty content. Finish reason: {finish_reason}")
    except requests.RequestException:
        raise
    except Exception:
        pass
    return text_output, data


def _parse_strict_json_object(text: str) -> Dict[str, Any]:
    if not isinstance(text, str) or not text.strip():
        raise ValueError("Empty model output")
    cleaned = text.strip()

    def _try_parse_object(candidate: str) -> Optional[Dict[str, Any]]:
        try:
            parsed_candidate = json.loads(candidate)
            return parsed_candidate if isinstance(parsed_candidate, dict) else None
        except json.JSONDecodeError:
            return None

    parsed = _try_parse_object(cleaned)
    if parsed is not None:
        return parsed

    if "```" in cleaned:
        parts = cleaned.split("```")
        for idx in range(1, len(parts), 2):
            fenced_block = parts[idx]
            fenced_lines = fenced_block.splitlines()
            if fenced_lines and fenced_lines[0].strip().lower() in ("json", "js", "javascript"):
                fenced_lines = fenced_lines[1:]
            candidate = "\n".join(fenced_lines).strip()
            parsed = _try_parse_object(candidate)
            if parsed is not None:
                return parsed

    start = cleaned.find("{")
    while start != -1:
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
            if ch == '"':
                in_string = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    parsed = _try_parse_object(cleaned[start : i + 1])
                    if parsed is not None:
                        return parsed
                    break
        start = cleaned.find("{", start + 1)
    raise ValueError("JSON decode error: Could not extract a valid JSON object from model output")


# ----- Ecommerce prompt builder and normalizers -----
def _build_ad_agency_prompts_ecommerce(
    user_data: Any,
    website_url: Optional[str] = None,
) -> Tuple[str, str]:
    if isinstance(user_data, str):
        data_block = user_data
    else:
        try:
            data_block = json.dumps(user_data, ensure_ascii=False, indent=2)
        except Exception:
            raise ValueError("Unsupported 'data' format. Provide a string or JSON-serializable object.")

    system_text = (
        "You are an elite advertising agency assistant. Your job is to find ecommerce-specific information "
        "about a company by visiting their website and using Google Search. Use the company URL and seed hints "
        "to explore the site and run targeted searches. Do not fabricate; use null or [] when data is unavailable."
    )
    intro = (
        "Visit the company's website and use Google Search to find the following ecommerce information. "
        "Use the URL below as the primary source; run targeted searches (e.g. '[company] shipping options', "
        "'[company] payment methods') when needed.\n\n"
        + ("**Company website (visit and inspect):** " + website_url + "\n\n" if website_url else "")
        + "**Seed hints (company name, domain, etc.):**\n" + data_block + "\n\n"
        + "**SEARCH STRATEGY:** Use multiple targeted Google Search queries and direct website inspection:\n\n"
    )
    user_text = (
        intro
        + "**1. Catalog Size (catalogue_size):**\n"
        "   - Search for '[company name] SKU count' or '[company name] number of SKUs'\n"
        "   - Search for '[company name] product catalog size' or '[company name] total products'\n"
        "   - Explore the company's website - product sitemaps, category pages, 'shop all' pages\n\n"
        "**2. Product Categories Count (product_categories_count):** Count ALL top-level product categories (menus, nav, filters). Do NOT count subcategories or non-product pages.\n\n"
        "**3. Shipping Methods (shipping_methods):** From shipping/FAQ pages or checkout.\n\n"
        "**4. Payment Methods (payment_methods):** From checkout or payment info.\n\n"
        "**5. Mobile App (has_mobile_app):** true/false/null from app store or site.\n\n"
        "**6. Subscription Model (has_subscription_model):** subscription/recurring options.\n\n"
        "**7. International Shipping (international_shipping):** true/false/null.\n\n"
        "Do NOT include website_platform or ecommerce_platform — those are determined from the page HTML only.\n\n"
        "Return a single JSON object with: catalogue_size (number|null), product_categories_count (number|null), "
        "shipping_methods (string[]), payment_methods (string[]), has_mobile_app (boolean|null), "
        "has_subscription_model (boolean|null), international_shipping (boolean|null). Output only the JSON object, no prose.\n\n"
        f"Seed hints:\n{data_block}"
    )
    return system_text, user_text


def _ensure_ecommerce_keys(parsed: Dict[str, Any]) -> None:
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
    }
    for key, default in defaults.items():
        if key not in parsed:
            parsed[key] = default


def _normalize_ecommerce_output(parsed: Dict[str, Any]) -> None:
    pm = parsed.get("payment_methods")
    parsed["payment_methods_str"] = ", ".join(pm) if isinstance(pm, list) and pm else (pm if isinstance(pm, str) else "")
    sm = parsed.get("shipping_methods")
    parsed["shipping_methods_str"] = ", ".join(sm) if isinstance(sm, list) and sm else (sm if isinstance(sm, str) else "")
    pc = parsed.get("product_categories_count")
    parsed["categories_count"] = int(pc) if pc is not None and isinstance(pc, (int, float)) else None
    cs = parsed.get("catalogue_size")
    if cs is not None and isinstance(cs, (int, float)):
        parsed["catalogue_size"] = int(cs)


def _run_company_validation(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run full company validation (HTML fetch, tech/checkboxes detection, optional ecommerce AI).
    Returns the result dict. Used by the background worker; does not POST to Zoho.
    """
    website_url = _get_website_url_from_payload(payload)
    if not website_url:
        return {
            "status": "error",
            "message": "Missing website URL. Provide 'website_url' or 'url' in payload, or 'data' with website/domain keys.",
            "website_url": None,
            "tech_stack": {"website_platform": None, "ecommerce_platform": None},
            "checkboxes": {"tag_manager": False, "google_ads": False, "meta_ads": False, "linkedin_ads": False},
            "ecommerce_info": None,
            "html_fetched": False,
        }

    gemini_key = (str(payload.get("Gemini_Key", "")).strip() or os.getenv("GEMINI_KEY", "").strip())
    run_ecommerce_ai = bool(payload.get("run_ecommerce_ai", True))
    user_data = payload.get("data") or {}
    if not isinstance(user_data, dict):
        user_data = {"data": user_data}
    timeout_seconds = min(60, max(5, int(payload.get("html_timeout_seconds", 15))))
    read_timeout_seconds = int(payload.get("Timeout_Seconds", 300))
    connect_timeout_seconds = int(payload.get("Connect_Timeout_Seconds", 60))
    if read_timeout_seconds < 60:
        read_timeout_seconds = 60
    if connect_timeout_seconds < 10:
        connect_timeout_seconds = 10

    result: Dict[str, Any] = {
        "website_url": website_url,
        "tech_stack": {"website_platform": None, "ecommerce_platform": None},
        "checkboxes": {
            "tag_manager": False,
            "google_ads": False,
            "meta_ads": False,
            "linkedin_ads": False,
        },
        "ecommerce_info": None,
    }

    html_snippet = _fetch_page_html(website_url, timeout_seconds=timeout_seconds)
    if not html_snippet:
        logger.warning("company_validation: HTML fetch failed or empty for %s", website_url)
        result["html_fetched"] = False
        result["message"] = "Could not fetch page HTML."
        return {"status": "ok", **result}

    result["html_fetched"] = True
    # Website and ecommerce platform are guaranteed from HTML only (never from AI).
    result["tech_stack"]["website_platform"] = _detect_website_platform_in_html(html_snippet)
    result["tech_stack"]["ecommerce_platform"] = _detect_ecommerce_platform_in_html(html_snippet)
    result["checkboxes"]["tag_manager"] = _detect_tag_manager_in_html(html_snippet)
    result["checkboxes"]["google_ads"] = _detect_google_ads_in_html(html_snippet)
    result["checkboxes"]["meta_ads"] = _detect_meta_ads_in_html(html_snippet)
    result["checkboxes"]["linkedin_ads"] = _detect_linkedin_ads_in_html(html_snippet)

    logger.info("company_validation: tech_stack=%s checkboxes=%s", result["tech_stack"], result["checkboxes"])

    if run_ecommerce_ai and gemini_key:
        try:
            system_instruction_text, user_prompt_text = _build_ad_agency_prompts_ecommerce(
                user_data, website_url=website_url
            )
            completion_text, _ = _call_gemini_generate_content(
                api_key=gemini_key,
                model="gemini-2.5-flash",
                prompt=user_prompt_text,
                system_instruction=system_instruction_text,
                read_timeout_seconds=read_timeout_seconds,
                connect_timeout_seconds=connect_timeout_seconds,
            )
        except requests.HTTPError as http_err:
            result["ecommerce_info"] = {"_error": "Gemini API HTTP error", "details": str(http_err)}
        except requests.RequestException as req_err:
            result["ecommerce_info"] = {"_error": "Gemini API request failed", "details": str(req_err)}
        else:
            if not completion_text or not completion_text.strip():
                result["ecommerce_info"] = {"_error": "Gemini API returned empty output"}
            else:
                try:
                    ecom_parsed = _parse_strict_json_object(completion_text)
                    _ensure_ecommerce_keys(ecom_parsed)
                    _normalize_ecommerce_output(ecom_parsed)
                    # Website and ecommerce platform are guaranteed from HTML only (AI is not asked for these).
                    ecom_parsed["website_platform"] = result["tech_stack"]["website_platform"]
                    ecom_parsed["ecommerce_platform"] = result["tech_stack"]["ecommerce_platform"]
                    result["ecommerce_info"] = ecom_parsed
                    logger.info(
                        "company_validation: ecommerce_info catalogue_size=%s has_subscription=%s",
                        ecom_parsed.get("catalogue_size"),
                        ecom_parsed.get("has_subscription_model"),
                    )
                except ValueError as parse_err:
                    result["ecommerce_info"] = {"_error": "Model output was not valid JSON", "details": str(parse_err)}
    elif run_ecommerce_ai and not gemini_key:
        result["ecommerce_info"] = {
            "_error": "Gemini API key required for ecommerce AI. Set Gemini_Key in payload or GEMINI_KEY env."
        }

    return {"status": "ok", **result}


# ----- Route -----
@company_validation_bp.route("/company_validation", methods=["POST"])
def company_validation():
    """
    Return 202 immediately (avoids Zoho timeout). Process in background, then POST result to Zoho.
    Requires Record_ID and website URL in payload.
    """
    payload = request.get_json(silent=True) or {}
    website_url = _get_website_url_from_payload(payload)
    record_id = payload.get("Record_ID")

    missing = []
    if not website_url:
        missing.append("website_url (or url / data with website/domain)")
    if record_id is None or record_id == "":
        missing.append("Record_ID")

    if missing:
        return (
            jsonify({
                "status": "error",
                "message": "Missing required keys for async processing.",
                "missing": missing,
            }),
            400,
        )

    def _background_worker(payload_data: Dict[str, Any], record_id_value: Any) -> None:
        logger.info("company_validation: background worker started Record_ID=%s", record_id_value)
        try:
            result = _run_company_validation(payload_data)
            body_for_callback: Dict[str, Any] = {**result, "Record_ID": record_id_value}
            try:
                requests.post(
                    ZOHO_COMPANY_VALIDATION_RETURN_URL,
                    headers={"Content-Type": "application/json", "Accept": "application/json"},
                    json={"data": body_for_callback},
                    timeout=(10, 60),
                )
            except Exception:
                pass
        except Exception as e:
            logger.exception("company_validation: [%s] unexpected error %s", record_id_value, e)
            try:
                requests.post(
                    ZOHO_COMPANY_VALIDATION_RETURN_URL,
                    headers={"Content-Type": "application/json", "Accept": "application/json"},
                    json={
                        "data": {
                            "status": "error",
                            "Record_ID": record_id_value,
                            "message": "Processing failed",
                            "details": str(e),
                        }
                    },
                    timeout=(10, 60),
                )
            except Exception:
                pass

    threading.Thread(target=_background_worker, args=(payload, record_id), daemon=True).start()

    return jsonify({
        "status": "accepted",
        "message": "Processing started",
        "Record_ID": record_id,
    }), 202
