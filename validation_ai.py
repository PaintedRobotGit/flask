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

        # Conditional ecommerce-specific call
        if customer_type == "ecommerce":
            # Initialize ecommerce-specific fields to null/empty (will be populated by ecommerce call)
            parsed["catalogue_size"] = None
            parsed["product_categories_count"] = None
            parsed["ecommerce_platform"] = None
            parsed["shipping_methods"] = []
            parsed["payment_methods"] = []
            parsed["has_mobile_app"] = None
            parsed["has_subscription_model"] = None
            parsed["international_shipping"] = None
            vendor_response_ecom = None
            try:
                system_instruction_text_ecom, user_prompt_text_ecom = _build_ad_agency_prompts_ecommerce(user_data)
                completion_text_ecom, vendor_response_ecom = _call_gemini_generate_content(
                    api_key=gemini_key,
                    model=model,
                    prompt=user_prompt_text_ecom,
                    system_instruction=system_instruction_text_ecom,
                    read_timeout_seconds=read_timeout_seconds,
                    connect_timeout_seconds=connect_timeout_seconds,
                )
                
                # Check for empty output before parsing
                if not completion_text_ecom or not completion_text_ecom.strip():
                    # Empty output - set to null but don't fail
                    parsed["catalogue_size"] = None
                    if debug_mode:
                        parsed["_ecommerce_call_error"] = {
                            "message": "Gemini API returned empty output (ecommerce call)",
                            "details": "The API response contained no text content",
                            "gemini_response": vendor_response_ecom,
                        }
                else:
                    parsed_ecom = _parse_strict_json_object(completion_text_ecom)
                    # Merge ecommerce-specific fields into primary result
                    parsed.update(parsed_ecom)
            except requests.HTTPError as http_err:
                # Log error but don't fail - primary data is still valid
                parsed["catalogue_size"] = None
                if debug_mode:
                    parsed["_ecommerce_call_error"] = {
                        "message": "Gemini API HTTP error (ecommerce call)",
                        "details": str(http_err),
                        "response": getattr(http_err, "response", None).text if getattr(http_err, "response", None) else None,
                    }
            except requests.RequestException as req_err:
                # Log error but don't fail - primary data is still valid
                parsed["catalogue_size"] = None
                if debug_mode:
                    parsed["_ecommerce_call_error"] = {
                        "message": "Gemini API request failed (ecommerce call)",
                        "details": str(req_err),
                        "gemini_response": vendor_response_ecom,
                    }
            except ValueError as parse_err:
                # Log error but don't fail - primary data is still valid
                parsed["catalogue_size"] = None
                if debug_mode:
                    parsed["_ecommerce_call_error"] = {
                        "message": "Model output was not valid JSON object (ecommerce call)",
                        "details": str(parse_err),
                        "raw_output": completion_text_ecom if 'completion_text_ecom' in locals() else None,
                        "gemini_response": vendor_response_ecom,
                    }

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
            
            # Conditional ecommerce-specific call
            if customer_type_value == "ecommerce":
                # Initialize ecommerce-specific fields to null/empty (will be populated by ecommerce call)
                parsed["catalogue_size"] = None
                parsed["product_categories_count"] = None
                parsed["ecommerce_platform"] = None
                parsed["shipping_methods"] = []
                parsed["payment_methods"] = []
                parsed["has_mobile_app"] = None
                parsed["has_subscription_model"] = None
                parsed["international_shipping"] = None
                try:
                    system_instruction_text_ecom, user_prompt_text_ecom = _build_ad_agency_prompts_ecommerce(payload_data)
                    completion_text_ecom, vendor_response_ecom = _call_gemini_generate_content(
                        api_key=gemini_api_key,
                        model=model_name,
                        prompt=user_prompt_text_ecom,
                        system_instruction=system_instruction_text_ecom,
                        read_timeout_seconds=rt_seconds,
                        connect_timeout_seconds=ct_seconds,
                    )
                    parsed_ecom = _parse_strict_json_object(completion_text_ecom)
                    # Merge ecommerce-specific fields into primary result
                    parsed.update(parsed_ecom)
                except (requests.HTTPError, requests.RequestException, ValueError):
                    # Log error but don't fail - primary data is still valid
                    # Set catalogue_size to null as fallback
                    parsed["catalogue_size"] = None
            
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
        "   - **Key Personnel**: Search for '[company name] leadership team', '[company name] executives', '[company name] management team'. Look on company website 'About' or 'Leadership' pages, LinkedIn company page, or press releases. Only include executives/C-suite and senior VPs.\n"
        "   - **Competitors**: Search for '[company name] competitors' or '[company name] vs competitors'. Look for industry reports, analyst comparisons, or company mentions of competitors. Only include direct competitors in the same market segment.\n"
        "   - **Annual Revenue**: Search for '[company name] revenue', '[company name] annual report', '[company name] financials'. Prioritize official SEC filings, annual reports, or verified financial databases.\n"
        "   - **Size (Employees)**: Search for '[company name] employees', '[company name] workforce size', '[company name] company size'. Check LinkedIn company page, company website, or industry databases.\n"
        "   - **Locations**: Search for '[company name] locations', '[company name] offices', '[company name] headquarters'. Check company website, LinkedIn, or press releases.\n\n"
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
        "  \"key_personnel\": [{ \"name\": string, \"title\": string | null, \"linkedin_profile\": string | null, \"email\": string | null, \"phone\": string | null }],\n"
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
        "- **KEY PERSONNEL:** Only include C-suite executives (CEO, CFO, COO, CTO, etc.) and Senior Vice Presidents. Do not include mid-level managers or department heads unless they are publicly prominent.\n"
        "- **COMPETITORS:** Only include direct competitors in the same market segment. Do not include suppliers, customers, or companies in adjacent markets.\n"
        "- **ANNUAL REVENUE:** Only include if found in official sources (SEC filings, annual reports, verified financial databases). If only estimates are available, use `null`.\n"
        "- Do not invent URLs, emails, or names.\n"
        "- Normalize all URLs to include `https://`.\n"
        "- Deduplicate all lists (e.g., `known_domains`, `sources`).\n"
        "- **All phone numbers must be formatted with the correct country calling code, for example, `+1 (555) 555-5555` for US/Canada numbers.**\n"
        "- If multiple candidates exist for a field, pick the most authoritative or include the top 3.\n"
        "- Output only the JSON object, with no prose or explanation outside of it.\n\n"
        f"Seed hints:\n{data_block}"
    )

    return system_text, user_text


def _build_ad_agency_prompts_ecommerce(user_data: Any) -> Tuple[str, str]:
    """Build system instruction and user prompt for ecommerce-specific fields only.

    Accepts either a string or any JSON-serializable structure in user_data.
    Returns (system_instruction_text, user_prompt_text).
    This is a focused call that only requests ecommerce-specific data like catalogue_size.
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
        "This is a focused research task for ecommerce-specific metrics only."
    )

    # Request structured JSON focused only on ecommerce-specific fields
    user_text = (
        "Your task is to find ecommerce-specific information about a company based on the provided seed hints. "
        "**IMPORTANT:** This company is confirmed to be an ecommerce company (sells products online). "
        "Research the following ecommerce-specific metrics that would be valuable for an advertising agency to know:\n\n"
        "**SEARCH STRATEGY:** Use multiple targeted Google Search queries and website exploration:\n\n"
        "**1. Catalog Size (catalogue_size):**\n"
        "   - Search for '[company name] SKU count' or '[company name] number of SKUs'\n"
        "   - Search for '[company name] product catalog size' or '[company name] total products'\n"
        "   - Search for '[company name] annual report' or '[company name] investor relations' and look for catalog/product count mentions\n"
        "   - For B2B distributors/industrial companies, search for '[company name] parts catalog' or '[company name] inventory size'\n"
        "   - Explore the company's website directly - look for product sitemaps, category pages, or 'shop all' pages that might show product counts\n"
        "   - Check press releases, news articles, or industry publications\n\n"
        "**2. Product Categories Count (product_categories_count):**\n"
        "   - Explore the company's website navigation/menu to count distinct product categories\n"
        "   - Look for category pages, department listings, or product taxonomy\n"
        "   - Search for '[company name] product categories' or '[company name] product lines'\n\n"
        "**3. Ecommerce Platform (ecommerce_platform):**\n"
        "   - Check the website's source code, footer, or 'Powered by' notices\n"
        "   - Look for platform-specific URLs (e.g., *.myshopify.com, *.bigcommerce.com)\n"
        "   - Search for '[company name] ecommerce platform' or '[company name] shopping cart software'\n"
        "   - Check job postings that might mention the platform\n"
        "   - Common platforms: Shopify, Magento, WooCommerce, BigCommerce, Salesforce Commerce Cloud, custom-built, etc.\n\n"
        "**4. Shipping Methods (shipping_methods):**\n"
        "   - Check the company's website shipping/FAQ page\n"
        "   - Look for shipping options during checkout (if accessible)\n"
        "   - Search for '[company name] shipping options' or '[company name] delivery methods'\n\n"
        "**5. Payment Methods (payment_methods):**\n"
        "   - Check the company's website checkout page or payment information\n"
        "   - Look for payment logos/icons (Visa, Mastercard, PayPal, Apple Pay, etc.)\n"
        "   - Search for '[company name] payment methods' or '[company name] accepted payments'\n\n"
        "**6. Mobile App (has_mobile_app):**\n"
        "   - Search for '[company name] mobile app' or '[company name] iOS app' or '[company name] Android app'\n"
        "   - Check App Store or Google Play Store listings\n"
        "   - Look for app download links on the company website\n\n"
        "**7. Subscription Model (has_subscription_model):**\n"
        "   - Look for subscription options on the website\n"
        "   - Search for '[company name] subscription' or '[company name] recurring orders'\n"
        "   - Check if they offer subscription boxes, recurring deliveries, or membership programs\n\n"
        "**8. International Shipping (international_shipping):**\n"
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
        "  \"catalogue_size\": number | null,  // The approximate number of products/SKUs in the company's catalog\n"
        "  \"product_categories_count\": number | null,  // The number of distinct product categories/departments\n"
        "  \"ecommerce_platform\": string | null,  // The ecommerce platform used (e.g., 'Shopify', 'Magento', 'WooCommerce', 'BigCommerce', 'Salesforce Commerce Cloud', 'custom-built', etc.)\n"
        "  \"shipping_methods\": string[],  // Available shipping options (e.g., ['Standard Shipping', 'Express Shipping', 'Overnight', 'International'])\n"
        "  \"payment_methods\": string[],  // Accepted payment methods (e.g., ['Credit Cards', 'PayPal', 'Apple Pay', 'Google Pay', 'Buy Now Pay Later'])\n"
        "  \"has_mobile_app\": boolean | null,  // Whether the company has a mobile app (iOS, Android, or both)\n"
        "  \"has_subscription_model\": boolean | null,  // Whether the company offers subscriptions, recurring orders, or membership programs\n"
        "  \"international_shipping\": boolean | null  // Whether the company ships internationally\n"
        "}\n\n"
        "Rules:\n"
        "- Do not invent or guess information. If you cannot find reliable information after searching, return `null` for single fields or empty arrays `[]` for list fields.\n"
        "- For catalogue_size: If you can only find a range, use the midpoint or lower bound. For very large catalogs (10,000+), round to the nearest thousand if exact number isn't available.\n"
        "- For ecommerce_platform: If you cannot definitively identify the platform, return `null`. Do not guess based on website appearance alone.\n"
        "- For boolean fields: Return `true` only if you find clear evidence. Return `false` if you find evidence they don't have it. Return `null` if you cannot determine.\n"
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


