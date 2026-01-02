from flask import Blueprint, request, jsonify
import requests
from typing import Any, Dict, List, Optional
import json
import threading
import os
import time


daily_brief_bp = Blueprint("daily_brief", __name__)


@daily_brief_bp.route("/daily_brief", methods=["POST"])
def daily_brief():
    payload = request.get_json(silent=True) or {}
    
    # Extract date and users from payload
    date = payload.get("date")
    users = payload.get("users", [])
    priorities = payload.get("priorities", [])
    errors = payload.get("errors", {})
    
    # Validate required fields
    if not date:
        return jsonify({
            "status": "error",
            "message": "Missing required field: date"
        }), 400
    
    if not users:
        return jsonify({
            "status": "error",
            "message": "Missing required field: users"
        }), 400
    
    # Get API key from environment or payload
    api_key_from_payload = str(payload.get("Anthropic_Key", "")).strip()
    api_key_env = os.getenv("ANTHROPIC_KEY", "").strip()
    api_key = api_key_from_payload or api_key_env
    
    if not api_key:
        return jsonify({
            "status": "error",
            "message": "Missing Anthropic API key. Provide Anthropic_Key in payload or set ANTHROPIC_KEY environment variable."
        }), 400
    
    # Get callback URL from payload or use default (user will provide later)
    callback_url = payload.get("callback_url", "")
    
    # Create a clean copy of payload without API key and callback URL before sending to AI
    clean_payload = {k: v for k, v in payload.items() if k not in ("Anthropic_Key", "callback_url")}
    
    # Transform payload: convert priorities from dict to list for each project
    transformed_payload = _transform_payload(clean_payload)
    
    def _background_worker(
        transformed_data: Dict[str, Any],
        anthropic_key: str,
        callback: str
    ) -> None:
        try:
            # Step 1: Generate structured JSON summary (without HTML)
            summary_completion = _call_anthropic_summary(
                api_key=anthropic_key,
                payload_data=transformed_data
            )
            
            # Parse the summary response
            summary_parsed = _parse_strict_json_object(summary_completion)
            
            # Step 2: Generate HTML daily brief using the summary
            html_completion = _call_anthropic_html(
                api_key=anthropic_key,
                summary_data=summary_parsed,
                original_data=transformed_data
            )
            
            # Parse HTML response (it should be a JSON object with html_daily_brief field)
            html_parsed = _parse_strict_json_object(html_completion)
            
            # Combine: merge HTML into summary
            final_output = summary_parsed.copy()
            if "html_daily_brief" in html_parsed:
                final_output["html_daily_brief"] = html_parsed["html_daily_brief"]
            else:
                # Fallback if HTML parsing didn't work as expected
                final_output["html_daily_brief"] = ""
            
            result_body: Dict[str, Any] = {
                "status": "ok",
                "date": transformed_data.get("date"),
                "output": final_output
            }
            
        except requests.HTTPError as http_err:
            result_body = {
                "status": "error",
                "date": transformed_data.get("date"),
                "message": "Anthropic API HTTP error",
                "details": str(http_err),
                "response": getattr(http_err, "response", None).text if getattr(http_err, "response", None) else None,
            }
        except requests.RequestException as req_err:
            result_body = {
                "status": "error",
                "date": transformed_data.get("date"),
                "message": "Anthropic API request failed",
                "details": str(req_err),
            }
        except ValueError as parse_err:
            result_body = {
                "status": "error",
                "date": transformed_data.get("date"),
                "message": "Model output was not valid JSON object",
                "details": str(parse_err),
            }
        except Exception as e:
            result_body = {
                "status": "error",
                "date": transformed_data.get("date"),
                "message": "Unexpected processing error",
                "details": str(e),
            }
        
        # Send result to callback URL if provided
        if callback:
            try:
                requests.post(
                    callback,
                    headers={"Content-Type": "application/json", "Accept": "application/json"},
                    json={"data": result_body},
                    timeout=(10, 60),
                )
            except Exception:
                # Swallow callback errors to avoid crashing the worker thread
                pass
    
    # Start background processing
    worker = threading.Thread(
        target=_background_worker,
        args=(transformed_payload, api_key, callback_url),
        daemon=True,
    )
    worker.start()
    
    # Return immediate response to Zoho
    return jsonify({
        "status": "accepted",
        "message": "Daily brief processing started",
        "date": date,
    }), 202


def _transform_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Transform payload: convert priorities from dict to list for each project."""
    transformed = {
        "date": payload.get("date"),
        "users": [],
        "priorities": payload.get("priorities", []),
        "errors": payload.get("errors", {})
    }
    
    users = payload.get("users", [])
    for user in users:
        user_data = {
            "user": user.get("user"),
            "total_hours": user.get("total_hours"),
            "morning_hours": user.get("morning_hours"),
            "afternoon_hours": user.get("afternoon_hours"),
            "projects": [],
            "blocks": user.get("blocks", [])
        }
        
        # Process projects
        projects = user.get("projects", [])
        for project in projects:
            project_data = {
                "project": project.get("project"),
                "total_block_hours": project.get("total_block_hours"),
                "priorities": [],
                "unprioritized_tasks": project.get("unprioritized_tasks", [])
            }
            
            # Convert priorities from dict to list
            priorities_dict = project.get("priorities")
            if priorities_dict and isinstance(priorities_dict, dict):
                priorities_list = list(priorities_dict.values())
                project_data["priorities"] = priorities_list
            elif isinstance(priorities_dict, list):
                project_data["priorities"] = priorities_dict
            else:
                project_data["priorities"] = []
            
            user_data["projects"].append(project_data)
        
        transformed["users"].append(user_data)
    
    return transformed


def _call_anthropic_summary(*, api_key: str, payload_data: Dict[str, Any]) -> str:
    """Call Anthropic API to generate structured JSON summary (without HTML)."""
    if not api_key:
        raise requests.RequestException("Missing Anthropic API key")
    
    # Build instructions for structured summary
    instructions = (
        "You are a project management assistant for PaintedRobot.\n"
        "Your job is to read a structured JSON payload describing today's time blocks, projects, priorities, and tasks for all users, and then produce a detailed JSON object optimized for generating a professional breakdown page.\n\n"
        "INPUT RULES:\n"
        "- The input JSON has these main parts:\n"
        "  - date: string (YYYY-MM-DD)\n"
        "  - users: [ { user, total_hours, morning_hours, afternoon_hours, projects[], blocks[] } ]\n"
        "  - Each project has: project, total_block_hours, priorities[], unprioritized_tasks[]\n"
        "- Tasks include all open work where status is not \"Completed\" or \"Cancelled\". Statuses like \"Doing\", \"On Deck\", and \"Ongoing\" represent active work, while statuses like \"Feedback\" or \"Needs Assistance\" often indicate waiting or blocked work that should still be mentioned.\n"
        "  - priorities: [all active priorities globally]\n"
        "  - errors.priorities_with_no_tasks: list of open priorities that have no associated tasks.\n\n"
        "USER NAME MAPPING:\n"
        "- IMPORTANT: When the `user` field is exactly `\"paintedrobot\"`, you MUST use `\"Devon\"` as the `display_name` and refer to them as \"Devon\" in all display text (summaries, descriptions, etc.).\n"
        "- Keep the JSON `user` field as `\"paintedrobot\"` for data consistency.\n"
        "- For other users like `\"ryan_paintedrobot\"` or `\"shawn_paintedrobot\"`, extract the name before the underscore (e.g., \"Ryan\", \"Shawn\") and use that as the `display_name`.\n"
        "- Always include both `user` (original) and `display_name` (human-readable) fields in your output.\n\n"
        "YOUR TASK:\n"
        "Using ONLY the data in the INPUT_DATA_JSON, produce a single JSON object with the following structure:\n"
        "{\n"
        "  \"date\": string,\n"
        "  \"overview\": {\n"
        "    \"headline\": string,\n"
        "    \"highlights\": [string, ...],\n"
        "    \"capacity_summary\": string\n"
        "  },\n"
        "  \"team_capacity\": [\n"
        "    {\n"
        "      \"user\": string,\n"
        "      \"display_name\": string,\n"
        "      \"total_hours\": number,\n"
        "      \"morning_hours\": number,\n"
        "      \"afternoon_hours\": number,\n"
        "      \"schedule_summary\": string,\n"
        "      \"workload_assessment\": string\n"
        "    }\n"
        "  ],\n"
        "  \"projects\": [\n"
        "    {\n"
        "      \"project\": string,\n"
        "      \"total_hours\": number,\n"
        "      \"assigned_users\": [string, ...],\n"
        "      \"priorities\": [\n"
        "        {\n"
        "          \"name\": string,\n"
        "          \"level\": string,\n"
        "          \"status\": string,\n"
        "          \"has_tasks\": boolean,\n"
        "          \"tasks\": [{ \"task_id\": string, \"name\": string, \"owner\": string, \"status\": string, \"due_date\": string, \"due_type\": string, \"days_overdue\": number, \"estimated_hours\": number }]\n"
        "        }\n"
        "      ],\n"
        "      \"unprioritized_tasks\": [{ \"task_id\": string, \"name\": string, \"owner\": string, \"status\": string, \"due_date\": string, \"due_type\": string, \"days_overdue\": number, \"estimated_hours\": number }],\n"
        "      \"time_blocks\": [{ \"project\": string, \"start\": string, \"end\": string, \"hours\": number, \"segment\": string }],\n"
        "      \"summary\": string\n"
        "    }\n"
        "  ],\n"
        "  \"priorities\": [\n"
        "    {\n"
        "      \"name\": string,\n"
        "      \"project\": string,\n"
        "      \"level\": string,\n"
        "      \"status\": string,\n"
        "      \"has_tasks\": boolean,\n"
        "      \"tasks\": [{ \"task_id\": string, \"name\": string, \"owner\": string, \"status\": string, \"due_date\": string, \"due_type\": string, \"days_overdue\": number, \"estimated_hours\": number }],\n"
        "      \"assigned_users\": [string, ...],\n"
        "      \"needs_attention\": boolean\n"
        "    }\n"
        "  ],\n"
        "  \"tasks\": {\n"
        "    \"due_today\": [{ \"task_id\": string, \"name\": string, \"project\": string, \"owner\": string, \"status\": string }],\n"
        "    \"overdue\": [{ \"task_id\": string, \"name\": string, \"project\": string, \"owner\": string, \"status\": string, \"days_overdue\": number }],\n"
        "    \"blocked\": [{ \"task_id\": string, \"name\": string, \"project\": string, \"owner\": string, \"status\": string }],\n"
        "    \"by_user\": {\n"
        "      \"user_name\": [{ \"task_id\": string, \"name\": string, \"project\": string, \"status\": string, \"due_date\": string, \"due_type\": string, \"days_overdue\": number }]\n"
        "    }\n"
        "  },\n"
        "  \"recommendations\": {\n"
        "    \"global\": [string, ...],\n"
        "    \"by_user\": {\n"
        "      \"user_name\": [string, ...]\n"
        "    }\n"
        "  },\n"
        "  \"issues\": {\n"
        "    \"priorities_without_tasks\": [{ \"name\": string, \"project\": string, \"level\": string, \"status\": string }],\n"
        "    \"structural_issues\": [string, ...],\n"
        "    \"other_observations\": [string, ...]\n"
        "  },\n"
        "  \"questions\": [string, ...]\n"
        "}\n\n"
        "DETAILED GUIDANCE:\n"
        "- Be descriptive and insightful — help the team understand what matters today.\n"
        "- Convert days_overdue from milliseconds into whole days (e.g., if days_overdue is 15987600000 milliseconds, that's approximately 185 days). NEVER mention milliseconds in output.\n"
        "- Prioritize actionable clarity and avoid unnecessary repetition.\n\n"
        "OVERVIEW SECTION:\n"
        "- The \"headline\" should mention key projects or urgent work.\n"
        "- The \"highlights\" section should call out 3–7 focus areas.\n"
        "- \"capacity_summary\" should clearly identify who is fully vs lightly booked.\n\n"
        "TEAM CAPACITY:\n"
        "- Include all users from the input.\n"
        "- Use `display_name` for human-readable names (\"Devon\" for paintedrobot, extracted names for others).\n"
        "- \"workload_assessment\" should be one of: \"fully booked\", \"lightly booked\", \"moderately booked\", \"overbooked\".\n"
        "- Base assessment on total_hours: 0-2 = lightly booked, 3-6 = moderately booked, 7-8 = fully booked, 8+ = overbooked.\n\n"
        "PROJECTS SECTION:\n"
        "- List all projects that have time blocks scheduled today.\n"
        "- \"assigned_users\" should list all users who have time blocks for this project.\n"
        "- For each priority in a project:\n"
        "  - Set `has_tasks` to true if there are tasks associated with this priority, false otherwise.\n"
        "  - Include tasks array only if `has_tasks` is true.\n"
        "- \"unprioritized_tasks\" are tasks in the project that are not associated with any priority.\n"
        "- \"time_blocks\" should include all blocks from the input for this project.\n"
        "- \"summary\" should be a brief description of the project's focus today.\n\n"
        "PRIORITIES SECTION:\n"
        "- Include all priorities from the global priorities list and from within projects.\n"
        "- Set `has_tasks` based on whether tasks are associated with this priority.\n"
        "- \"assigned_users\" should list users who have tasks or time blocks related to this priority.\n"
        "- Set `needs_attention` to true if:\n"
        "  - The priority has no tasks (`has_tasks` is false), OR\n"
        "  - The priority is high level but has incomplete or blocked tasks, OR\n"
        "  - The priority status suggests it needs attention\n\n"
        "TASK ORGANIZATION:\n"
        "- \"due_today\": All tasks where `due_type` is \"today\".\n"
        "- \"overdue\": All tasks where `due_type` is \"overdue\". Convert `days_overdue` from milliseconds to whole days.\n"
        "- \"blocked\": Tasks where status is \"Feedback\" or \"Needs Assistance\" (waiting on someone or blocked).\n"
        "- \"by_user\": Group tasks by owner, using `display_name` as the key (e.g., \"Devon\", \"Ryan\", \"Shawn\").\n\n"
        "RECOMMENDATIONS:\n"
        "- \"global\": 3–7 concrete cross-team actions or observations.\n"
        "- \"by_user\": 2–5 personalized recommendations per user, using `display_name` as the key.\n"
        "- Focus on actionable items: what should be done, what needs attention, what dependencies exist.\n\n"
        "ISSUES SECTION:\n"
        "- \"priorities_without_tasks\": Use data from errors.priorities_with_no_tasks.\n"
        "- \"structural_issues\": Point out problems like high-priority work without tasks, users with scheduled hours but no tasks, priorities that need task creation.\n"
        "- \"other_observations\": Any other noteworthy observations about the day's work structure.\n\n"
        "QUESTIONS:\n"
        "- Include ONLY if real uncertainties exist in the input.\n"
        "- Ask concise questions to resolve missing details (scheduled hours with no tasks, priorities lacking tasks, etc.).\n"
        "- If everything is sufficiently clear, OMIT the questions field entirely.\n\n"
        "OUTPUT FORMAT RULES:\n"
        "- Output MUST be valid JSON.\n"
        "- Do NOT wrap it in markdown or add explanations.\n"
        "- Use double quotes for all keys and values.\n"
        "- Do NOT include html_daily_brief in your output - that will be generated separately.\n"
        "- Ensure all user references in display text use `display_name` (e.g., \"Devon\" not \"paintedrobot\").\n\n"
        "Below is today's INPUT_DATA_JSON:"
    )
    
    # Build messages
    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": instructions
                },
                {
                    "type": "text",
                    "text": f"INPUT_DATA_JSON:\n{json.dumps(payload_data, ensure_ascii=False, indent=2)}"
                }
            ]
        }
    ]
    
    # Build request payload
    request_body = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 10000,
        "temperature": 0.3,
        "messages": messages
    }
    
    # Headers
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01"
    }
    
    # Make API call with retry logic for rate limiting and server errors
    max_attempts = 3
    attempt = 0
    last_exc: Optional[Exception] = None
    data = None
    
    while attempt < max_attempts:
        try:
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=request_body,
                timeout=(60, 300)  # (connect timeout, read timeout)
            )
            
            # Handle rate limiting (429) and server errors (5xx) with retries
            if response.status_code == 429:
                attempt += 1
                if attempt >= max_attempts:
                    response.raise_for_status()
                # Check for Retry-After header, otherwise use exponential backoff
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait_time = int(retry_after)
                    except ValueError:
                        wait_time = 2 ** attempt
                else:
                    # Exponential backoff: 2s, 4s, 8s
                    wait_time = 2 ** attempt
                time.sleep(wait_time)
                continue
            elif 500 <= response.status_code < 600:
                attempt += 1
                if attempt >= max_attempts:
                    response.raise_for_status()
                # Exponential backoff for server errors
                wait_time = 2 ** attempt
                time.sleep(wait_time)
                continue
            
            response.raise_for_status()
            data = response.json()
            break
            
        except requests.HTTPError as http_err:
            # If it's a 429 or 5xx that we haven't retried enough, continue
            if http_err.response is not None:
                status_code = http_err.response.status_code
                if (status_code == 429 or (500 <= status_code < 600)) and attempt < max_attempts - 1:
                    attempt += 1
                    wait_time = 2 ** attempt
                    time.sleep(wait_time)
                    continue
            # Otherwise, re-raise
            raise
        except requests.RequestException as req_err:
            last_exc = req_err
            attempt += 1
            if attempt >= max_attempts:
                raise
            # Exponential backoff for other request errors
            wait_time = 2 ** attempt
            time.sleep(wait_time)
    
    if data is None:
        raise requests.RequestException("Failed to get response from Anthropic API after retries")
    
    # Extract text from response
    text_output = ""
    content = data.get("content", [])
    for item in content:
        if item.get("type") == "text":
            text_output += item.get("text", "")
    
    if not text_output:
        raise requests.RequestException("Anthropic API returned empty content")
    
    return text_output


def _call_anthropic_html(*, api_key: str, summary_data: Dict[str, Any], original_data: Dict[str, Any]) -> str:
    """Call Anthropic API to generate HTML daily brief from the summary."""
    if not api_key:
        raise requests.RequestException("Missing Anthropic API key")
    
    # Build instructions for HTML generation
    instructions = (
        "You are a project management assistant for PaintedRobot.\n"
        "Your job is to convert a daily brief summary into a concise, readable HTML format for team chat.\n"
        "IMPORTANT: This is a BRIEF team chat update, NOT a detailed breakdown page. Keep it concise and scannable.\n\n"
        "You will receive:\n"
        "1. A structured JSON summary of today's daily brief (already generated)\n"
        "2. The original raw data (for reference if needed)\n\n"
        "USER NAME MAPPING:\n"
        "- IMPORTANT: Always use `display_name` from the summary (e.g., \"Devon\", \"Ryan\", \"Shawn\").\n"
        "- NEVER use the raw `user` field values like \"paintedrobot\" in the HTML output.\n"
        "- The summary JSON includes `display_name` fields - use those for all user references.\n\n"
        "YOUR TASK:\n"
        "Generate a single JSON object with this structure:\n"
        "{\n"
        "  \"html_daily_brief\": string\n"
        "}\n\n"
        "HTML REQUIREMENTS:\n"
        "- Must be concise and readable in a team chat (this is NOT a detailed breakdown page).\n"
        "- Use ONLY these HTML tags: h2, h3, p, ul, li, strong, em, br.\n"
        "- Do NOT use div, span, or other container tags.\n"
        "- Structure should be:\n"
        "  1) Projects We Are Working On Today — short list with type of work & hours\n"
        "  2) Main Priorities — 3–7 bullets\n"
        "  3) User Breakdown — a few short, compact sentences per user\n"
        "- Do not overload with long bullet lists.\n"
        "- Keep focus on what the TEAM is doing today.\n"
        "- Be conversational and actionable.\n"
        "- Highlight urgent items and key focus areas.\n"
        "- Convert days_overdue into whole days (e.g. \"overdue by 3 days\"). NEVER mention milliseconds.\n"
        "- Use display names from the summary (e.g., \"Devon\", \"Ryan\", \"Shawn\") - never use raw user IDs.\n\n"
        "CONTENT PRIORITIZATION:\n"
        "- Focus on what's happening TODAY, not everything in the system.\n"
        "- Emphasize urgent items (due today, overdue, blocked tasks).\n"
        "- Highlight priorities that need attention (especially those without tasks).\n"
        "- Keep user breakdowns brief - 2-3 sentences max per user.\n"
        "- Don't repeat information that's already in the projects or priorities sections.\n\n"
        "OUTPUT FORMAT RULES:\n"
        "- Output MUST be valid JSON with only the html_daily_brief field.\n"
        "- Do NOT wrap it in markdown or add explanations.\n"
        "- Use double quotes for all keys and values.\n"
        "- Escape HTML properly within the JSON string.\n\n"
        "Below is the SUMMARY_JSON (use this as your primary source):"
    )
    
    # Build messages
    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": instructions
                },
                {
                    "type": "text",
                    "text": f"SUMMARY_JSON:\n{json.dumps(summary_data, ensure_ascii=False, indent=2)}"
                },
                {
                    "type": "text",
                    "text": f"\n\nORIGINAL_DATA_JSON (for reference only):\n{json.dumps(original_data, ensure_ascii=False, indent=2)}"
                }
            ]
        }
    ]
    
    # Build request payload
    request_body = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 4000,  # Less tokens needed for HTML generation
        "temperature": 0.3,
        "messages": messages
    }
    
    # Headers
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01"
    }
    
    # Make API call with retry logic for rate limiting and server errors
    max_attempts = 3
    attempt = 0
    last_exc: Optional[Exception] = None
    data = None
    
    while attempt < max_attempts:
        try:
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=request_body,
                timeout=(60, 300)  # (connect timeout, read timeout)
            )
            
            # Handle rate limiting (429) and server errors (5xx) with retries
            if response.status_code == 429:
                attempt += 1
                if attempt >= max_attempts:
                    response.raise_for_status()
                # Check for Retry-After header, otherwise use exponential backoff
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait_time = int(retry_after)
                    except ValueError:
                        wait_time = 2 ** attempt
                else:
                    # Exponential backoff: 2s, 4s, 8s
                    wait_time = 2 ** attempt
                time.sleep(wait_time)
                continue
            elif 500 <= response.status_code < 600:
                attempt += 1
                if attempt >= max_attempts:
                    response.raise_for_status()
                # Exponential backoff for server errors
                wait_time = 2 ** attempt
                time.sleep(wait_time)
                continue
            
            response.raise_for_status()
            data = response.json()
            break
            
        except requests.HTTPError as http_err:
            # If it's a 429 or 5xx that we haven't retried enough, continue
            if http_err.response is not None:
                status_code = http_err.response.status_code
                if (status_code == 429 or (500 <= status_code < 600)) and attempt < max_attempts - 1:
                    attempt += 1
                    wait_time = 2 ** attempt
                    time.sleep(wait_time)
                    continue
            # Otherwise, re-raise
            raise
        except requests.RequestException as req_err:
            last_exc = req_err
            attempt += 1
            if attempt >= max_attempts:
                raise
            # Exponential backoff for other request errors
            wait_time = 2 ** attempt
            time.sleep(wait_time)
    
    if data is None:
        raise requests.RequestException("Failed to get response from Anthropic API after retries")
    
    # Extract text from response
    text_output = ""
    content = data.get("content", [])
    for item in content:
        if item.get("type") == "text":
            text_output += item.get("text", "")
    
    if not text_output:
        raise requests.RequestException("Anthropic API returned empty content")
    
    return text_output


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

