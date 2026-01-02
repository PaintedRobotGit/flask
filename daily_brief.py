from flask import Blueprint, request, jsonify
import requests
from typing import Any, Dict, List, Optional
import json
import threading
import os


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
        "Your job is to read a structured JSON payload describing today's time blocks, projects, priorities, and tasks for all users, and then produce a summarized JSON object.\n\n"
        "INPUT RULES:\n"
        "- The input JSON has these main parts:\n"
        "  - date: string (YYYY-MM-DD)\n"
        "  - users: [ { user, total_hours, morning_hours, afternoon_hours, projects[], blocks[] } ]\n"
        "  - Each project has: project, total_block_hours, priorities[], unprioritized_tasks[]\n"
        "- Tasks include all open work where status is not \"Completed\" or \"Cancelled\". Statuses like \"Doing\", \"On Deck\", and \"Ongoing\" represent active work, while statuses like \"Feedback\" or \"Needs Assistance\" often indicate waiting or blocked work that should still be mentioned.\n"
        "  - priorities: [all active priorities globally]\n"
        "  - errors.priorities_with_no_tasks: list of open priorities that have no associated tasks.\n\n"
        "YOUR TASK:\n"
        "Using ONLY the data in the INPUT_DATA_JSON, produce a single JSON object with the following structure:\n"
        "{\n"
        "  \"date\": string,\n"
        "  \"overall_summary\": {\n"
        "    \"headline\": string,\n"
        "    \"highlights\": [string, ...],\n"
        "    \"capacity_summary\": string,\n"
        "    \"global_priorities\": [string, ...],\n"
        "    \"global_recommended_actions\": [string, ...]\n"
        "  },\n"
        "  \"users\": [\n"
        "    {\n"
        "      \"user\": string,\n"
        "      \"summary\": string,\n"
        "      \"schedule\": {\n"
        "        \"total_hours\": number,\n"
        "        \"morning_hours\": number,\n"
        "        \"afternoon_hours\": number,\n"
        "        \"schedule_summary\": string\n"
        "      },\n"
        "      \"key_projects\": [string, ...],\n"
        "      \"today_focus\": [string, ...],\n"
        "      \"due_today_tasks\": [ { \"task_id\": string, \"name\": string, \"project\": string, \"status\": string } ],\n"
        "      \"overdue_tasks\": [ { \"task_id\": string, \"name\": string, \"project\": string, \"status\": string, \"days_overdue\": number } ],\n"
        "      \"priority_notes\": [string, ...],\n"
        "      \"personal_recommended_actions\": [string, ...]\n"
        "    }\n"
        "  ],\n"
        "  \"admin_notes\": {\n"
        "    \"priority_definition_issues\": [ { \"name\": string, \"project\": string, \"level\": string, \"status\": string } ],\n"
        "    \"other_observations\": [string, ...]\n"
        "  },\n"
        "  \"questions\": [string, ...]\n"
        "}\n\n"
        "DETAILED GUIDANCE:\n"
        "- Be descriptive and insightful — help the team understand what matters today.\n"
        "- Convert days_overdue into whole days (e.g. \"overdue by 3 days\"). NEVER mention milliseconds.\n"
        "- Prioritize actionable clarity and avoid unnecessary repetition.\n"
        "- The \"headline\" should mention key projects or urgent work.\n"
        "- The \"highlights\" section should call out 3–7 focus areas.\n"
        "- \"capacity_summary\" should clearly identify who is fully vs lightly booked.\n"
        "- \"global_priorities\" must list the highest-level priorities explicitly.\n"
        "- \"global_recommended_actions\" should be 3–7 concrete cross-team actions.\n"
        "- For each user:\n"
        "  - Provide a short narrative summary of workload and urgency.\n"
        "  - \"key_projects\" should reflect the type of work (e.g. marketing, dev work, website updates).\n"
        "- If they have NO Doing/On Deck/Ongoing tasks, instruct them to review backlog or look at tasks in \"Feedback\" / \"Needs Assistance\" status and consider what is needed to unblock or move them forward.\n"
        "- \"today_focus\" should have 3–5 of the most important things they should work on today, prioritizing Doing/On Deck/Ongoing and due_today or overdue tasks, but also briefly calling out any key tasks in statuses like \"Feedback\" or \"Needs Assistance\" where they are waiting on someone or blocked.\n"
        "ADMIN NOTES:\n"
        "- Use errors.priorities_with_no_tasks to fill priority_definition_issues.\n"
        "- Point out structural issues such as high-priority work without tasks.\n\n"
        "QUESTIONS:\n"
        "- Include ONLY if real uncertainties exist in the input.\n"
        "- Ask concise questions to resolve missing details (scheduled hours with no tasks, priorities lacking tasks, etc.).\n"
        "- If everything is sufficiently clear, OMIT the questions field entirely.\n\n"
        "OUTPUT FORMAT RULES:\n"
        "- Output MUST be valid JSON.\n"
        "- Do NOT wrap it in markdown or add explanations.\n"
        "- Use double quotes for all keys and values.\n"
        "- Do NOT include html_daily_brief in your output - that will be generated separately.\n\n"
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
    
    # Make API call
    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers=headers,
        json=request_body,
        timeout=(60, 300)  # (connect timeout, read timeout)
    )
    response.raise_for_status()
    
    data = response.json()
    
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
        "Your job is to convert a daily brief summary into a concise, readable HTML format for team chat.\n\n"
        "You will receive:\n"
        "1. A structured JSON summary of today's daily brief (already generated)\n"
        "2. The original raw data (for reference if needed)\n\n"
        "YOUR TASK:\n"
        "Generate a single JSON object with this structure:\n"
        "{\n"
        "  \"html_daily_brief\": string\n"
        "}\n\n"
        "HTML REQUIREMENTS:\n"
        "- Must be concise and readable in a team chat.\n"
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
        "- Convert days_overdue into whole days (e.g. \"overdue by 3 days\"). NEVER mention milliseconds.\n\n"
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
    
    # Make API call
    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers=headers,
        json=request_body,
        timeout=(60, 300)  # (connect timeout, read timeout)
    )
    response.raise_for_status()
    
    data = response.json()
    
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

