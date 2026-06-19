"""
Local smoke test for the Google Ads pipeline. Secrets come from env vars so
nothing is hard-coded. Run with the project venv:

    GADS_CLIENT_ID=...        \
    GADS_CLIENT_SECRET=...    \
    GADS_DEV_TOKEN=...        \
    GADS_REFRESH_TOKEN=...    \
    GADS_LOGIN_CUSTOMER_ID=...  (MCC id, digits only ok with/without dashes) \
    GADS_CUSTOMER_ID=...        (client account id) \
    GADS_REPORT_TYPE=campaign_performance \
    ./venv/Scripts/python.exe test_google_ads.py

On Windows PowerShell, set each with `$env:GADS_CLIENT_ID="..."` first.
"""
import os
import json
import google_ads_bp as ads


def main():
    client_id = os.environ["GADS_CLIENT_ID"]
    client_secret = os.environ["GADS_CLIENT_SECRET"]
    developer_token = os.environ["GADS_DEV_TOKEN"]
    refresh_token = os.environ["GADS_REFRESH_TOKEN"]
    login_customer_id = ads._normalize_customer_id(os.environ.get("GADS_LOGIN_CUSTOMER_ID", ""))
    customer_id = ads._normalize_customer_id(os.environ["GADS_CUSTOMER_ID"])
    report_type = os.environ.get("GADS_REPORT_TYPE", "campaign_performance")

    print("1) Exchanging refresh token for access token...")
    access_token = ads._get_google_access_token(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
    )
    print("   OK - got access token")

    query = ads._build_gaql_query(report_type, None)
    print(f"2) Running report '{report_type}':")
    print("   " + query)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "developer-token": developer_token,
        "Content-Type": "application/json",
    }
    if login_customer_id:
        headers["login-customer-id"] = login_customer_id

    result = ads._fetch_all_search_rows(
        headers=headers,
        customer_id=customer_id,
        gaql_query=query,
    )
    print(f"3) Got {result['rowCount']} rows. fieldMask: {result.get('fieldMask')}")
    print("   First row sample:")
    rows = result.get("results", [])
    print(json.dumps(rows[0] if rows else {}, indent=2))


if __name__ == "__main__":
    main()
