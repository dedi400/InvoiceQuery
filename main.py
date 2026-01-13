import io
import os
import uuid
import hashlib
import datetime
import tempfile
import requests
import pandas as pd
import xml.etree.ElementTree as ET

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.auth import default


# =========================================================
# Utilities
# =========================================================

def utc_now_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def masked_timestamp(dt_iso):
    dt = datetime.datetime.strptime(dt_iso, "%Y-%m-%dT%H:%M:%SZ")
    return dt.strftime("%Y%m%d%H%M%S")

def password_hash(password):
    return hashlib.sha512(password.encode()).hexdigest().upper()

def request_signature(request_id, timestamp, signature_key):
    base = request_id + masked_timestamp(timestamp) + signature_key
    return hashlib.sha3_512(base.encode()).hexdigest().upper()

def validate_environment():
    required_vars = [
        "COMPANY_CONFIG_FILE_ID",
        "SUMMARY_LOG_FOLDER_ID"
    ]

    missing = [v for v in required_vars if not os.environ.get(v)]

    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}"
        )

def validate_company_schema(df):
    required_columns = {
        "company_code",
        "nav_login",
        "nav_password",
        "nav_tax_number",
        "nav_signature_key",
        "nav_base_url",
        "target_folder_id",
        "active",
    }

    missing = required_columns - set(df.columns)

    if missing:
        raise ValueError(
            f"Company config Excel missing columns: {', '.join(sorted(missing))}"
        )

    if df.empty:
        raise ValueError("Company config Excel contains no rows")

    # Optional: enforce types / content
    if not df["company_code"].is_unique:
        raise ValueError("company_code must be unique")

    invalid_urls = df[
        ~df["nav_base_url"].astype(str).str.startswith("http")
    ]
    if not invalid_urls.empty:
        raise ValueError("nav_base_url must start with http/https")

    invalid_active = df[
        ~df["active"].isin([True, False])
    ]
    if not invalid_active.empty:
        raise ValueError("active column must contain TRUE/FALSE only")


# =========================================================
# XML builders & parsers
# =========================================================

def build_query_xml(
    request_id,
    timestamp,
    login,
    password_hash_value,
    tax_number,
    signature,
    page,
    date_from,
    date_to
):
    root = ET.Element("QueryInvoiceDigestRequest")

    header = ET.SubElement(root, "header")
    ET.SubElement(header, "requestId").text = request_id
    ET.SubElement(header, "timestamp").text = timestamp
    ET.SubElement(header, "requestVersion").text = "3.0"
    ET.SubElement(header, "headerVersion").text = "1.0"

    user = ET.SubElement(root, "user")
    ET.SubElement(user, "login").text = login
    ET.SubElement(user, "passwordHash", cryptoType="SHA-512").text = password_hash_value
    ET.SubElement(user, "taxNumber").text = tax_number
    ET.SubElement(user, "requestSignature", cryptoType="SHA3-512").text = signature

    software = ET.SubElement(root, "software")
    ET.SubElement(software, "softwareId").text = "CORPOFIN_MULTI_COMPANY_EXPORT"
    ET.SubElement(software, "softwareName").text = "WeeklyInvoiceExport"
    ET.SubElement(software, "softwareOperation").text = "ONLINE_SERVICE"
    ET.SubElement(software, "softwareMainVersion").text = "1.0"
    ET.SubElement(software, "softwareDevName").text = "Corpofin Kft."
    ET.SubElement(software, "softwareDevContact").text = "balazs.dedinszky@corpofin.hu"

    ET.SubElement(root, "page").text = str(page)
    ET.SubElement(root, "invoiceDirection").text = "INBOUND"

    iq = ET.SubElement(root, "invoiceQueryParams")
    mandatory = ET.SubElement(iq, "mandatoryQueryParams")
    iid = ET.SubElement(mandatory, "invoiceIssueDate")
    ET.SubElement(iid, "dateFrom").text = date_from
    ET.SubElement(iid, "dateTo").text = date_to

    return ET.tostring(root, encoding="utf-8")


def parse_response(xml_text):
    root = ET.fromstring(xml_text)

    current_page = int(root.findtext("currentPage", "0"))
    available_page = int(root.findtext("availablePage", "0"))

    rows = []
    for inv in root.findall(".//invoiceDigest"):
        row = {el.tag: el.text for el in inv}
        rows.append(row)

    return rows, current_page, available_page


# =========================================================
# NAV query (per company)
# =========================================================

def fetch_all_invoices(company, date_from, date_to):
    all_rows = []
    page = 1

    while True:
        request_id = uuid.uuid4().hex[:30]
        timestamp = utc_now_iso()

        xml = build_query_xml(
            request_id=request_id,
            timestamp=timestamp,
            login=company["nav_login"],
            password_hash_value=password_hash(company["nav_password"]),
            tax_number=str(company["nav_tax_number"]),
            signature=request_signature(
                request_id,
                timestamp,
                company["nav_signature_key"]
            ),
            page=page,
            date_from=date_from,
            date_to=date_to
        )

        resp = requests.post(
            f"{company['nav_base_url']}/queryInvoiceDigest",
            data=xml,
            headers={"Content-Type": "application/xml"},
            timeout=30
        )

        resp.raise_for_status()

        rows, current_page, available_page = parse_response(resp.text)
        all_rows.extend(rows)

        if current_page >= available_page:
            break

        page += 1

    return pd.DataFrame(all_rows)


# =========================================================
# Google Drive helpers
# =========================================================

def get_drive_service():
    creds, _ = default()
    return build("drive", "v3", credentials=creds)


def load_companies_from_drive():
    service = get_drive_service()
    file_id = os.environ["COMPANY_CONFIG_FILE_ID"]

    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.seek(0)
    df = pd.read_excel(fh, sheet_name="companies")
    validate_company_schema(df)
    return df[df["active"] == True]

def upsert_company_excel(df_new, filename, folder_id):
    service = get_drive_service()

    file_id = find_file_in_folder(service, filename, folder_id)

    if file_id:
        # Existing file → append
        df_existing = download_excel_from_drive(file_id)
        df_final = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        # New file
        df_final = df_new

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, filename)
        df_final.to_excel(path, index=False)

        media = MediaFileUpload(
            path,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        if file_id:
            # Overwrite existing
            service.files().update(
                fileId=file_id,
                media_body=media
            ).execute()
        else:
            # Create new
            service.files().create(
                body={"name": filename, "parents": [folder_id]},
                media_body=media,
                fields="id"
            ).execute()

# replaced by upsert_company_excel
# def upload_excel(df, filename, folder_id):
    # service = get_drive_service()

    # with tempfile.TemporaryDirectory() as tmp:
        # path = os.path.join(tmp, filename)
        # df.to_excel(path, index=False)

        # media = MediaFileUpload(
            # path,
            # mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        # )

        # service.files().create(
            # body={"name": filename, "parents": [folder_id]},
            # media_body=media,
            # fields="id"
        # ).execute()

def upload_dataframe_as_excel(df, filename, folder_id):
    service = get_drive_service()

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, filename)
        df.to_excel(path, index=False)

        media = MediaFileUpload(
            path,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        service.files().create(
            body={"name": filename, "parents": [folder_id]},
            media_body=media,
            fields="id"
        ).execute()

def find_file_in_folder(service, filename, folder_id):
    query = (
        f"name='{filename}' and "
        f"'{folder_id}' in parents and "
        f"trashed=false"
    )

    results = service.files().list(
        q=query,
        spaces="drive",
        fields="files(id, name)"
    ).execute()

    files = results.get("files", [])
    return files[0]["id"] if files else None

def download_excel_from_drive(file_id):
    service = get_drive_service()

    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.seek(0)
    return pd.read_excel(fh)

# =========================================================
# Cloud Function entry point
# =========================================================

def weekly_invoice_export(request):
    """
    HTTP-triggered Cloud Function
    """
    validate_environment()
    
    today = datetime.date.today()
    last_monday = today - datetime.timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + datetime.timedelta(days=6)

    companies = load_companies_from_drive()

    log_rows = []

    for _, company in companies.iterrows():
        processed_at = utc_now_iso()

        try:
            df = fetch_all_invoices(
                company,
                last_monday.isoformat(),
                last_sunday.isoformat()
            )
            
            df["period_from"] = last_monday.isoformat()
            df["period_to"] = last_sunday.isoformat()
            filename = filename = f"{company['company_code']}_invoices.xlsx"

            #upload_excel(df,filename,company["target_folder_id"])
            upsert_company_excel(
            df,
            filename,
            company["target_folder_id"]
            )


            log_rows.append({
                "company_code": company["company_code"],
                "period_from": last_monday.isoformat(),
                "period_to": last_sunday.isoformat(),
                "status": "SUCCESS",
                "invoice_count": len(df),
                "error": "",
                "processed_at": processed_at
            })

        except Exception as e:
            log_rows.append({
                "company_code": company["company_code"],
                "period_from": last_monday.isoformat(),
                "period_to": last_sunday.isoformat(),
                "status": "FAILED",
                "invoice_count": 0,
                "error": str(e)[:500],  # safety cap
                "processed_at": processed_at
            })

    # --- Upload weekly summary log ---
    log_df = pd.DataFrame(log_rows)

    log_filename = (
        f"summary_{last_monday}_{last_sunday}.xlsx"
    )

    upload_dataframe_as_excel(
        log_df,
        log_filename,
        os.environ["SUMMARY_LOG_FOLDER_ID"]
    )

    return {
        "period": f"{last_monday} – {last_sunday}",
        "companies_processed": len(log_rows),
        "summary_file": log_filename
    }, 200
