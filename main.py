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


# =========================================================
# Validation
# =========================================================

def validate_environment(minimal=False):
    required = ["SUMMARY_LOG_FOLDER_ID"] if minimal else [
        "SUMMARY_LOG_FOLDER_ID",
        "COMPANY_CONFIG_FILE_ID",
    ]

    missing = [v for v in required if not os.environ.get(v)]
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

    if not df["company_code"].is_unique:
        raise ValueError("company_code must be unique")

    if not df["active"].isin([True, False]).all():
        raise ValueError("active column must contain TRUE/FALSE only")


# =========================================================
# Google Drive Wrapper (Shared Drive safe)
# =========================================================

class DriveClient:
    def __init__(self):
        creds, _ = default()
        self.service = build("drive", "v3", credentials=creds)

    def get_metadata(self, file_id):
        return self.service.files().get(
            fileId=file_id,
            fields="id, name, mimeType",
            supportsAllDrives=True
        ).execute()

    def download_as_excel_stream(self, file_id):
        meta = self.get_metadata(file_id)
        mime = meta["mimeType"]

        if mime == "application/vnd.google-apps.spreadsheet":
            request = self.service.files().export(
                fileId=file_id,
                mimeType=(
                    "application/vnd.openxmlformats-officedocument."
                    "spreadsheetml.sheet"
                )
            )
        else:
            request = self.service.files().get_media(
                fileId=file_id,
                supportsAllDrives=True
            )

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        fh.seek(0)
        return fh

    def find_file_in_folder(self, filename, folder_id):
        query = (
            f"name='{filename}' and "
            f"'{folder_id}' in parents and "
            f"trashed=false"
        )

        results = self.service.files().list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        files = results.get("files", [])
        return files[0]["id"] if files else None

    def upload_excel(self, local_path, filename, folder_id):
        media = MediaFileUpload(
            local_path,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        return self.service.files().create(
            body={"name": filename, "parents": [folder_id]},
            media_body=media,
            fields="id",
            supportsAllDrives=True
        ).execute()

    def update_excel(self, file_id, local_path):
        media = MediaFileUpload(
            local_path,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        return self.service.files().update(
            fileId=file_id,
            media_body=media,
            supportsAllDrives=True
        ).execute()


# =========================================================
# NAV XML & API
# =========================================================

def build_query_xml(
    request_id,
    timestamp,
    company,
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
    ET.SubElement(user, "login").text = company["nav_login"]
    ET.SubElement(
        user,
        "passwordHash",
        cryptoType="SHA-512"
    ).text = password_hash(company["nav_password"])
    ET.SubElement(user, "taxNumber").text = str(company["nav_tax_number"])
    ET.SubElement(
        user,
        "requestSignature",
        cryptoType="SHA3-512"
    ).text = request_signature(
        request_id,
        timestamp,
        company["nav_signature_key"]
    )

    software = ET.SubElement(root, "software")
    ET.SubElement(software, "softwareId").text = "MULTI_COMPANY_EXPORT"
    ET.SubElement(software, "softwareName").text = "WeeklyInvoiceExport"
    ET.SubElement(software, "softwareOperation").text = "ONLINE_SERVICE"
    ET.SubElement(software, "softwareMainVersion").text = "1.0"
    ET.SubElement(software, "softwareDevName").text = "Internal"
    ET.SubElement(software, "softwareDevContact").text = "noreply@example.com"

    ET.SubElement(root, "page").text = str(page)
    ET.SubElement(root, "invoiceDirection").text = "OUTBOUND"

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
        rows.append({el.tag: el.text for el in inv})

    return rows, current_page, available_page


def fetch_all_invoices(company, date_from, date_to):
    all_rows = []
    page = 1

    while True:
        request_id = uuid.uuid4().hex[:30]
        timestamp = utc_now_iso()

        xml = build_query_xml(
            request_id,
            timestamp,
            company,
            page,
            date_from,
            date_to
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
# Business logic
# =========================================================

def load_companies_from_drive():
    drive = DriveClient()
    file_id = os.environ["COMPANY_CONFIG_FILE_ID"]

    fh = drive.download_as_excel_stream(file_id)
    df = pd.read_excel(fh, sheet_name="companies")

    validate_company_schema(df)
    return df[df["active"] == True]


def upsert_company_excel(df_new, company_code, folder_id):
    drive = DriveClient()
    filename = f"{company_code}_invoices.xlsx"

    existing_id = drive.find_file_in_folder(filename, folder_id)

    if existing_id:
        fh = drive.download_as_excel_stream(existing_id)
        df_existing = pd.read_excel(fh)
        df_final = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_final = df_new

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, filename)
        df_final.to_excel(path, index=False)

        if existing_id:
            drive.update_excel(existing_id, path)
        else:
            drive.upload_excel(path, filename, folder_id)


def upload_summary_log(df, filename):
    drive = DriveClient()
    folder_id = os.environ["SUMMARY_LOG_FOLDER_ID"]

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, filename)
        df.to_excel(path, index=False)
        drive.upload_excel(path, filename, folder_id)


# =========================================================
# Cloud Function entry point
# =========================================================

def weekly_invoice_export(request):
    today = datetime.date.today()
    last_monday = today - datetime.timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + datetime.timedelta(days=6)

    period_from = last_monday.isoformat()
    period_to = last_sunday.isoformat()

    try:
        validate_environment()
        companies = load_companies_from_drive()

        log_rows = []

        for _, company in companies.iterrows():
            try:
                df = fetch_all_invoices(company, period_from, period_to)
                df["period_from"] = period_from
                df["period_to"] = period_to

                upsert_company_excel(
                    df,
                    company["company_code"],
                    company["target_folder_id"]
                )

                log_rows.append({
                    "company_code": company["company_code"],
                    "period_from": period_from,
                    "period_to": period_to,
                    "status": "SUCCESS",
                    "invoice_count": len(df),
                    "error": "",
                    "processed_at": utc_now_iso()
                })

            except Exception as e:
                log_rows.append({
                    "company_code": company["company_code"],
                    "period_from": period_from,
                    "period_to": period_to,
                    "status": "FAILED",
                    "invoice_count": 0,
                    "error": str(e)[:500],
                    "processed_at": utc_now_iso()
                })

        log_df = pd.DataFrame(log_rows)
        upload_summary_log(
            log_df,
            f"summary_{period_from}_{period_to}.xlsx"
        )

        return {"status": "ok", "companies": len(log_rows)}, 200

    except Exception as e:
        print("CRITICAL FAILURE:", str(e))
        raise
