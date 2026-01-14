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

#Full column list: 
#OUTPUT_COLUMNS = [
    # "period_from",
    # "period_to",
    # "invoiceNumber",
    # "invoiceOperation",
    # "invoiceCategory",
    # "invoiceIssueDate",
    # "supplierTaxNumber",
    # "supplierGroupMemberTaxNumber",
    # "supplierName",
    # "customerTaxNumber",
    # "customerName",
    # "invoiceAppearance",
    # "source",
    # "invoiceDeliveryDate",
    # "currency",
    # "transactionId",
    # "index",
    # "insDate",
    # "completenessIndicator",
    # "paymentMethod",
    # "paymentDate",
    # "invoiceNetAmount",
    # "invoiceNetAmountHUF",
    # "invoiceVatAmount",
    # "invoiceVatAmountHUF",
    # "comment"
# ]

OUTPUT_COLUMNS = [
    "invoiceIssueDate",
    "invoiceNumber",
    "supplierName",
    "invoiceDeliveryDate",
    "paymentDate",
    "source",
    "currency",
    "invoiceNetAmount",
    "comment"
]

DATE_COLUMNS = [
    "invoiceIssueDate",
    "invoiceDeliveryDate",
    "paymentDate",
]

NUMERIC_COLUMNS = [
    "invoiceNetAmount",
]

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
    NS_API = "http://schemas.nav.gov.hu/OSA/3.0/api"
    NS_COMMON = "http://schemas.nav.gov.hu/NTCA/1.0/common"

    ET.register_namespace("", NS_API)
    ET.register_namespace("common", NS_COMMON)

    root = ET.Element(f"{{{NS_API}}}QueryInvoiceDigestRequest")

    # --- header (common) ---
    header = ET.SubElement(root, f"{{{NS_COMMON}}}header")
    ET.SubElement(header, f"{{{NS_COMMON}}}requestId").text = request_id
    ET.SubElement(header, f"{{{NS_COMMON}}}timestamp").text = timestamp
    ET.SubElement(header, f"{{{NS_COMMON}}}requestVersion").text = "3.0"
    ET.SubElement(header, f"{{{NS_COMMON}}}headerVersion").text = "1.0"

    # --- user (common) ---
    user = ET.SubElement(root, f"{{{NS_COMMON}}}user")
    ET.SubElement(user, f"{{{NS_COMMON}}}login").text = company["nav_login"]

    ET.SubElement(
        user,
        f"{{{NS_COMMON}}}passwordHash",
        cryptoType="SHA-512"
    ).text = password_hash(company["nav_password"])

    ET.SubElement(
        user,
        f"{{{NS_COMMON}}}taxNumber"
    ).text = str(company["nav_tax_number"])

    ET.SubElement(
        user,
        f"{{{NS_COMMON}}}requestSignature",
        cryptoType="SHA3-512"
    ).text = request_signature(
        request_id,
        timestamp,
        company["nav_signature_key"]
    )

    # --- software (api, children ALSO api!) ---
    software = ET.SubElement(root, f"{{{NS_API}}}software")
    ET.SubElement(software, f"{{{NS_API}}}softwareId").text = "CORPOFINCOMPEX0001"
    ET.SubElement(software, f"{{{NS_API}}}softwareName").text = "WeeklyInvoiceExport"
    ET.SubElement(software, f"{{{NS_API}}}softwareOperation").text = "ONLINE_SERVICE"
    ET.SubElement(software, f"{{{NS_API}}}softwareMainVersion").text = "1.0"
    ET.SubElement(software, f"{{{NS_API}}}softwareDevName").text = "Corpofin Kft."
    ET.SubElement(software, f"{{{NS_API}}}softwareDevContact").text = "balazs.dedinszky@corpofin.hu"
    ET.SubElement(software, f"{{{NS_API}}}softwareDevCountryCode").text = "HU"

    # --- paging & direction ---
    ET.SubElement(root, f"{{{NS_API}}}page").text = str(page)
    ET.SubElement(root, f"{{{NS_API}}}invoiceDirection").text = "INBOUND"

    # --- query params ---
    iq = ET.SubElement(root, f"{{{NS_API}}}invoiceQueryParams")
    mandatory = ET.SubElement(iq, f"{{{NS_API}}}mandatoryQueryParams")
    iid = ET.SubElement(mandatory, f"{{{NS_API}}}invoiceIssueDate")
    ET.SubElement(iid, f"{{{NS_API}}}dateFrom").text = date_from
    ET.SubElement(iid, f"{{{NS_API}}}dateTo").text = date_to

    return ET.tostring(root, encoding="utf-8")




def parse_response(xml_text):
    NS_API = "http://schemas.nav.gov.hu/OSA/3.0/api"

    root = ET.fromstring(xml_text)

    def findtext(parent, tag, default=None):
        el = parent.find(f"{{{NS_API}}}{tag}")
        return el.text if el is not None else default

    current_page = int(
        root.findtext(f".//{{{NS_API}}}currentPage", "1")
    )
    available_page = int(
        root.findtext(f".//{{{NS_API}}}availablePage", "1")
    )

    rows = []

    for inv in root.findall(f".//{{{NS_API}}}invoiceDigest"):
        row = {}
        for child in inv:
            # Strip namespace from tag name
            tag = child.tag.split("}", 1)[-1]
            row[tag] = child.text
        rows.append(row)
    
    print("Invoices parsed:", len(rows))
    return rows, current_page, available_page



def fetch_all_invoices(company, date_from, date_to):
    all_rows = []
    page = 1
    last_request_xml = None
    last_response_text = None

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

        last_request_xml = xml.decode("utf-8")

        resp = requests.post(
            f"{company['nav_base_url']}/queryInvoiceDigest",
            data=xml,
            headers={"Content-Type": "application/xml"},
            timeout=30
        )

        last_response_text = resp.text

        if resp.status_code != 200:
            raise RuntimeError(
                f"NAV HTTP {resp.status_code}",
                last_request_xml,
                last_response_text
            )

        rows, current_page, available_page = parse_response(resp.text)
        all_rows.extend(rows)

        if current_page >= available_page:
            break
        page += 1

    return pd.DataFrame(all_rows), last_request_xml, last_response_text



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
                df, request_xml, response_xml = fetch_all_invoices(
                    company,
                    period_from,
                    period_to
                )

                df["period_from"] = period_from
                df["period_to"] = period_to
                
                df = df.reindex(columns=OUTPUT_COLUMNS)
                df[DATE_COLUMNS] = df[DATE_COLUMNS].apply(pd.to_datetime, errors="coerce")
                df[NUMERIC_COLUMNS] = df[NUMERIC_COLUMNS].apply(pd.to_numeric, errors="coerce")

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
                    "request_xml": "",
                    "nav_error_response": "",
                    "processed_at": utc_now_iso()
                })

            except Exception as e:
                request_xml = ""
                response_xml = ""

                if len(e.args) >= 3:
                    request_xml = e.args[1][:30000]     # Excel-safe
                    response_xml = e.args[2][:30000]

                log_rows.append({
                    "company_code": company["company_code"],
                    "period_from": period_from,
                    "period_to": period_to,
                    "status": "FAILED",
                    "invoice_count": 0,
                    "error": str(e.args[0]),
                    "request_xml": request_xml,
                    "nav_error_response": response_xml,
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
