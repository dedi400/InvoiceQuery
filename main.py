import io
import base64
import binascii
import gzip
import zlib
import os
import uuid
import hashlib
import datetime
import tempfile
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import requests
from openpyxl.utils import get_column_letter
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
    "invoiceGrossAmount",
    "comment"
]

DATE_COLUMNS = [
    "invoiceIssueDate",
    "invoiceDeliveryDate",
    "paymentDate",
]

NUMERIC_COLUMNS = [
    "invoiceNetAmount",
    "invoiceGrossAmount",
]

# =========================================================
# Utilities
# =========================================================

def utc_now_iso():
    return (
        datetime.datetime.now(datetime.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def masked_timestamp(dt_iso):
    dt = datetime.datetime.strptime(dt_iso, "%Y-%m-%dT%H:%M:%SZ")
    return dt.strftime("%Y%m%d%H%M%S")


def password_hash(password):
    return hashlib.sha512(password.encode()).hexdigest().upper()


def request_signature(request_id, timestamp, signature_key):
    base = request_id + masked_timestamp(timestamp) + signature_key
    return hashlib.sha3_512(base.encode()).hexdigest().upper()

def write_excel_with_autowidth(df, path, sheet_name="Sheet1", max_width=60):
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)

        ws = writer.book[sheet_name]

        # ---- auto column widths ----
        for idx, col in enumerate(df.columns, start=1):
            value_widths = (len(str(value)) for value in df[col] if pd.notna(value))
            max_len = max(len(col), max(value_widths, default=0))
            ws.column_dimensions[get_column_letter(idx)].width = min(
                max_len + 2,
                max_width
            )

        # ---- date formatting ----
        for col in DATE_COLUMNS:
            col_idx = df.columns.get_loc(col) + 1
            col_letter = get_column_letter(col_idx)
            for cell in ws[col_letter][1:]:
                cell.number_format = "yyyy-mm-dd"

        # ---- numeric formatting ----
        for col in NUMERIC_COLUMNS:
            col_idx = df.columns.get_loc(col) + 1
            col_letter = get_column_letter(col_idx)
            for cell in ws[col_letter][1:]:
                cell.number_format = "#,##0"


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

    def get_metadata(self, file_id, fields="id, name, mimeType"):
        return self.service.files().get(
            fileId=file_id,
            fields=fields,
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

def build_request_root(request_id, timestamp, company, request_type):
    NS_API = "http://schemas.nav.gov.hu/OSA/3.0/api"
    NS_COMMON = "http://schemas.nav.gov.hu/NTCA/1.0/common"

    ET.register_namespace("", NS_API)
    ET.register_namespace("common", NS_COMMON)

    root = ET.Element(f"{{{NS_API}}}{request_type}")

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

    return root


def build_query_xml(request_id, timestamp, company, page, date_from, date_to):
    NS_API = "http://schemas.nav.gov.hu/OSA/3.0/api"
    root = build_request_root(
        request_id, timestamp, company, "QueryInvoiceDigestRequest"
    )

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




def parse_nav_envelope(xml_text):
    NS_COMMON = "http://schemas.nav.gov.hu/NTCA/1.0/common"

    root = ET.fromstring(xml_text)

    func_code = root.findtext(f".//{{{NS_COMMON}}}funcCode")
    if func_code == "ERROR":
        error_code = root.findtext(
            f".//{{{NS_COMMON}}}errorCode", "UNKNOWN_ERROR"
        )
        message = root.findtext(f".//{{{NS_COMMON}}}message", "")
        details = f": {message}" if message else ""
        raise ValueError(f"NAV API {error_code}{details}")

    return root


def parse_response(xml_text):
    NS_API = "http://schemas.nav.gov.hu/OSA/3.0/api"
    root = parse_nav_envelope(xml_text)

    current_page = int(
        root.findtext(f".//{{{NS_API}}}currentPage", "0")
    )
    available_page = int(
        root.findtext(f".//{{{NS_API}}}availablePage", "0")
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


def build_invoice_data_xml(request_id, timestamp, company, invoice):
    ns = "http://schemas.nav.gov.hu/OSA/3.0/api"
    root = build_request_root(
        request_id, timestamp, company, "QueryInvoiceDataRequest"
    )
    query = ET.SubElement(root, f"{{{ns}}}invoiceNumberQuery")
    for name, value in (
        ("invoiceNumber", invoice.get("invoiceNumber")),
        ("invoiceDirection", "INBOUND"),
        ("supplierTaxNumber", invoice.get("supplierTaxNumber")),
    ):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"OPG detail query requires {name}")
        ET.SubElement(query, f"{{{ns}}}{name}").text = value
    return ET.tostring(root, encoding="utf-8")


def parse_opg_amounts(xml_text, invoice):
    """Derive invoice-currency net/VAT from NAV's OPG simplified summaries.

    VAT content is a fraction of gross, not a percentage of net. Round the
    aggregate VAT to two decimals; net is the remaining summary gross.
    """
    ns = {
        "a": "http://schemas.nav.gov.hu/OSA/3.0/api",
        "d": "http://schemas.nav.gov.hu/OSA/3.0/data",
        "b": "http://schemas.nav.gov.hu/OSA/3.0/base",
    }
    root = parse_nav_envelope(xml_text)
    if root.tag != f"{{{ns['a']}}}QueryInvoiceDataResponse":
        raise ValueError("Unexpected NAV invoice detail response")
    result = root.find("a:invoiceDataResult", ns)
    if result is None:
        raise ValueError("OPG invoice details were not found")
    encoded = result.findtext("a:invoiceData", "", ns)
    payload = base64.b64decode("".join(encoded.split()), validate=True)
    compressed = result.findtext("a:compressedContentIndicator", "", ns).strip()
    if compressed in ("true", "1"):
        payload = gzip.decompress(payload)
    elif compressed not in ("false", "0"):
        raise ValueError("Invalid invoice detail compression indicator")
    data = ET.fromstring(payload)
    if data.tag != f"{{{ns['d']}}}InvoiceData":
        raise ValueError("Unsupported OPG invoice data schema")
    if data.findtext("d:invoiceNumber", namespaces=ns) != invoice["invoiceNumber"]:
        raise ValueError("OPG detail invoice number does not match digest")
    detail = data.find("d:invoiceMain/d:invoice", ns)
    if detail is None:
        raise ValueError("Expected a single OPG invoice")
    supplier = detail.findtext(
        "d:invoiceHead/d:supplierInfo/d:supplierTaxNumber/b:taxpayerId",
        namespaces=ns,
    )
    if supplier != invoice["supplierTaxNumber"]:
        raise ValueError("OPG detail supplier does not match digest")
    currency = detail.findtext(
        "d:invoiceHead/d:invoiceDetail/d:currencyCode", namespaces=ns
    )
    if not currency or (invoice.get("currency") and currency != invoice["currency"]):
        raise ValueError("OPG detail currency does not match digest")
    groups = detail.findall("d:invoiceSummary/d:summarySimplified", ns)
    if not groups:
        raise ValueError("OPG invoice has no simplified summary amounts")

    def number(text):
        try:
            value = Decimal(text) if text is not None else Decimal("NaN")
        except InvalidOperation:
            raise ValueError("Invalid OPG summary amount or VAT content") from None
        if not value.is_finite():
            raise ValueError("Missing or invalid OPG summary amount or VAT content")
        return value

    gross = Decimal(0)
    vat = Decimal(0)
    for group in groups:
        amount = number(group.findtext("d:vatContentGrossAmount", namespaces=ns))
        rate = group.find("d:vatRate", ns)
        if rate is None or len(rate) != 1:
            raise ValueError("Missing or ambiguous OPG VAT treatment")
        treatment = rate[0]
        if treatment.tag == f"{{{ns['d']}}}vatContent":
            fraction = number(treatment.text)
            if not 0 <= fraction <= 1:
                raise ValueError("Invalid OPG VAT content range")
        elif treatment.tag in (
            f"{{{ns['d']}}}vatExemption", f"{{{ns['d']}}}vatOutOfScope"
        ):
            if not treatment.findtext("d:case", namespaces=ns):
                raise ValueError("Missing OPG VAT exemption or scope case")
            fraction = Decimal(0)
        else:
            raise ValueError("Unsupported OPG VAT treatment")
        gross += amount
        vat += amount * fraction
    vat = vat.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return {
        "invoiceNetAmount": str(gross - vat),
        "invoiceVatAmount": str(vat),
        "currency": currency,
    }


def enrich_opg_amounts(company, rows):
    """Fetch missing OPG amounts before export; failures use company isolation."""
    cache = {}
    for row in rows:
        missing = [
            field for field in ("invoiceNetAmount", "invoiceVatAmount")
            if pd.isna(pd.to_numeric(row.get(field), errors="coerce"))
        ]
        if row.get("source") != "OPG" or not missing:
            continue
        key = (row.get("supplierTaxNumber"), row.get("invoiceNumber"), row.get("currency"))
        if key not in cache:
            xml = build_invoice_data_xml(uuid.uuid4().hex[:30], utc_now_iso(), company, row)
            response_text = ""
            try:
                response = requests.post(
                    f"{company['nav_base_url'].rstrip('/')}/queryInvoiceData",
                    data=xml,
                    headers={"Content-Type": "application/xml", "Accept": "application/xml"},
                    timeout=30,
                )
                response_text = response.text
                if response.status_code != 200:
                    raise ValueError(f"NAV HTTP {response.status_code}")
                cache[key] = parse_opg_amounts(response_text, row)
            except (requests.RequestException, ET.ParseError, ValueError,
                    binascii.Error, OSError, EOFError, zlib.error, InvalidOperation) as error:
                message = ("NAV invoice detail request failed"
                           if isinstance(error, requests.RequestException) else str(error))
                raise RuntimeError(
                    f"OPG amount enrichment failed: {message}",
                    xml.decode("utf-8"), response_text,
                ) from error
        for field in missing:
            row[field] = cache[key][field]
        if not row.get("currency"):
            row["currency"] = cache[key]["currency"]


def add_calculated_amounts(df):
    """Add output amounts that are not provided directly by invoice digest."""
    net_amount = pd.to_numeric(
        df.get("invoiceNetAmount", pd.Series(index=df.index, dtype="float64")),
        errors="coerce",
    )
    vat_amount = pd.to_numeric(
        df.get("invoiceVatAmount", pd.Series(index=df.index, dtype="float64")),
        errors="coerce",
    )
    df["invoiceGrossAmount"] = net_amount + vat_amount
    return df



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
            f"{company['nav_base_url'].rstrip('/')}/queryInvoiceDigest",
            data=xml,
            headers={
                "Content-Type": "application/xml",
                "Accept": "application/xml",
            },
            timeout=30
        )

        last_response_text = resp.text

        if resp.status_code != 200:
            raise RuntimeError(
                f"NAV HTTP {resp.status_code}",
                last_request_xml,
                last_response_text
            )

        try:
            rows, current_page, available_page = parse_response(resp.text)
        except (ET.ParseError, ValueError) as error:
            raise RuntimeError(
                str(error),
                last_request_xml,
                last_response_text
            ) from error
        all_rows.extend(rows)

        if current_page >= available_page:
            break
        page += 1

    enrich_opg_amounts(company, all_rows)
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

        # Users may add their own columns after the queried output columns.
        # Preserve only that rightmost suffix so obsolete/non-NAV columns mixed
        # into the managed schema do not become part of the export contract.
        existing_columns = list(df_existing.columns)
        queried_positions = [
            existing_columns.index(column)
            for column in OUTPUT_COLUMNS
            if column in existing_columns
        ]
        last_queried_position = max(queried_positions, default=-1)
        user_columns = [
            column
            for column in existing_columns[last_queried_position + 1:]
            if column not in OUTPUT_COLUMNS
        ]
        workbook_columns = OUTPUT_COLUMNS + user_columns

        df_existing = df_existing.reindex(columns=workbook_columns)
        df_new = df_new.reindex(columns=workbook_columns)
        df_final = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_final = df_new.reindex(columns=OUTPUT_COLUMNS)

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, filename)
        write_excel_with_autowidth(df_final, path)

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

                # InvoiceDigest has no gross amount field. Calculate it only
                # when both the net and VAT amounts are available.
                df = add_calculated_amounts(df)
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
