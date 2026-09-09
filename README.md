# InvoiceQuery

InvoiceQuery is a small Python service for downloading **incoming (INBOUND) invoice digests** from the Hungarian NAV Online Invoice System for multiple companies.

The service is designed to run weekly in Google Cloud. It reads company-specific NAV credentials and target folder IDs from an Excel/Google Sheets configuration file stored on a Google **Shared Drive**, queries every available page of `QueryInvoiceDigestRequest`, appends the selected fields to a separate Excel file for each company, and writes a weekly summary log.

## Current behaviour

- Queries `QueryInvoiceDigestRequest` with `invoiceDirection = INBOUND`.
- Uses `invoiceIssueDate` with the previous complete Monday-Sunday interval.
- Retrieves every available result page.
- Reads multiple company configurations from one Drive-hosted workbook.
- Uses Shared Drive-aware Google Drive API calls.
- Appends new rows to one rolling Excel workbook per company.
- Output columns are controlled centrally by `OUTPUT_COLUMNS` in `main.py`.
- Date and numeric columns are converted to proper pandas types and formatted in Excel.
- NAV request XML and NAV error response are included in the summary log when a company fails.
- A failure for one company does not stop processing the remaining companies.

## Repository layout

- `main.py` - production service and HTTP entry point (`weekly_invoice_export`).
- `requirements.txt` - Python dependencies.
- `env.example.yaml` - example environment-variable file; copy it locally to `env.yaml` and fill in the real IDs.
- `AGENTS.md` - project instructions and invariants for Codex and other coding agents.
- `docs/ARCHITECTURE.md` - data flow, configuration schema, NAV/Drive implementation notes and operational behaviour.
- `docs/SOURCES.md` - NAV API reference material and external examples.

## Required environment variables

```yaml
COMPANY_CONFIG_FILE_ID: "YOUR_SHARED_DRIVE_CONFIG_FILE_ID"
SUMMARY_LOG_FOLDER_ID: "YOUR_SHARED_DRIVE_SUMMARY_LOG_FOLDER_ID"
```

Do **not** commit the real `env.yaml` file. It is ignored by Git.

## Company configuration workbook

The workbook must contain a sheet named `companies` with these columns:

| Column | Purpose |
| --- | --- |
| `company_code` | Stable identifier used in output filenames |
| `nav_login` | NAV technical-user login |
| `nav_password` | NAV technical-user password |
| `nav_tax_number` | First 8 digits of the taxpayer number |
| `nav_signature_key` | NAV technical-user signature key |
| `nav_base_url` | NAV API base URL, normally `https://api.onlineszamla.nav.gov.hu/invoiceService/v3` |
| `target_folder_id` | Shared Drive folder for the company's rolling Excel output |
| `active` | `TRUE` / `FALSE` |

The configuration workbook contains credentials and must **never** be committed to this repository.

## Google Drive requirements

The Cloud service runs as a service account. Service accounts do not have personal Drive storage quota, so automated output must be written to a **Shared Drive**. Grant the runtime service account access to:

1. the company configuration file,
2. each company output folder, and
3. the summary-log folder.

The Drive wrapper in `main.py` uses `supportsAllDrives=True` and automatically handles both native Google Sheets and binary `.xlsx` files when reading.

## Local development

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

For local calls that access Google Drive, configure Google Application Default Credentials and ensure the authenticated identity has access to the Shared Drive resources.

## Google Cloud

The production HTTP entry point is:

```text
weekly_invoice_export
```

Cloud Scheduler should invoke the deployed HTTP service once per week. The application itself calculates the previous complete Monday-Sunday period, so the Scheduler only needs to trigger it; it does not pass dates.

## Security

This repository is public. Never commit:

- NAV logins, passwords or signature keys,
- the company configuration workbook,
- Google service-account JSON keys,
- production environment-variable files,
- generated invoice Excel files or summary logs.

See `.gitignore` and use `env.example.yaml` as the template.

## NAV API reference material

The external repository below contains NAV Online Invoice examples, schemas/documentation and implementation material. It is partly English and partly Hungarian:

- https://github.com/pzs/nav-online-invoice

For the most relevant files and the project's NAV-specific implementation rules, see [`docs/SOURCES.md`](docs/SOURCES.md) and [`AGENTS.md`](AGENTS.md).

When reference examples, documentation and a live NAV validation response disagree, treat the current NAV schema/API validation response as authoritative and verify the XML against the applicable XSD before changing production behaviour.
