# Architecture

## Overview

InvoiceQuery is a weekly multi-company batch process.

```text
Cloud Scheduler
      |
      v
HTTP service / function: weekly_invoice_export
      |
      +--> Google Shared Drive: company configuration workbook
      |
      +--> NAV Online Invoice API (one company at a time)
      |       |
      |       +--> QueryInvoiceDigestRequest, INBOUND
      |       +--> all result pages
      |
      +--> pandas transformation / schema selection
      |
      +--> Google Shared Drive: one rolling Excel workbook per company
      |
      +--> Google Shared Drive: weekly summary log
```

## Weekly period

The HTTP entry point calculates the previous complete Monday-Sunday period. Scheduler does not pass a date range.

For a run on any day in the current week:

- `last_monday = today - timedelta(days=today.weekday() + 7)`
- `last_sunday = last_monday + timedelta(days=6)`

A historical gap or backfill should normally be handled as a one-time local script rather than changing the production schedule logic.

## Company configuration

The configuration workbook is read from Google Drive using `COMPANY_CONFIG_FILE_ID` and must contain a sheet named `companies`.

Required columns:

- `company_code`
- `nav_login`
- `nav_password`
- `nav_tax_number`
- `nav_signature_key`
- `nav_base_url`
- `target_folder_id`
- `active`

Only rows where `active == True` are processed.

The workbook is sensitive because it contains NAV credentials. It belongs on restricted Google Drive storage, not in Git.

## NAV request flow

For each active company:

1. Start at page 1.
2. Generate a fresh request ID and UTC timestamp.
3. Build a NAV 3.0 `QueryInvoiceDigestRequest`.
4. Query `invoiceDirection = INBOUND` for the weekly `invoiceIssueDate` interval.
5. POST to `{nav_base_url}/queryInvoiceDigest`.
6. Reject HTTP failures and HTTP 200 responses whose common `result/funcCode` is `ERROR`, retaining the request XML and response body for the summary log.
7. Parse `currentPage`, `availablePage` and each namespace-qualified `invoiceDigest` only after the business result succeeds.
8. Continue until the last available page.
9. Return a pandas DataFrame.

### NAV namespaces

- API: `http://schemas.nav.gov.hu/OSA/3.0/api`
- Common: `http://schemas.nav.gov.hu/NTCA/1.0/common`

Element namespace placement is schema-sensitive. The current code was corrected using NAV `SCHEMA_VIOLATION` responses, including the requirement that `softwareId`, `softwareName`, `softwareOperation`, etc. belong to the API namespace.

## Output transformation

`OUTPUT_COLUMNS` in `main.py` is the single output schema shared by all companies.

After retrieval:

```python
df = df.reindex(columns=OUTPUT_COLUMNS)
df[DATE_COLUMNS] = df[DATE_COLUMNS].apply(pd.to_datetime, errors="coerce")
df[NUMERIC_COLUMNS] = df[NUMERIC_COLUMNS].apply(pd.to_numeric, errors="coerce")
```

This keeps column selection/order consistent and produces proper date/numeric values for Excel.

## Rolling Excel output

Each company writes to:

```text
{company_code}_invoices.xlsx
```

The service:

1. finds the file in the company's target Shared Drive folder,
2. downloads it if it exists,
3. retains user-added columns found to the right of the queried columns,
4. concatenates old and new rows, leaving those user columns blank on new rows,
5. rewrites the local `.xlsx`, and
6. updates the same Drive file (or creates it on first run).

Current Excel presentation:

- auto-sized columns, capped at a maximum width,
- date format `yyyy-mm-dd`,
- numeric format `#,##0`.

## Google Drive abstraction

All Drive operations should remain centralized in `DriveClient`.

It is responsible for:

- Shared Drive-compatible metadata calls,
- automatic Google Sheets vs binary Excel detection,
- downloading,
- finding files in folders,
- creating files,
- updating files.

Important: service accounts have no personal Drive storage quota. Automated output therefore uses Shared Drives.

## Error handling and logging

Failures are isolated per company.

For each company, the weekly summary records:

- company code,
- period,
- success/failure status,
- invoice count,
- error message,
- failing request XML (when available),
- NAV error response (when available),
- processing timestamp.

A company-level NAV error does not stop subsequent companies.

Job-level failures such as inability to load the configuration workbook still appear in Cloud logging even when Drive-based logging is unavailable.

## Known data limitation

`InvoiceDigest` is intentionally a digest, not the full invoice payload. Some monetary fields may be blank for invoices whose `source` is `OPG`. Neither `summaryGrossData` nor `invoiceGrossAmount` is part of `InvoiceDigestType`, so the digest process does not export or synthesize a gross total. A future requirement for data outside the digest must use `queryInvoiceData` and account for its full response model.
