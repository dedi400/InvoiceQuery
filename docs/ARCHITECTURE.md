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
      |       +--> QueryInvoiceDataRequest for missing OPG amounts
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
9. Retrieve full invoice data for OPG rows with missing/invalid net or VAT amounts, and fill those amounts from simplified summaries.
10. Return a pandas DataFrame.

### NAV namespaces

- API: `http://schemas.nav.gov.hu/OSA/3.0/api`
- Common: `http://schemas.nav.gov.hu/NTCA/1.0/common`

Element namespace placement is schema-sensitive. The current code was corrected using NAV `SCHEMA_VIOLATION` responses, including the requirement that `softwareId`, `softwareName`, `softwareOperation`, etc. belong to the API namespace.

## Output transformation

`OUTPUT_COLUMNS` in `main.py` is the single output schema shared by all companies.

After retrieval:

```python
df = add_calculated_amounts(df)
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

`InvoiceDigest` is intentionally a digest, not the full invoice payload. Some monetary fields may be blank for invoices whose `source` is `OPG`. Neither `summaryGrossData` nor `invoiceGrossAmount` is part of `InvoiceDigestType`.

The export intentionally adds `invoiceGrossAmount` through `add_calculated_amounts`, before selecting `OUTPUT_COLUMNS`. It sums `invoiceNetAmount` and `invoiceVatAmount`, both in the invoice currency, after numeric coercion. Missing or invalid inputs leave gross blank; zero and negative values retain their arithmetic meaning. The `*HUF` amounts are not used. The existing column name is retained for workbook compatibility, and historical rows without gross are not backfilled by this change.

This is a calculated export value, not a retrieved gross total or a reconciled payable balance. It is not guaranteed to match the reported invoice gross: the bundled specification's warning 880 explicitly checks for differences between reported gross and net plus VAT. A requirement to retrieve the reported gross or other full invoice data must use `queryInvoiceData` and account for its full response model.

### OPG amount retrieval

After digest pagination, `enrich_opg_amounts` calls `/queryInvoiceData` for OPG rows with missing or invalid net/VAT amounts. Queries use the digest invoice number, `INBOUND`, and supplier tax number (the VAT group number where applicable). OPG invoices do not use batch modifications. Repeated lookups for the same supplier, invoice and currency are cached within that company's run. Complete OPG rows and non-OPG rows require no detail calls.

The response is BASE64-decoded and, when indicated, GZIP-decompressed. The parser checks the NAV 3.0 data namespace, invoice number, supplier and currency before reading `invoiceMain/invoice/invoiceSummary/summarySimplified`. It uses invoice-currency `vatContentGrossAmount`, never the HUF counterpart or line totals.

For each summary group, VAT is gross multiplied by the supplied `vatContent` fraction. Explicit `vatExemption` and `vatOutOfScope` groups contribute zero VAT. Missing or unsupported VAT treatment is an error, not zero VAT. Decimal arithmetic sums all groups, rounds aggregate VAT to two decimals using half-up rounding, and derives net as summary gross minus that VAT. This rounding is an export convention. NAV's rounded VAT-content fractions can produce a slightly different net from dividing by a nominal VAT rate. Zero and negative amounts retain their signs.

Only missing/invalid digest amounts are filled; valid digest amounts remain authoritative. A missing digest currency is filled from the detail. `add_calculated_amounts` still calculates exported gross as net plus VAT. The separate reported `summaryGrossData/invoiceGrossAmount` is not substituted, so exported gross remains calculated.

HTTP, business, decoding, identity and amount-parsing failures (including no matching detail) fail that company's run before its workbook is updated. The existing weekly summary captures the failing detail request and response; remaining companies continue. Successful full invoice payloads are used in memory only. Diagnostic XML remains sensitive operational data.

No configuration or runtime dependency is added. Each distinct incomplete OPG invoice adds one sequential request with a 30-second timeout. Historical workbook rows are not backfilled; rerunning a historical interval would append rows under the existing rolling-workbook behaviour.
