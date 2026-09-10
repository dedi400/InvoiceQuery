# AGENTS.md

This repository contains a production Python service for weekly retrieval of incoming invoice digests from the Hungarian NAV Online Invoice System.

## Primary goal

Maintain a reliable, simple batch pipeline that:

1. reads multiple companies from a Drive-hosted configuration workbook,
2. queries NAV `QueryInvoiceDigestRequest` for `INBOUND` invoices,
3. retrieves all pages for the requested interval,
4. appends selected fields to one rolling Excel workbook per company,
5. stores files on Google Shared Drive, and
6. writes a weekly summary log.

## Important implementation invariants

Do not change these without a specific reason and validation against current NAV documentation/XSDs.

### NAV request

- API version: 3.0.
- Production base URL normally ends with `/invoiceService/v3`.
- Request type: `QueryInvoiceDigestRequest`.
- Current direction: `INBOUND`.
- Current mandatory filter: `invoiceIssueDate/dateFrom` + `dateTo`.
- Page through all `availablePage` values.
- Request XML uses:
  - API namespace: `http://schemas.nav.gov.hu/OSA/3.0/api`
  - common namespace: `http://schemas.nav.gov.hu/NTCA/1.0/common`
- `header` and `user` belong to the common namespace.
- `software` and every `software*` child belong to the API namespace.
- `softwareId` must match NAV's schema, currently `[0-9A-Z\-]{18}`.
- Password hash: SHA-512 uppercase hex.
- Request signature: SHA3-512 uppercase hex over request ID + masked timestamp + signature key, as implemented in `main.py`.
- NAV schema validation is strict. Preserve namespace placement and element order.
- Send both `Content-Type: application/xml` and `Accept: application/xml`.
- HTTP 200 only confirms that NAV could process the request envelope. Always reject a response whose common `result/funcCode` is `ERROR`.

### Response parsing

- NAV response elements are namespace-qualified.
- `invoiceDigest` must be found using the API namespace.
- Output is intentionally limited to fields listed in global `OUTPUT_COLUMNS`.
- `DATE_COLUMNS` and `NUMERIC_COLUMNS` define pandas/Excel formatting behaviour.
- Do not assume all monetary fields are populated for all sources. In particular, OPG-sourced digest records may have blank amount fields.
- `summaryGrossData` is not part of `InvoiceDigest`; do not invent or parse it from `QueryInvoiceDigestResponse` unless the current schema changes.
- `invoiceGrossAmount` is also not part of `InvoiceDigestType`. Do not derive it from net and VAT amounts; use `queryInvoiceData` if full invoice data becomes a requirement.

### Multi-company behaviour

- The config sheet is named `companies`.
- Required columns are validated in `validate_company_schema`.
- One company's failure must not stop the remaining companies.
- NAV request XML and NAV error response are captured in the weekly summary log on per-company failures.

### Google Drive

- Runtime uses a service account.
- Automated output must live on a Google Shared Drive because service accounts do not have personal Drive storage quota.
- All Drive access should go through `DriveClient`; avoid direct scattered `service.files()` calls.
- Shared Drive calls must keep the relevant `supportsAllDrives=True` / `includeItemsFromAllDrives=True` flags.
- Reads must support both native Google Sheets (`files.export`) and binary Excel (`files.get_media`).
- Company result files are rolling Excel files: existing content is downloaded, new rows are appended, then the same Drive file is updated.

### Excel output

- `OUTPUT_COLUMNS` is the single common schema for every company.
- Reindex the DataFrame to that list before export.
- Preserve user-added columns that follow the queried columns in existing rolling workbooks, including their historical contents; keep them at the rightmost positions and leave them blank on newly appended rows.
- Convert date columns with `pd.to_datetime(..., errors="coerce")` and numeric columns with `pd.to_numeric(..., errors="coerce")`.
- Excel formatting currently uses:
  - dates: `yyyy-mm-dd`
  - numbers: `#,##0`
  - automatic column widths capped by `max_width`.
- Number/date formatting is applied cell-by-cell because `.xlsx` number formats are cell styles.

## Source hierarchy

Use these sources in this order when changing NAV-specific behaviour:

1. The bundled English NAV Online Invoice 3.0 specification dated 12 February 2026 (`docs/EN_Online Invoice System 3.0 Interface Specification (2026.02.12.).pdf`) and current official XSDs.
2. Actual NAV API technical validation/error responses.
3. Reference material in https://github.com/pzs/nav-online-invoice (partly Hungarian, partly English).
4. Existing code and comments in this repository.

Do not blindly copy the external repository: it is a PHP implementation/reference. Translate concepts to this Python codebase and verify against the current NAV 3.0 schema.

See `docs/SOURCES.md` for links.

## Security

This repository is public.

Never commit:

- NAV login/password/signature keys,
- the real company configuration workbook,
- service-account private key JSON,
- real production environment files,
- generated invoice exports or summary logs.

Do not print secrets in normal logs. Request XML may contain a hashed password and request signature; treat diagnostic logs as sensitive operational data even though they do not contain the clear-text password/signature key.

## Change style

- Prefer small, readable changes over frameworks or unnecessary abstractions.
- Keep the weekly job deterministic.
- Preserve per-company error isolation.
- Avoid adding dependencies unless they materially simplify or harden the service.
- If changing XML generation or response parsing, add/retain a way to inspect request/response XML on failure.
