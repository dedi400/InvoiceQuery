# NAV 3.0 specification review

## Review baseline

The project was reviewed against the bundled English **NAV Online Invoice System 3.0 Interface Specification dated 12 February 2026**. The review focused on the complete runtime path in `main.py`, its tests, and the repository's operator/agent documentation. The document history says the 12 February 2026 revision updates WARN messages 11400 and 11401; those invoice-validation warnings do not affect this digest-only query service.

## Findings

| Area | Specification requirement | Review outcome |
| --- | --- | --- |
| Service and operation | API context root `/invoiceService/v3`; POST `/queryInvoiceDigest` | Existing implementation conforms. A trailing slash is now normalized before appending the operation. |
| HTTP headers | Requests specify `content-type=application/xml` and `accept=application/xml` | Added the missing `Accept` header. |
| Authentication | SHA-512 uppercase password hash; SHA3-512 uppercase request signature over request ID, UTC timestamp in `YYYYMMDDhhmmss` form, and signature key | Existing hashing conforms. UTC timestamp generation was modernized without changing its wire format. |
| Common request structure | `header` and `user` use the common namespace; `software` uses the API schema | Existing XML generation conforms. Tests now lock down the namespace placement and signature input. |
| Digest query | Page is at least 1, direction is `INBOUND`, and exactly one mandatory query branch is selected | Existing request selects `invoiceIssueDate/dateFrom` and `dateTo`, and weekly intervals are safely below the 35-day maximum. |
| Pagination | The server controls page size and sorting; `currentPage` and `availablePage` start at 0 when there are no results | Existing loop retrieves all pages. Response defaults now model the documented no-result values rather than inventing page 1. |
| Business errors | HTTP 200 can contain `result/funcCode=ERROR` | Fixed: business errors are now rejected and their request/response XML reaches the per-company summary log. |
| Digest fields | `InvoiceDigestType` provides optional net and VAT amounts in the invoice currency, but no gross total | The export intentionally calculates `invoiceGrossAmount` as net plus VAT when both are numeric (PR #5). This is a project-derived value, not a NAV digest field; retrieving reported gross requires `/queryInvoiceData`. |
| Optional values | Many digest fields, including monetary values, are optional | Existing coercion to blank/`NaN` is retained. No missing amount is fabricated. |
| Output schema | This service intentionally exports a selected common subset | Queried data is consistently reindexed to `OUTPUT_COLUMNS`. User-added columns at the right of an existing workbook are retained with their historical contents and remain blank on newly appended rows. Obsolete fields mixed into the managed queried columns are removed. |
| Drive and company isolation | Project-specific behaviour, outside the NAV interface specification | Reviewed and retained: Shared Drive flags, native Sheet/binary Excel reads, rolling file updates, and per-company exception isolation remain intact. |

## Gross amount follow-up review (15 September 2026)

- `455b46b` (PR #1) added the gross output column without a calculation, so digest responses could not populate it.
- `548af37` (PR #4) removed that column and introduced the instruction against deriving gross.
- `4e2672c` (PR #5, merged as `9f68402`) deliberately restored the column with `add_calculated_amounts` and tests for missing/invalid amounts, but left the earlier documentation unchanged.

The bundled specification, section 1.8.6.2 (printed pages 53-56), and the [official API XSD](https://github.com/nav-gov-hu/Online-Invoice/blob/master/src/schemas/nav/gov/hu/OSA/invoiceApi.xsd) reviewed on this date agree that the digest includes optional net and VAT amounts, both in the invoice currency, but no gross-total field. Absence from the response schema does not prohibit a project-calculated export column. The former blanket prohibition was a project restriction, not a NAV schema requirement, and is superseded by PR #5.

Keep the existing calculation and column name. Coerce both inputs to numeric, leave gross blank when either is missing or invalid, and do not substitute zero or mix invoice-currency amounts with HUF amounts. Calculate before selecting the output columns, because VAT is an intermediate input. Historical rows without gross remain blank.

Do not describe the calculation as the reported invoice gross or a reconciled payable balance. Warning 880 in the bundled specification (printed page 335) checks differences between reported gross and net plus VAT; equality is not guaranteed. Retrieving reported gross requires the full invoice response through `queryInvoiceData`.

Validation: the existing six unit tests passed on the local Python 3.14 environment, including calculation and blank-value tests. This follow-up changes documentation only; it does not validate live invoice totals.

## OPG amount implementation (15 September 2026)

The requested OPG fallback supersedes the earlier digest-only scope. Reviewed the bundled specification sections 1.8.5 (printed pages 36-42), 1.6.5 (GZIP handling, printed page 17), and Annex IV: OPG uses single SIMPLIFIED invoices (printed pages 384, 390) and summary groups containing VAT content, exemption or out-of-scope markers (printed pages 394-396). Checked the current official `invoiceApi.xsd` (`InvoiceNumberQueryType`, `InvoiceDataResultType`) and `invoiceData.xsd` (`SummarySimplifiedType`, `VatRateType`).

The implementation retrieves missing OPG amounts and derives net/VAT from invoice-currency simplified summaries. Gross remains calculated by the existing export function. See [architecture](ARCHITECTURE.md#opg-amount-retrieval) for rounding, failure policy and request cost. All 34 offline tests pass, covering request structure, pagination, selective retrieval, mixed collectors, zero/negative amounts, BASE64/GZIP, identity checks and per-company failure isolation.

An explicitly authorized live run using the test configuration retrieved the previous week's invoices, populated OPG net and calculated gross, and appended to the existing workbook. Download verification confirmed that historical rows were retained. No user-added columns were present in that workbook, so their preservation was verified only by the offline regression test. The test destination matched a production destination; the operator explicitly approved the append after that overlap was identified. Operational backups, exports and verification logs remain outside version control. This check did not reconcile calculated net against original invoice documents.

## Deliberately unchanged

- Non-OPG invoices continue to use digest amounts only.
- The selected output does not add every available digest field. `OUTPUT_COLUMNS` intentionally defines the common business export.
- The 30-second client timeout is retained. It exceeds NAV's typical synchronous response time while remaining below the broader job timeout concerns.
- No dependency was added for runtime XSD validation. NAV-specific XML structure is covered by focused unit tests, while live technical validation responses remain captured for diagnosis.

## Future review trigger

Repeat this audit when NAV publishes a newer interface specification or XSD set, when the service begins using a different operation, or when a live NAV validation response contradicts the documented request/response model.
