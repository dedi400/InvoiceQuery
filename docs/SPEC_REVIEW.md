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
| Digest fields | `InvoiceDigestType` provides net and VAT amounts, but no gross total | Removed `invoiceGrossAmount`, which could never be populated by this operation. Gross data must not be inferred; full data would require `/queryInvoiceData`. |
| Optional values | Many digest fields, including monetary values, are optional | Existing coercion to blank/`NaN` is retained. No missing amount is fabricated. |
| Output schema | This service intentionally exports a selected common subset | Queried data is consistently reindexed to `OUTPUT_COLUMNS`. User-added columns at the right of an existing workbook are retained with their historical contents and remain blank on newly appended rows. Obsolete fields mixed into the managed queried columns are removed. |
| Drive and company isolation | Project-specific behaviour, outside the NAV interface specification | Reviewed and retained: Shared Drive flags, native Sheet/binary Excel reads, rolling file updates, and per-company exception isolation remain intact. |

## Deliberately unchanged

- The service remains a digest query. It does not add `/queryInvoiceData`, because that would materially expand network traffic, response parsing, and data handling.
- The selected output does not add every available digest field. `OUTPUT_COLUMNS` intentionally defines the common business export.
- The 30-second client timeout is retained. It exceeds NAV's typical synchronous response time while remaining below the broader job timeout concerns.
- No dependency was added for runtime XSD validation. NAV-specific XML structure is covered by focused unit tests, while live technical validation responses remain captured for diagnosis.

## Future review trigger

Repeat this audit when NAV publishes a newer interface specification or XSD set, when the service begins using a different operation, or when a live NAV validation response contradicts the documented request/response model.
