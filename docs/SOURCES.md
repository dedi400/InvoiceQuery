# NAV API source material

This project integrates with the Hungarian NAV Online Invoice System 3.0 API.

## Primary reference

Use the current official NAV Online Invoice 3.0 specification and XSD schemas whenever available. The NAV API itself is also authoritative when it returns schema-validation details such as `SCHEMA_VIOLATION` messages.

## External reference repository

A useful public implementation/reference repository is:

- https://github.com/pzs/nav-online-invoice

It is a PHP project, but it contains documentation and API examples that are useful when interpreting NAV request/response structures. The material is partly Hungarian and partly English.

Particularly relevant locations:

- Repository README: https://github.com/pzs/nav-online-invoice/blob/master/README.md
- Documentation: https://github.com/pzs/nav-online-invoice/tree/master/docs
- XML namespace notes: https://github.com/pzs/nav-online-invoice/blob/master/docs/xml_namespaces.md
- API examples: https://github.com/pzs/nav-online-invoice/tree/master/examples
- Query invoice digest example: https://github.com/pzs/nav-online-invoice/blob/master/examples/queryInvoiceDigest.php
- Query invoice data example: https://github.com/pzs/nav-online-invoice/blob/master/examples/queryInvoiceData.php

The external repository is MIT-licensed. This project links to it as reference material rather than copying its implementation.

## Project-specific NAV facts already established

The current implementation has been validated against live NAV responses for the following details:

- Production endpoint base: `https://api.onlineszamla.nav.gov.hu/invoiceService/v3`
- Operation: `queryInvoiceDigest`
- Direction: `INBOUND`
- Query interval: `invoiceIssueDate/dateFrom` and `dateTo`
- API namespace: `http://schemas.nav.gov.hu/OSA/3.0/api`
- Common namespace: `http://schemas.nav.gov.hu/NTCA/1.0/common`
- `header` and `user` are in the common namespace.
- `software` and its children are in the API namespace.
- NAV `SoftwareIdType` requires an 18-character value matching `[0-9A-Z\-]{18}`.
- NAV responses are namespace-qualified and must be parsed accordingly.
- `QueryInvoiceDigestResponse/InvoiceDigest` does not include `summaryGrossData`.
- Some `OPG`-source digest records can have blank amount fields; do not fabricate missing monetary values.

## How to resolve conflicts

If sources disagree, use this order:

1. current official NAV XSD/specification,
2. current live NAV validation/error response,
3. external reference repository,
4. historical comments or assumptions in this project.

When changing XML generation, keep diagnostic request/response logging available so NAV validation errors can be traced precisely.
