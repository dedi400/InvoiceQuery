import base64
import gzip
import io
import unittest
import xml.etree.ElementTree as ET
from contextlib import redirect_stdout
from unittest.mock import Mock, patch

import pandas as pd

import main


API = "http://schemas.nav.gov.hu/OSA/3.0/api"
COMMON = "http://schemas.nav.gov.hu/NTCA/1.0/common"
DATA = "http://schemas.nav.gov.hu/OSA/3.0/data"
BASE = "http://schemas.nav.gov.hu/OSA/3.0/base"
COMPANY = {
    "nav_login": "technical-user", "nav_password": "password",
    "nav_tax_number": "12345678", "nav_signature_key": "signature-key",
    "nav_base_url": "https://example.invalid/invoiceService/v3/",
}
INVOICE = {
    "invoiceNumber": "OPG-1", "supplierTaxNumber": "87654321",
    "source": "OPG", "currency": "HUF",
}


def group(amount="1270", treatment="<vatContent>0.2126</vatContent>"):
    return f"""<summarySimplified><vatRate>{treatment}</vatRate>
      <vatContentGrossAmount>{amount}</vatContentGrossAmount>
      <vatContentGrossAmountHUF>999999</vatContentGrossAmountHUF>
    </summarySimplified>"""


def detail_response(groups=None, compressed="false", number="OPG-1",
                    supplier="87654321", currency="HUF"):
    if groups is None:
        groups = group()
    payload = f"""<InvoiceData xmlns="{DATA}" xmlns:base="{BASE}">
      <invoiceNumber>{number}</invoiceNumber><invoiceIssueDate>2026-09-01</invoiceIssueDate>
      <completenessIndicator>false</completenessIndicator>
      <invoiceMain><invoice><invoiceHead>
        <supplierInfo><supplierTaxNumber><base:taxpayerId>{supplier}</base:taxpayerId>
        </supplierTaxNumber></supplierInfo>
        <invoiceDetail><invoiceCategory>SIMPLIFIED</invoiceCategory>
          <currencyCode>{currency}</currencyCode></invoiceDetail>
      </invoiceHead><invoiceSummary>{groups}
        <summaryGrossData><invoiceGrossAmount>99999</invoiceGrossAmount></summaryGrossData>
      </invoiceSummary></invoice></invoiceMain></InvoiceData>""".encode()
    if compressed in ("true", "1"):
        payload = gzip.compress(payload)
    encoded = base64.b64encode(payload).decode()
    return f"""<QueryInvoiceDataResponse xmlns="{API}" xmlns:common="{COMMON}">
      <common:result><common:funcCode>OK</common:funcCode></common:result>
      <invoiceDataResult><invoiceData>\n{encoded}\n</invoiceData>
        <auditData><source>OPG</source></auditData>
        <compressedContentIndicator>{compressed}</compressedContentIndicator>
      </invoiceDataResult></QueryInvoiceDataResponse>"""


def digest_response(rows, page=1, pages=1):
    digests = "".join(
        "<invoiceDigest>" + "".join(f"<{k}>{v}</{k}>" for k, v in row.items())
        + "</invoiceDigest>" for row in rows
    )
    return f"""<QueryInvoiceDigestResponse xmlns="{API}" xmlns:common="{COMMON}">
      <common:result><common:funcCode>OK</common:funcCode></common:result>
      <currentPage>{page}</currentPage><availablePage>{pages}</availablePage>
      {digests}</QueryInvoiceDigestResponse>"""


class OpgAmountsTest(unittest.TestCase):
    def test_detail_request_structure_and_authentication(self):
        root = ET.fromstring(main.build_invoice_data_xml(
            "REQUEST1", "2026-09-10T12:34:56Z", COMPANY, INVOICE
        ))
        self.assertEqual(root.tag, f"{{{API}}}QueryInvoiceDataRequest")
        self.assertEqual([n.tag for n in root], [
            f"{{{COMMON}}}header", f"{{{COMMON}}}user",
            f"{{{API}}}software", f"{{{API}}}invoiceNumberQuery",
        ])
        self.assertEqual([n.tag for n in root[-1]], [
            f"{{{API}}}invoiceNumber", f"{{{API}}}invoiceDirection",
            f"{{{API}}}supplierTaxNumber",
        ])
        self.assertEqual([n.text for n in root[-1]], ["OPG-1", "INBOUND", "87654321"])
        self.assertEqual(root.findtext(f".//{{{COMMON}}}requestSignature"),
                         main.request_signature("REQUEST1", "2026-09-10T12:34:56Z", "signature-key"))
        self.assertTrue(all(n.tag.startswith(f"{{{API}}}") for n in root[2]))

    def test_mixed_opg_collectors_and_compression(self):
        groups = (group("1050", "<vatContent>0.0476</vatContent>")
                  + group("1180", "<vatContent>0.1525</vatContent>") + group()
                  + group("100", "<vatExemption><case>TAM</case></vatExemption>")
                  + group("200", "<vatOutOfScope><case>ATK</case></vatOutOfScope>"))
        for compressed in ("true", "false", "1", "0"):
            with self.subTest(compressed=compressed):
                amounts = main.parse_opg_amounts(detail_response(groups, compressed), INVOICE)
                # Use NAV's stated VAT-content fractions, not 5/105 or 27/127.
                self.assertEqual(amounts["invoiceNetAmount"], "3300.07")
                self.assertEqual(amounts["invoiceVatAmount"], "499.93")
                df = main.add_calculated_amounts(pd.DataFrame([amounts]))
                self.assertAlmostEqual(df.loc[0, "invoiceGrossAmount"], 3800)

    def test_zero_negative_and_zero_vat(self):
        for gross, treatment, expected_net, expected_vat in [
            ("-1270", "<vatContent>0.2126</vatContent>", "-1000.00", "-270.00"),
            ("0", "<vatContent>0.2126</vatContent>", "0.00", "0.00"),
            ("1270", "<vatContent>0</vatContent>", "1270.00", "0.00"),
        ]:
            with self.subTest(gross=gross, treatment=treatment):
                amounts = main.parse_opg_amounts(detail_response(group(gross, treatment)), INVOICE)
                self.assertEqual(amounts["invoiceNetAmount"], expected_net)
                self.assertEqual(amounts["invoiceVatAmount"], expected_vat)

    def test_rejects_incomplete_or_unsupported_summary_without_assuming_zero_vat(self):
        for groups in ["", group(""), group("invalid"), group("NaN"),
                       group("Infinity"), group(treatment=""),
                       group(treatment="<vatContent>1.1</vatContent>"),
                       group(treatment="<vatContent>bad</vatContent>"),
                       group(treatment="<vatExemption/>"),
                       group(treatment="<marginSchemeIndicator>TRAVEL_AGENCY</marginSchemeIndicator>"),
                       group() + group(treatment="")]:
            with self.subTest(groups=groups):
                with self.assertRaises(ValueError):
                    main.parse_opg_amounts(detail_response(groups), INVOICE)

    def test_identity_and_currency_checks(self):
        for change in [{"number": "OTHER"}, {"supplier": "11111111"}, {"currency": "EUR"}]:
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, "does not match"):
                main.parse_opg_amounts(detail_response(**change), INVOICE)

    def test_fetch_all_pages_and_only_enrich_missing_opg_amounts(self):
        complete = dict(INVOICE, invoiceNumber="complete", invoiceNetAmount="0", invoiceVatAmount="0")
        non_opg = dict(INVOICE, source="XML", invoiceNumber="other")
        responses = [
            Mock(status_code=200, text=digest_response([INVOICE, non_opg], 1, 2)),
            Mock(status_code=200, text=digest_response([complete, INVOICE], 2, 2)),
            Mock(status_code=200, text=detail_response()),
        ]
        with patch.object(main.requests, "post", side_effect=responses) as post:
            df, _, _ = main.fetch_all_invoices(COMPANY, "2026-09-01", "2026-09-07")
        self.assertEqual(len(df), 4)
        self.assertEqual([call.args[0].rsplit("/", 1)[1] for call in post.call_args_list],
                         ["queryInvoiceDigest", "queryInvoiceDigest", "queryInvoiceData"])
        self.assertEqual(df.loc[0, "invoiceNetAmount"], "1000.00")
        self.assertEqual(df.loc[3, "invoiceNetAmount"], "1000.00")
        self.assertTrue(pd.isna(df.loc[1, "invoiceNetAmount"]))
        self.assertEqual(df.loc[2, "invoiceNetAmount"], "0")
        self.assertEqual(post.call_args.kwargs["headers"],
                         {"Content-Type": "application/xml", "Accept": "application/xml"})
        self.assertEqual(post.call_args.kwargs["timeout"], 30)

    def test_preserves_available_digest_amount_and_fills_missing_currency(self):
        rows = [dict(INVOICE, invoiceNetAmount="1001", invoiceVatAmount="invalid")]
        rows[0].pop("currency")
        with patch.object(main.requests, "post", return_value=Mock(status_code=200, text=detail_response())):
            main.enrich_opg_amounts(COMPANY, rows)
        self.assertEqual(rows[0]["invoiceNetAmount"], "1001")
        self.assertEqual(rows[0]["invoiceVatAmount"], "270.00")
        self.assertEqual(rows[0]["currency"], "HUF")

    def test_errors_capture_diagnostics_and_do_not_print_credentials(self):
        error = f"""<QueryInvoiceDataResponse xmlns="{API}" xmlns:common="{COMMON}">
          <common:result><common:funcCode>ERROR</common:funcCode>
          <common:errorCode>INVALID_REQUEST</common:errorCode></common:result>
        </QueryInvoiceDataResponse>"""
        no_match = f'<QueryInvoiceDataResponse xmlns="{API}"/>'
        bad_base64 = detail_response().replace("<invoiceData>", "<invoiceData>!")
        bad_gzip = detail_response().replace(
            "<compressedContentIndicator>false", "<compressedContentIndicator>true"
        )
        for response in [Mock(status_code=500, text="server error"),
                         Mock(status_code=200, text=error),
                         Mock(status_code=200, text=no_match),
                         Mock(status_code=200, text=bad_base64),
                         Mock(status_code=200, text=bad_gzip),
                         Mock(status_code=200, text="invalid XML"),
                         Mock(status_code=200, text=detail_response(compressed="invalid"))]:
            with self.subTest(response=response), redirect_stdout(io.StringIO()) as output:
                with patch.object(main.requests, "post", return_value=response):
                    with self.assertRaises(RuntimeError) as raised:
                        main.enrich_opg_amounts(COMPANY, [dict(INVOICE)])
                self.assertIn("QueryInvoiceDataRequest", raised.exception.args[1])
                self.assertEqual(raised.exception.args[2], response.text)
                self.assertEqual(output.getvalue(), "")

    def test_missing_query_identity_does_not_send_ambiguous_request(self):
        for field in ("invoiceNumber", "supplierTaxNumber"):
            row = dict(INVOICE)
            del row[field]
            with self.subTest(field=field), patch.object(main.requests, "post") as post:
                with self.assertRaisesRegex(ValueError, field):
                    main.enrich_opg_amounts(COMPANY, [row])
                post.assert_not_called()

    def test_timeout_captures_request(self):
        with patch.object(main.requests, "post", side_effect=main.requests.Timeout("timeout")):
            with self.assertRaises(RuntimeError) as raised:
                main.enrich_opg_amounts(COMPANY, [dict(INVOICE)])
        self.assertIn("QueryInvoiceDataRequest", raised.exception.args[1])
        self.assertEqual(raised.exception.args[2], "")

    def test_detail_failure_isolates_company_and_weekly_export_populates_workbook(self):
        companies = pd.DataFrame([dict(COMPANY, company_code=code, target_folder_id="test")
                                  for code in ("bad", "good")])
        responses = [Mock(status_code=200, text=digest_response([INVOICE])),
                     Mock(status_code=503, text="unavailable"),
                     Mock(status_code=200, text=digest_response([INVOICE])),
                     Mock(status_code=200, text=detail_response())]
        with patch.object(main, "validate_environment"), \
             patch.object(main, "load_companies_from_drive", return_value=companies), \
             patch.object(main.requests, "post", side_effect=responses), \
             patch.object(main, "upsert_company_excel") as upsert, \
             patch.object(main, "upload_summary_log") as summary:
            _, status = main.weekly_invoice_export(None)
        self.assertEqual(status, 200)
        upsert.assert_called_once()
        exported = upsert.call_args.args[0]
        self.assertEqual(list(exported.columns), main.OUTPUT_COLUMNS)
        self.assertEqual(exported.loc[0, "invoiceNetAmount"], 1000)
        self.assertEqual(exported.loc[0, "invoiceGrossAmount"], 1270)
        log = summary.call_args.args[0]
        self.assertEqual(list(log["status"]), ["FAILED", "SUCCESS"])
        self.assertIn("QueryInvoiceDataRequest", log.loc[0, "request_xml"])
        self.assertEqual(log.loc[0, "nav_error_response"], "unavailable")


if __name__ == "__main__":
    unittest.main()
