import hashlib
import io
import unittest
import xml.etree.ElementTree as ET
from unittest.mock import Mock, patch

import pandas as pd

import main


NS_API = "http://schemas.nav.gov.hu/OSA/3.0/api"
NS_COMMON = "http://schemas.nav.gov.hu/NTCA/1.0/common"


class NavApiTest(unittest.TestCase):
    def test_build_query_xml_uses_documented_namespaces_and_signature(self):
        company = {
            "nav_login": "technical-user",
            "nav_password": "password",
            "nav_tax_number": "12345678",
            "nav_signature_key": "signature-key",
        }

        xml = main.build_query_xml(
            "REQUEST1",
            "2026-09-10T12:34:56Z",
            company,
            2,
            "2026-09-01",
            "2026-09-07",
        )
        root = ET.fromstring(xml)

        self.assertEqual(root.tag, f"{{{NS_API}}}QueryInvoiceDigestRequest")
        self.assertIsNotNone(root.find(f"{{{NS_COMMON}}}header"))
        self.assertIsNotNone(root.find(f"{{{NS_COMMON}}}user"))
        self.assertIsNotNone(root.find(f"{{{NS_API}}}software"))
        self.assertEqual(root.findtext(f"{{{NS_API}}}page"), "2")
        self.assertEqual(
            root.findtext(f"{{{NS_API}}}invoiceDirection"), "INBOUND"
        )
        expected_signature = hashlib.sha3_512(
            b"REQUEST120260910123456signature-key"
        ).hexdigest().upper()
        self.assertEqual(
            root.findtext(f".//{{{NS_COMMON}}}requestSignature"),
            expected_signature,
        )

    def test_parse_response_returns_all_digest_rows_and_page_data(self):
        xml = f"""
        <QueryInvoiceDigestResponse xmlns="{NS_API}"
            xmlns:common="{NS_COMMON}">
          <common:result><common:funcCode>OK</common:funcCode></common:result>
          <currentPage>1</currentPage><availablePage>2</availablePage>
          <invoiceDigest><invoiceNumber>INV-1</invoiceNumber></invoiceDigest>
        </QueryInvoiceDigestResponse>
        """

        rows, current_page, available_page = main.parse_response(xml)

        self.assertEqual(rows, [{"invoiceNumber": "INV-1"}])
        self.assertEqual((current_page, available_page), (1, 2))

    def test_fetch_rejects_business_error_returned_with_http_200(self):
        response = Mock(
            status_code=200,
            text=f"""
            <QueryInvoiceDigestResponse xmlns="{NS_API}"
                xmlns:common="{NS_COMMON}">
              <common:result>
                <common:funcCode>ERROR</common:funcCode>
                <common:errorCode>BAD_QUERY_PARAM</common:errorCode>
                <common:message>Invalid query</common:message>
              </common:result>
            </QueryInvoiceDigestResponse>
            """,
        )
        company = {
            "nav_login": "technical-user",
            "nav_password": "password",
            "nav_tax_number": "12345678",
            "nav_signature_key": "signature-key",
            "nav_base_url": "https://example.invalid/invoiceService/v3/",
        }

        with patch.object(main.requests, "post", return_value=response) as post:
            with self.assertRaisesRegex(RuntimeError, "BAD_QUERY_PARAM") as raised:
                main.fetch_all_invoices(company, "2026-09-01", "2026-09-07")

        self.assertIn("QueryInvoiceDigestRequest", raised.exception.args[1])
        self.assertEqual(raised.exception.args[2], response.text)
        self.assertEqual(
            post.call_args.args[0],
            "https://example.invalid/invoiceService/v3/queryInvoiceDigest",
        )
        self.assertEqual(
            post.call_args.kwargs["headers"]["Accept"], "application/xml"
        )


class UpsertCompanyExcelTest(unittest.TestCase):
    def test_existing_workbook_retains_rightmost_user_columns(self):
        existing = pd.DataFrame({
            "invoiceIssueDate": ["2026-08-01"],
            "invoiceNumber": ["old-1"],
            "supplierName": ["Existing supplier"],
            "invoiceDeliveryDate": ["2026-08-02"],
            "paymentDate": ["2026-08-10"],
            "source": ["XML"],
            "currency": ["HUF"],
            "invoiceNetAmount": [1000],
            # This obsolete managed column is not a rightmost user column.
            "invoiceGrossAmount": [1270],
            "comment": ["historical"],
            "Reviewed": [True],
            "Notes": ["Already reconciled"],
        })
        existing_stream = io.BytesIO()
        existing.to_excel(existing_stream, index=False)
        existing_stream.seek(0)
        new = pd.DataFrame({
            "invoiceIssueDate": [pd.Timestamp("2026-09-01")],
            "invoiceNumber": ["new-1"],
            "invoiceNetAmount": [2000],
        })

        written = {}
        drive = Mock()
        drive.find_file_in_folder.return_value = "existing-file-id"
        drive.download_as_excel_stream.return_value = existing_stream
        drive.update_excel.side_effect = (
            lambda _file_id, path: written.setdefault("data", pd.read_excel(path))
        )

        with patch.object(main, "DriveClient", return_value=drive):
            main.upsert_company_excel(new, "COMPANY", "folder-id")

        result = written["data"]
        self.assertEqual(
            list(result.columns),
            main.OUTPUT_COLUMNS + ["Reviewed", "Notes"],
        )
        self.assertNotIn("invoiceGrossAmount", result.columns)
        self.assertTrue(result.loc[0, "Reviewed"])
        self.assertEqual(result.loc[0, "Notes"], "Already reconciled")
        self.assertTrue(pd.isna(result.loc[1, "Reviewed"]))
        self.assertTrue(pd.isna(result.loc[1, "Notes"]))
        self.assertEqual(result.loc[1, "invoiceNetAmount"], 2000)


if __name__ == "__main__":
    unittest.main()
