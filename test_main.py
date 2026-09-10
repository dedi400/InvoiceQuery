import io
import unittest
from unittest.mock import patch

import pandas as pd

import main


class UpsertCompanyExcelTest(unittest.TestCase):
    def test_existing_workbook_preserves_user_columns_and_adds_gross_amount(self):
        existing = pd.DataFrame({
            "invoiceIssueDate": ["2026-08-01"],
            "invoiceNumber": ["old-1"],
            "supplierName": ["Existing supplier"],
            "invoiceDeliveryDate": ["2026-08-02"],
            "paymentDate": ["2026-08-10"],
            "source": ["DATA_EXCHANGE"],
            "currency": ["HUF"],
            "invoiceNetAmount": [1000],
            "Notes": ["Already reviewed"],
            "comment": ["historical"],
        })
        existing_stream = io.BytesIO()
        existing.to_excel(existing_stream, index=False)
        existing_stream.seek(0)

        new = pd.DataFrame({
            "invoiceIssueDate": [pd.Timestamp("2026-09-01")],
            "invoiceNumber": ["new-1"],
            "supplierName": ["New supplier"],
            "invoiceDeliveryDate": [pd.Timestamp("2026-09-02")],
            "paymentDate": [pd.Timestamp("2026-09-10")],
            "source": ["DATA_EXCHANGE"],
            "currency": ["HUF"],
            "invoiceNetAmount": [2000],
            "invoiceGrossAmount": [2540],
            "comment": ["new"],
        })

        written = {}
        drive = unittest.mock.Mock()
        drive.find_file_in_folder.return_value = "existing-file-id"
        drive.download_as_excel_stream.return_value = existing_stream

        def capture_workbook(_file_id, path):
            written["data"] = pd.read_excel(path)

        drive.update_excel.side_effect = capture_workbook

        with patch.object(main, "DriveClient", return_value=drive):
            main.upsert_company_excel(new, "COMPANY", "folder-id")

        result = written["data"]
        self.assertEqual(
            [column for column in result.columns if column in existing.columns],
            list(existing.columns),
        )
        self.assertEqual(result.loc[0, "Notes"], "Already reviewed")
        self.assertEqual(
            result.columns.get_loc("invoiceGrossAmount"),
            result.columns.get_loc("invoiceNetAmount") + 1,
        )
        self.assertTrue(pd.isna(result.loc[0, "invoiceGrossAmount"]))
        self.assertEqual(result.loc[1, "invoiceGrossAmount"], 2540)
        self.assertTrue(pd.isna(result.loc[1, "Notes"]))


if __name__ == "__main__":
    unittest.main()
