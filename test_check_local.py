import contextlib
import io
import unittest
from unittest.mock import Mock, patch

import check_local


class LocalCheckTest(unittest.TestCase):
    def test_rejects_production_fallback_and_missing_test_id(self):
        for test_id in [None, "production", "https://example.invalid/workbook"]:
            data = {"COMPANY_CONFIG_FILE_ID": "production", "SUMMARY_LOG_FOLDER_ID": "summary"}
            if test_id is not None:
                data["COMPANY_TEST_CONFIG_FILE_ID"] = test_id
            with self.subTest(test_id=test_id), patch.object(check_local.Path, "read_text", return_value=""), \
                 patch.object(check_local.yaml, "safe_load", return_value=data), self.assertRaises(check_local.ConfigurationError):
                check_local.load_test_config("unused")

    def test_yaml_error_does_not_expose_configuration(self):
        with patch.object(check_local.Path, "read_text", return_value="secret: [PRIVATE_VALUE"), \
             self.assertRaises(check_local.ConfigurationError) as caught:
            check_local.load_test_config("unused")
        self.assertNotIn("PRIVATE_VALUE", str(caught.exception))

    def test_folder_must_be_shared_untrashed_and_writable(self):
        valid = {"mimeType": "application/vnd.google-apps.folder", "driveId": "shared",
                 "capabilities": {"canAddChildren": True}}
        for changes in [{"driveId": None}, {"mimeType": "text/plain"}, {"trashed": True}, {"capabilities": {}}]:
            drive = Mock()
            drive.get_metadata.return_value = dict(valid, **changes)
            with self.subTest(changes=changes), self.assertRaises(check_local.ConfigurationError):
                check_local.require_shared_folder(drive, "folder")

    def test_read_only_check_downloads_only_test_config_and_never_writes(self):
        config = {"COMPANY_CONFIG_FILE_ID": "production", "COMPANY_TEST_CONFIG_FILE_ID": "test", "SUMMARY_LOG_FOLDER_ID": "summary"}
        drive = Mock()
        drive.get_metadata.return_value = {"mimeType": "application/vnd.google-apps.folder", "driveId": "shared", "capabilities": {"canAddChildren": True}}
        from test_debugging import company
        frame = check_local.pd.DataFrame([company()])
        with patch.object(check_local, "load_test_config", return_value=config), \
             patch.object(check_local.main, "DriveClient", return_value=drive), \
             patch.object(check_local.pd, "read_excel", return_value=frame), \
             patch.object(check_local.main, "fetch_all_invoices") as nav, \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(check_local.check("unused"), 0)
        drive.download_as_excel_stream.assert_called_once_with("test")
        drive.upload_excel.assert_not_called()
        drive.update_excel.assert_not_called()
        nav.assert_not_called()

    def test_error_output_hides_signed_xml_and_ids(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            check_local.report_failure("NAV query", RuntimeError("PRIVATE_ID", "SIGNED_XML", "PRIVATE_RESPONSE"))
        for secret in ["PRIVATE_ID", "SIGNED_XML", "PRIVATE_RESPONSE"]:
            self.assertNotIn(secret, output.getvalue())


if __name__ == "__main__":
    unittest.main()
