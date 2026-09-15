"""Opt-in, read-only checks. Never runs the weekly job or uploads a file."""

import argparse
import contextlib
import datetime
import io
import re
from pathlib import Path

import pandas as pd
import yaml
from google.auth.exceptions import DefaultCredentialsError

import main


class ConfigurationError(ValueError):
    pass


def load_test_config(path):
    try:
        config = yaml.safe_load(Path(path).read_text(encoding="utf-8-sig"))
    except yaml.YAMLError as error:
        mark = getattr(error, "problem_mark", None)
        location = f" at line {mark.line + 1}" if mark else ""
        raise ConfigurationError(f"Invalid YAML{location}; values suppressed") from None
    if not isinstance(config, dict):
        raise ConfigurationError("Configuration must be a YAML mapping")
    for key in ("COMPANY_CONFIG_FILE_ID", "COMPANY_TEST_CONFIG_FILE_ID",
                "SUMMARY_LOG_FOLDER_ID"):
        value = config.get(key)
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
            raise ConfigurationError(f"{key} must contain a file/folder ID, not a URL")
    if config["COMPANY_TEST_CONFIG_FILE_ID"] == config["COMPANY_CONFIG_FILE_ID"]:
        raise ConfigurationError("Test workbook must differ from production workbook")
    return config


def require_shared_folder(drive, folder_id):
    metadata = drive.get_metadata(
        folder_id, fields="mimeType,driveId,trashed,capabilities(canAddChildren)"
    )
    if metadata.get("trashed") or metadata.get("mimeType") != "application/vnd.google-apps.folder":
        raise ConfigurationError("Configured output must be an untrashed folder")
    if not metadata.get("driveId"):
        raise ConfigurationError("Output folder must be on a Shared Drive")
    if not metadata.get("capabilities", {}).get("canAddChildren"):
        raise ConfigurationError("Authenticated identity cannot add files to output folder")


def check(path, nav=False):
    stage = "local configuration"
    try:
        config = load_test_config(path)
        print("PASS: local IDs are valid and test workbook differs from production")
        stage = "Google Application Default Credentials"
        drive = main.DriveClient()
        stage = "production configuration metadata (no workbook download)"
        drive.get_metadata(config["COMPANY_CONFIG_FILE_ID"])
        print("PASS: production configuration metadata is accessible")
        stage = "summary folder permissions (no upload)"
        require_shared_folder(drive, config["SUMMARY_LOG_FOLDER_ID"])
        print("PASS: summary folder is on Shared Drive and permits adding files")
        stage = "test workbook download and schema"
        stream = drive.download_as_excel_stream(config["COMPANY_TEST_CONFIG_FILE_ID"])
        companies = pd.read_excel(stream, sheet_name="companies")
        main.validate_company_schema(companies)
        active = companies[companies["active"] == True]
        if active.empty:
            raise ConfigurationError("Test workbook has no active companies")
        print(f"PASS: test workbook schema; {len(active)} active companies")
        for ordinal, (_, company) in enumerate(active.iterrows(), 1):
            stage = f"test company {ordinal} output folder"
            require_shared_folder(drive, company["target_folder_id"])
            print(f"PASS: test company {ordinal} folder is on Shared Drive and permits adding files")
        if nav:
            # A single day bounds this diagnostic query; no rolling workbooks are changed.
            day = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
            failures = 0
            for ordinal, (_, company) in enumerate(active.iterrows(), 1):
                stage = f"test company {ordinal} NAV query"
                try:
                    base_url = str(company["nav_base_url"]).rstrip("/")
                    if base_url not in {
                        "https://api.onlineszamla.nav.gov.hu/invoiceService/v3",
                        "https://api-test.onlineszamla.nav.gov.hu/invoiceService/v3",
                    }:
                        raise ConfigurationError("NAV checks require an official NAV endpoint")
                    with contextlib.redirect_stdout(io.StringIO()):
                        rows, _, _ = main.fetch_all_invoices(company, day, day)
                    print(f"PASS: test company {ordinal} NAV query for {day}; {len(rows)} digests")
                except Exception as error:
                    report_failure(stage, error)
                    failures += 1
            if failures:
                return 1
        print("Read-only checks complete; no Drive files were created or updated.")
        return 0
    except Exception as error:
        report_failure(stage, error)
        return 1


def report_failure(stage, error):
    # Do not print exception bodies: NAV errors can contain signed XML, and
    # Google errors can contain resource IDs or URLs. Keep only safe diagnostics.
    print(f"FAIL: {stage} ({type(error).__name__})")
    if isinstance(error, ConfigurationError):
        print(str(error))
    elif isinstance(error, DefaultCredentialsError):
        print("Python ADC is missing. gcloud init is not sufficient; see docs/LOCAL_TESTING.md.")
    else:
        diagnostic = str(error)
        for marker, hint in {
            "iam.serviceAccounts.getAccessToken": "Missing Service Account Token Creator permission on the impersonated account.",
            "ACCESS_TOKEN_SCOPE_INSUFFICIENT": "Credentials lack a required API scope.",
            "SERVICE_DISABLED": "A required Google API is disabled for the credential project.",
            "invalid_grant": "Google login needs to be refreshed with application-default login.",
            "Gaia id not found": "The impersonated service account could not be found.",
        }.items():
            if marker in diagnostic:
                print(hint)
        status = getattr(getattr(error, "resp", None), "status", None)
        if isinstance(status, int):
            print(f"Google HTTP status: {status}")
        print("Check authentication, API scopes, resource permissions, and configuration. Sensitive details suppressed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default="env.yaml")
    parser.add_argument("--nav", action="store_true", help="Also query yesterday's NAV digests for active test companies")
    args = parser.parse_args()
    raise SystemExit(check(args.env_file, nav=args.nav))
