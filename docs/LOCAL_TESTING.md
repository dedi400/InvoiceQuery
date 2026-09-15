# Local development and connection checks

## Install and run offline tests

From the repository root in PowerShell:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m unittest discover -v
```

VS Code's Testing panel uses the same unittest discovery when configured for `.venv`.
The tests use synthetic data and mocked network calls; Google and NAV credentials are not required.
Coverage includes pagination, empty results, API/XML failures, company schema checks,
inactive companies, per-company failure isolation and diagnostic logging, Shared Drive
flags, native Sheets/binary Excel downloads, Excel styles, and calculated gross amounts.

## Local configuration

Copy `env.example.yaml` to the Git-ignored `env.yaml` and populate:

```yaml
COMPANY_CONFIG_FILE_ID: "PRODUCTION_CONFIG_WORKBOOK_ID"
COMPANY_TEST_CONFIG_FILE_ID: "SEPARATE_TEST_CONFIG_WORKBOOK_ID"
SUMMARY_LOG_FOLDER_ID: "SUMMARY_FOLDER_ID"
```

Use IDs, not full URLs. Put notes and reference URLs on lines beginning with `#`.
The test workbook needs the same `companies` schema as production, with test output
folder IDs in `target_folder_id`. At least one company must be active for live checks.
The checker requires a test workbook different from production and never falls back
to the production workbook. This does not prove that every folder in that workbook
is isolated from production; verify those IDs before any future write tests.

Only `check_local.py` loads this YAML. The production entry point continues to read
process environment variables. `VERBOSE_LOGGING` and `MAX_COMPANIES_PER_RUN` in the
example are currently unused; they do not limit a production run.

## Google authentication

`gcloud init` selects a project and authenticates the CLI. Python's
`google.auth.default()` needs separate Application Default Credentials (ADC).
For service-account impersonation:

1. Enable the Google Drive API and IAM Service Account Credentials API in the project.
2. Have an administrator grant your signed-in user **Service Account Token Creator**
   on the intended service account.
3. Grant that service account access to the configuration workbooks and relevant
   Shared Drive folders. Adding output files requires folder write permission.
4. Run and complete the browser sign-in:

```powershell
gcloud auth application-default login --impersonate-service-account=SERVICE_ACCOUNT_EMAIL --scopes=https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/drive
```

If `gcloud` is missing from an existing VS Code terminal after installation, restart
VS Code or use the Google Cloud CLI terminal. ADC is stored outside the repository;
do not copy its tokens into YAML, tests, or chat.

Reference: [Google's local ADC setup](https://docs.cloud.google.com/docs/authentication/set-up-adc-local-dev-environment).

## Read-only live checks

```powershell
# Validate configuration, Google authentication, workbook schema and folder capabilities:
.\.venv\Scripts\python.exe check_local.py

# Also query yesterday's NAV digests and missing OPG amounts for each active test company:
.\.venv\Scripts\python.exe check_local.py --nav
```

The checker reads production workbook metadata, downloads only the test workbook
into memory, and checks whether summary and test output folders are on Shared Drive
and report `canAddChildren`. It creates or updates no Drive files and never invokes
`weekly_invoice_export`. Folder capability checks do not prove an actual upload or
an update of an existing file will succeed.

`--nav` contacts the official NAV endpoint specified by each test company's
`nav_base_url`, which may be production NAV even when output folders are for testing.
It queries one day, retrieves every returned page, and retrieves full invoice data
for OPG rows missing net/VAT amounts through the same fallback as production.
It reports only counts. It does
not save invoice contents or signed XML. An empty successful result verifies access
but does not validate actual invoice amount values. NAV company failures are isolated.

Exit status is zero on success and one on failure. The failing stage, exception type,
and selected safe authentication hints are printed; raw exceptions and configuration
values are suppressed because they can contain credentials or signed request XML.

Useful failure checks:

- `ConfigurationError`: fix YAML syntax or the indicated configuration field.
- `DefaultCredentialsError`: complete the ADC sign-in above.
- `RefreshError` mentioning missing token-creation permission: grant Service Account
  Token Creator on the impersonated account (project selection alone is insufficient).
- Google HTTP 403: check Drive scope, API enablement and resource permissions.
- Google HTTP 404: check the resource ID and whether the service account can access it.

Do not run the full weekly job merely to check connectivity: it appends invoice rows
and uploads a summary to the configured folders.
