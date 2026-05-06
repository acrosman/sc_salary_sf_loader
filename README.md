# South Carolina Salary Data Salesforce Loader

This project provides tools to load South Carolina state employee salary data into Salesforce. The data source is the [SC Salary Data](https://github.com/acrosman/sc_salary_data) project, which processes and stores South Carolina state employee salary data in a SQLite database.

## Project Overview

This application:
- Loads person records as Person Accounts in Salesforce
- Creates salary history records for each person
- Uses Salesforce Bulk API for efficient data loading
- Supports both test and production environments
- Includes proper error handling and logging

## Prerequisites

- Python 3.11 or higher
- Salesforce CLI
- Visual Studio Code with Salesforce Extension Pack
- Salesforce Developer Account with DevHub enabled
- **For `sf_loader.py`:** SQLite database from [SC Salary Data](https://github.com/acrosman/sc_salary_data)
- **For `sf_loader_bq.py`:** Google Cloud project with BigQuery dataset and a service account

## Setup Instructions

### 1. Python Environment Setup

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate

# Install required packages
pip install -r requirements.txt
```

### 2. Salesforce DevHub Setup

1. Enable DevHub in your Salesforce org:
   - Log into your Salesforce org
   - Setup → Dev Hub → Enable DevHub

2. Authorize your DevHub:
```bash
sf org login web --set-default-dev-hub
```

### 3. Create a Scratch Org

```bash
# Create the scratch org
sf org create scratch -f config/project-scratch-def.json -a sc_salary_loader --duration-days 7

# Push the metadata
sf project deploy start --target-org sc_salary_loader

# Assign permission set to the default user
sf org assign permset --name Migrate_Data --target-org sc_salary_loader

# Open the org
sf org open --target-org sc_salary_loader
```

### 4. Running the SQLite Loader (`sf_loader.py`)

1. Ensure you have the SQLite database from the SC Salary Data project
2. Run the loader script:
```bash
python scripts/sf_loader.py
```

3. When prompted:
   - Enter the path to your SQLite database
   - Provide your Salesforce credentials
   - Select the appropriate domain (test/login)

---

### 5. Running the BigQuery + OAuth2 Loader (`sf_loader_bq.py`)

This loader reads data from a Google BigQuery dataset and authenticates with
Salesforce using the **OAuth2 JWT Bearer Token** flow — no username/password
prompts at runtime.

#### 5a. Salesforce Connected App Setup

1. In Salesforce Setup, navigate to **App Manager → New Connected App**.
2. Enable **OAuth Settings** and add the scope `api` (or `full`).
3. Enable **Use digital signatures** and upload your RSA public key (see below).
4. Save the app and copy the **Consumer Key**.
5. In **Manage Connected Apps**, set the app's OAuth policies to
   *Admin approved users are pre-authorised* and add the relevant profiles or
   permission sets.

#### 5b. Generate an RSA Key Pair

```bash
# Generate a 2048-bit private key (keep this file secret!)
openssl genrsa -out sf_private_key.pem 2048

# Export the matching public key to upload to the Connected App
openssl rsa -in sf_private_key.pem -pubout -out sf_public_key.pem
```

#### 5c. Google Cloud Setup

1. Create (or identify) a **BigQuery dataset** that contains `Person` and
   `Salary` tables matching the schema from the
   [SC Salary Data](https://github.com/acrosman/sc_salary_data) project.
2. Create a **service account** with the `BigQuery Data Editor` and
   `BigQuery Job User` roles on the project.
3. Download a **JSON key** for the service account.  Use
   `bq_service_account.json.example` as a reference for the expected file
   structure:
```bash
cp bq_service_account.json.example bq_service_account.json
# Replace placeholder values with the real values from Google Cloud Console
```

#### 5d. Configure Environment Variables

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
# Edit .env with your credentials
```

The variables are:

| Variable | Description |
|---|---|
| `SALESFORCE_CONSUMER_KEY` | Connected App Consumer Key |
| `SALESFORCE_PRIVATE_KEY_PATH` | Path to `sf_private_key.pem` |
| `SALESFORCE_USERNAME` | Salesforce username (pre-authorised for the app) |
| `SALESFORCE_DOMAIN` | `login` (production) or `test` (sandbox) |
| `BQ_PROJECT_ID` | Google Cloud project ID |
| `BQ_DATASET_ID` | BigQuery dataset ID |
| `BQ_CREDENTIALS_FILE` | Path to the service account JSON key file (see `bq_service_account.json.example`) |

> **Security note:** The `.env` file, `*.pem` key files, and
> `*_service_account.json` files are all listed in `.gitignore` and must
> **never** be committed to source control.

#### 5e. Run the BigQuery Loader

```bash
python scripts/sf_loader_bq.py
```

When prompted, optionally enter a maximum number of person records to load
(press **Enter** to load all records).

---

## Project Structure

```
├── config/                          # Salesforce project configuration
├── force-app/                       # Salesforce metadata
│   └── main/default/
│       └── objects/                # Custom object definitions
│           └── fields/             # Custom field definitions
├── scripts/                         # Python scripts
│   ├── sf_loader.py                 # SQLite + username/password loader
│   └── sf_loader_bq.py              # BigQuery + OAuth2 JWT Bearer loader
├── .env.example                     # Template for credential environment variables
├── bq_service_account.json.example  # Template for Google Cloud service account key
├── requirements.txt                 # Python dependencies
└── README.md                        # This file
```

## Error Handling

The loader will:
- Log all errors to a timestamped file
- Continue processing on non-fatal errors
- Provide summary statistics after completion

## Contributing

1. Fork the repository
2. Create a feature branch
3. Submit a Pull Request

## License

This project is licensed under the BSD 3-Clause License - see the LICENSE file for details.

## Support

For issues with:
- Data loading: Open an issue in this repository
- Source data: Visit [SC Salary Data](https://github.com/acrosman/sc_salary_data)
- Salesforce setup: Check [Salesforce Documentation](https://developer.salesforce.com)
