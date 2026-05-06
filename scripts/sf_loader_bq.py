"""Salesforce loader that reads from Google BigQuery and authenticates via OAuth2 JWT Bearer flow.

This script is a parallel version of sf_loader.py that replaces:
  - SQLite database  →  Google BigQuery dataset
  - Username/password Salesforce auth  →  OAuth2 JWT Bearer Token flow

All credentials are loaded from environment variables (or a .env file).
See .env.example for the full list of required settings.
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any
from collections.abc import Iterator

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
import jwt
import requests
from simple_salesforce import Salesforce

# Load .env file if present (does nothing when vars are already set in the environment)
load_dotenv()


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------

def _require_env(name: str) -> str:
    """Return the value of an environment variable or raise an error."""
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{name}' is not set. "
            "See .env.example for setup instructions."
        )
    return value


def _load_private_key(path: str):
    """Load an RSA private key from a PEM file."""
    with open(path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend(),
        )
    return private_key


# ---------------------------------------------------------------------------
# Salesforce OAuth2 — JWT Bearer Token flow
# ---------------------------------------------------------------------------

def get_salesforce_token(
    consumer_key: str,
    private_key_path: str,
    username: str,
    domain: str = "login",
) -> tuple[str, str]:
    """Obtain a Salesforce access token via the OAuth2 JWT Bearer Token flow.

    Args:
        consumer_key: Connected App Consumer Key (client_id).
        private_key_path: Path to the RSA private key PEM file.
        username: Salesforce username to authenticate as.
        domain: 'login' (production) or 'test' (sandbox).

    Returns:
        Tuple of (access_token, instance_url).
    """
    audience = f"https://{domain}.salesforce.com"
    token_url = f"{audience}/services/oauth2/token"

    now = int(time.time())
    payload = {
        "iss": consumer_key,
        "sub": username,
        "aud": audience,
        "exp": now + 300,  # 5-minute expiry
    }

    private_key = _load_private_key(private_key_path)
    # Export to PEM bytes for jwt.encode
    pem_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )

    assertion = jwt.encode(payload, pem_bytes, algorithm="RS256")

    response = requests.post(
        token_url,
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": assertion,
        },
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    return data["access_token"], data["instance_url"]


# ---------------------------------------------------------------------------
# Main loader class
# ---------------------------------------------------------------------------

class SalesforceBigQueryLoader:
    """Transfer salary data from Google BigQuery to Salesforce.

    Credentials and settings are read from environment variables (or a .env
    file).  See .env.example for the complete list.

    Required environment variables
    --------------------------------
    SALESFORCE_CONSUMER_KEY   – Connected App Consumer Key
    SALESFORCE_PRIVATE_KEY_PATH – Path to RSA private key PEM file
    SALESFORCE_USERNAME       – Salesforce username
    SALESFORCE_DOMAIN         – 'login' (production) or 'test' (sandbox)
    BQ_PROJECT_ID             – Google Cloud project that owns the dataset
    BQ_DATASET_ID             – BigQuery dataset containing Person and Salary tables
    BQ_CREDENTIALS_FILE       – Path to the service account JSON key file
                                 (or set GOOGLE_APPLICATION_CREDENTIALS instead)

    Example:
        loader = SalesforceBigQueryLoader()
        loader.connect()
        try:
            loader.load_persons()
            loader.load_salaries()
        finally:
            loader.close()
    """

    sf: Salesforce | None
    bq_client: bigquery.Client | None
    batch_size: int
    person_account_record_type_id: str | None
    logger: logging.Logger

    # BigQuery dataset reference (set during connect)
    _bq_project: str
    _bq_dataset: str

    def __init__(self, batch_size: int = 10000):
        self.sf = None
        self.bq_client = None
        self.batch_size = batch_size
        self.person_account_record_type_id = None
        self._bq_project = ""
        self._bq_dataset = ""
        self.setup_logging()

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def setup_logging(self):
        """Configure logging to both file and console."""
        self.logger = logging.getLogger("sf_loader_bq")
        self.logger.setLevel(logging.INFO)

        fh = logging.FileHandler(
            f"sf_loader_bq_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        fh.setLevel(logging.INFO)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def connect(self):
        """Establish connections to Salesforce (OAuth2) and BigQuery."""
        try:
            self._connect_salesforce()
            self._connect_bigquery()
        except Exception as e:
            self.logger.error(f"Connection error: {str(e)}")
            sys.exit(1)

    def _connect_salesforce(self):
        """Authenticate with Salesforce using OAuth2 JWT Bearer flow."""
        consumer_key = _require_env("SALESFORCE_CONSUMER_KEY")
        private_key_path = _require_env("SALESFORCE_PRIVATE_KEY_PATH")
        username = _require_env("SALESFORCE_USERNAME")
        domain = os.environ.get("SALESFORCE_DOMAIN", "login")

        if not os.path.exists(private_key_path):
            raise FileNotFoundError(
                f"Salesforce private key not found: {private_key_path}"
            )

        self.logger.info("Authenticating with Salesforce via OAuth2 JWT Bearer flow…")
        access_token, instance_url = get_salesforce_token(
            consumer_key, private_key_path, username, domain
        )

        self.sf = Salesforce(instance_url=instance_url, session_id=access_token)
        self.logger.info("Connected to Salesforce successfully")

        # Resolve Person Account Record Type ID
        result = self.sf.query(
            """
            SELECT Id
            FROM RecordType
            WHERE SObjectType = 'Account'
            AND IsPersonType = true
            AND IsActive = true
            LIMIT 1
            """
        )
        if result["totalSize"] == 0:
            raise RuntimeError("No active Person Account Record Type found")

        self.person_account_record_type_id = result["records"][0]["Id"]
        self.logger.info(
            f"Found Person Account Record Type: {self.person_account_record_type_id}"
        )

    def _connect_bigquery(self):
        """Create an authenticated BigQuery client."""
        self._bq_project = _require_env("BQ_PROJECT_ID")
        self._bq_dataset = _require_env("BQ_DATASET_ID")

        # Accept either a custom var or the standard ADC env var
        credentials_file = os.environ.get(
            "BQ_CREDENTIALS_FILE",
            os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        )

        if credentials_file:
            if not os.path.exists(credentials_file):
                raise FileNotFoundError(
                    f"BigQuery credentials file not found: {credentials_file}"
                )
            credentials = service_account.Credentials.from_service_account_file(
                credentials_file,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            self.bq_client = bigquery.Client(
                project=self._bq_project, credentials=credentials
            )
        else:
            # Fall back to Application Default Credentials
            self.bq_client = bigquery.Client(project=self._bq_project)

        self.logger.info(
            f"Connected to BigQuery project '{self._bq_project}', "
            f"dataset '{self._bq_dataset}'"
        )

        self._ensure_sfid_columns()

    # ------------------------------------------------------------------
    # BigQuery schema helpers
    # ------------------------------------------------------------------

    def _table_ref(self, table_name: str) -> str:
        """Return a fully-qualified BigQuery table reference."""
        return f"`{self._bq_project}.{self._bq_dataset}.{table_name}`"

    def _column_exists(self, table_name: str, column_name: str) -> bool:
        """Return True if *column_name* exists in *table_name*."""
        table = self.bq_client.get_table(
            f"{self._bq_project}.{self._bq_dataset}.{table_name}"
        )
        return any(field.name.upper() == column_name.upper() for field in table.schema)

    def _ensure_sfid_columns(self):
        """Add SFID columns to Person and Salary tables if they are missing."""
        for table_name in ("Person", "Salary"):
            if not self._column_exists(table_name, "SFID"):
                table_ref = f"{self._bq_project}.{self._bq_dataset}.{table_name}"
                table = self.bq_client.get_table(table_ref)
                new_schema = list(table.schema) + [
                    bigquery.SchemaField("SFID", "STRING", mode="NULLABLE")
                ]
                table.schema = new_schema
                self.bq_client.update_table(table, ["schema"])
                self.logger.info(f"Added SFID column to {table_name} table")
            else:
                # Reset existing SFIDs so the run starts fresh
                self.bq_client.query(
                    f"UPDATE {self._table_ref(table_name)} SET SFID = NULL WHERE TRUE"
                ).result()

        self.logger.info("BigQuery schema ready")

    # ------------------------------------------------------------------
    # Batch helpers
    # ------------------------------------------------------------------

    def chunk_data(
        self, data: list[dict[str, Any]], size: int
    ) -> Iterator[list[dict[str, Any]]]:
        """Split *data* into chunks of at most *size* items."""
        for i in range(0, len(data), size):
            yield data[i : i + size]

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def load_persons(self, limit: int | None = None):
        """Load Person rows from BigQuery into Salesforce Person Accounts.

        Args:
            limit: Maximum number of records to process (None = all records).
        """
        sql = (
            f"SELECT ID, FirstName, LastName "
            f"FROM {self._table_ref('Person')} "
            f"WHERE SFID IS NULL"
        )
        if limit:
            sql += f" LIMIT {limit}"

        query_job = self.bq_client.query(sql)
        rows = list(query_job.result())

        records = [
            {
                "RecordTypeId": self.person_account_record_type_id,
                "FirstName": row.FirstName,
                "LastName": row.LastName,
                "External_Id__pc": str(row.ID),
            }
            for row in rows
        ]

        success_count = 0
        error_count = 0
        total_records = len(records)

        for i, batch in enumerate(self.chunk_data(records, self.batch_size)):
            processed = min((i + 1) * self.batch_size, total_records)
            percentage = (processed / total_records) * 100 if total_records else 0
            print(
                f"Processing person batch {i+1}: "
                f"{processed}/{total_records} records ({percentage:.1f}%)"
            )

            try:
                results = self.sf.bulk.Account.insert(batch)

                # Write Salesforce IDs back to BigQuery
                updates = []
                for record, result in zip(batch, results):
                    if result["success"]:
                        success_count += 1
                        updates.append(
                            {"sfid": result["id"], "ext_id": record["External_Id__pc"]}
                        )
                    else:
                        error_count += 1
                        self.logger.error(f"Error inserting person: {result}")

                if updates:
                    self._update_sfids("Person", "ID", updates)

            except Exception as e:
                self.logger.error(f"Batch error: {str(e)}")
                error_count += len(batch)

        self.logger.info(
            f"Person Account load complete. "
            f"Successes: {success_count}, Errors: {error_count}"
        )

    def load_salaries(self):
        """Load Salary rows from BigQuery into Salesforce Salary_History__c records."""
        sql = f"""
            SELECT s.ID, p.SFID AS PersonSFID, s.Title, s.Employer, s.Salary,
                   s.Bonus, s.TotalPay, s.EntryDate, s.SourceFile, s.LineNumber
            FROM {self._table_ref('Salary')} s
            INNER JOIN {self._table_ref('Person')} p ON s.PersonID = p.ID
            WHERE s.SFID IS NULL
            AND p.SFID IS NOT NULL
        """

        query_job = self.bq_client.query(sql)
        rows = list(query_job.result())

        records = [
            {
                "External_Id__c": str(row.ID),
                "Person__c": row.PersonSFID,
                "Title__c": row.Title,
                "Employer__c": row.Employer,
                "Salary__c": row.Salary,
                "Bonus__c": row.Bonus,
                "TotalPay__c": row.TotalPay,
                "EntryDate__c": str(row.EntryDate) if row.EntryDate else None,
                "SourceFile__c": row.SourceFile,
                "LineNumber__c": row.LineNumber,
            }
            for row in rows
        ]

        success_count = 0
        error_count = 0
        total_records = len(records)

        for i, batch in enumerate(self.chunk_data(records, self.batch_size)):
            processed = min((i + 1) * self.batch_size, total_records)
            percentage = (processed / total_records) * 100 if total_records else 0
            print(
                f"Processing salary batch {i+1}: "
                f"{processed}/{total_records} records ({percentage:.1f}%)"
            )

            try:
                results = self.sf.bulk.Salary_History__c.insert(batch)

                updates = []
                for record, result in zip(batch, results):
                    if result["success"]:
                        success_count += 1
                        updates.append(
                            {"sfid": result["id"], "ext_id": record["External_Id__c"]}
                        )
                    else:
                        error_count += 1
                        self.logger.error(f"Error inserting salary: {result}")

                if updates:
                    self._update_sfids("Salary", "ID", updates)

            except Exception as e:
                self.logger.error(f"Batch error: {str(e)}")
                error_count += len(batch)

        self.logger.info(
            f"Salary History load complete. "
            f"Successes: {success_count}, Errors: {error_count}"
        )

    def _update_sfids(
        self, table_name: str, id_column: str, updates: list[dict[str, str]]
    ):
        """Write Salesforce IDs back to a BigQuery table using DML UPDATE.

        Each item in *updates* must have keys ``sfid`` and ``ext_id``.
        """
        if not updates:
            return

        # Build a VALUES clause for a MERGE/UPDATE via a subquery
        value_rows = ", ".join(
            f"('{u['sfid']}', '{u['ext_id']}')" for u in updates
        )
        sql = f"""
            UPDATE {self._table_ref(table_name)} AS t
            SET t.SFID = src.sfid
            FROM (
                SELECT sfid, ext_id
                FROM UNNEST([
                    STRUCT<sfid STRING, ext_id STRING>
                    {value_rows}
                ])
            ) AS src
            WHERE CAST(t.{id_column} AS STRING) = src.ext_id
        """
        self.bq_client.query(sql).result()

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close(self):
        """Release any open connections."""
        if self.bq_client:
            self.bq_client.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    loader = SalesforceBigQueryLoader()

    limit_input = input(
        "Enter maximum number of person records to load (press Enter for all): "
    ).strip()
    record_limit = int(limit_input) if limit_input else None

    loader.connect()

    try:
        loader.load_persons(record_limit)
        print("\nAll person records have been loaded. Beginning salary records…")
        loader.load_salaries()
    except Exception as e:
        loader.logger.error(f"Fatal error: {str(e)}")
    finally:
        loader.close()


if __name__ == "__main__":
    main()
