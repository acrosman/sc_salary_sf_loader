import sqlite3
from simple_salesforce import Salesforce
import getpass
import logging
from datetime import datetime
import sys
from typing import Any
from collections.abc import Iterator
import os

class SalesforceLoader:
    """A class to manage the transfer of salary data from SQLite to Salesforce.

    This class handles the extraction of person and salary data from a SQLite database
    and loads it into Salesforce using Person Accounts and a custom Salary History object.
    It uses batch processing to handle large datasets efficiently and provides logging
    of all operations.

    Attributes:
        sf (Salesforce): Connection to Salesforce instance, initialized during connect()
        db_conn (sqlite3.Connection): Connection to SQLite database, initialized during connect()
        batch_size (int): Number of records to process in each batch, defaults to 200
        logger (logging.Logger): Logger instance for tracking operations

    Example:
        loader = SalesforceLoader()
        loader.connect("path/to/database.db")
        try:
            loader.load_persons()
            loader.load_salaries()
        finally:
            loader.close()
    """

    sf: Salesforce | None
    db_conn: sqlite3.Connection | None
    batch_size: int
    person_account_record_type_id: str | None
    logger: logging.Logger

    def __init__(self, batch_size: int = 10000):
        self.sf = None
        self.db_conn = None
        self.batch_size = batch_size
        self.person_account_record_type_id = None
        self.setup_logging()

    def setup_logging(self):
        """Configure logging to both file and console"""
        self.logger = logging.getLogger('sf_loader')
        self.logger.setLevel(logging.INFO)

        # File handler
        fh = logging.FileHandler(f'sf_loader_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        fh.setLevel(logging.INFO)

        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        # Formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    def get_credentials(self) -> tuple[str, str, str, str]:
        """Interactive credential collection"""
        print("\nEnter Salesforce Credentials:")
        username = input("Username: ")
        password = getpass.getpass("Password: ")
        security_token = getpass.getpass("Security Token: ")
        domain = input("Domain (test/login) [login]: ") or "login"
        return username, password, security_token, domain

    def connect(self, db_path: str):
        """Establish connections to both systems"""
        try:
            # Salesforce connection
            username, password, token, domain = self.get_credentials()
            self.sf = Salesforce(username=username, password=password,
                                security_token=token, domain=domain)
            self.logger.info("Connected to Salesforce successfully")

            # Get Person Account Record Type ID
            query = """
                SELECT Id
                FROM RecordType
                WHERE SObjectType = 'Account'
                AND IsPersonType = true
                AND IsActive = true
                LIMIT 1
            """
            result = self.sf.query(query)
            if result['totalSize'] == 0:
                raise Exception("No active Person Account Record Type found")

            self.person_account_record_type_id = result['records'][0]['Id']
            self.logger.info(f"Found Person Account Record Type: {self.person_account_record_type_id}")

            # SQLite connection
            self.validate_db_path(db_path)
            self.db_conn = sqlite3.connect(db_path)
            self.setup_database_schema()
            self.logger.info(f"Connected to SQLite database: {db_path}")

        except Exception as e:
            self.logger.error(f"Connection error: {str(e)}")
            sys.exit(1)

    def validate_db_path(self, db_path: str) -> bool:
        """Validate the SQLite database path and schema"""
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found: {db_path}")

        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Check for required tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('Person', 'Salary')")
            tables = cursor.fetchall()
            if len(tables) != 2:
                raise ValueError("Database missing required tables")

            return True
        except sqlite3.Error as e:
            raise ValueError(f"Invalid database file: {str(e)}")
        finally:
            if 'conn' in locals():
                conn.close()

    def chunk_data(self, data: list[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
        """Split data into chunks for batch processing"""
        for i in range(0, len(data), size):
            yield data[i:i + size]

    def setup_database_schema(self):
        """Add or reset SFID columns in the database"""
        try:
            cursor = self.db_conn.cursor()

            # Add SFID column to Person table if it doesn't exist
            cursor.execute("""
                SELECT COUNT(*)
                FROM pragma_table_info('Person')
                WHERE name='SFID'
            """)
            if cursor.fetchone()[0] == 0:
                cursor.execute("ALTER TABLE Person ADD COLUMN SFID TEXT")
            else:
                cursor.execute("UPDATE Person SET SFID = NULL")

            # Add SFID column to Salary table if it doesn't exist
            cursor.execute("""
                SELECT COUNT(*)
                FROM pragma_table_info('Salary')
                WHERE name='SFID'
            """)
            if cursor.fetchone()[0] == 0:
                cursor.execute("ALTER TABLE Salary ADD COLUMN SFID TEXT")
            else:
                cursor.execute("UPDATE Salary SET SFID = NULL")

            self.db_conn.commit()
            self.logger.info("Database schema updated successfully")

        except sqlite3.Error as e:
            self.logger.error(f"Database schema update failed: {str(e)}")
            raise

    def load_persons(self, limit: int = None):
        """Load Person records to Salesforce Person Accounts

        Args:
            limit (int, optional): Maximum number of records to process. Defaults to None (all records).
        """
        cursor = self.db_conn.cursor()
        query = """
            SELECT ID, FirstName, LastName
            FROM Person
            WHERE SFID IS NULL
        """
        if limit:
            query += f" LIMIT {limit}"
        cursor.execute(query)

        records = []
        for row in cursor.fetchall():
            records.append({
                'RecordTypeId': self.person_account_record_type_id,
                'FirstName': row[1],
                'LastName': row[2],
                'External_Id__pc': str(row[0])
            })

        success_count = 0
        error_count = 0

        total_records = len(records)
        for i, batch in enumerate(self.chunk_data(records, self.batch_size)):
            processed = min((i + 1) * self.batch_size, total_records)
            percentage = (processed / total_records) * 100
            print(f"Processing batch {i+1}: {processed}/{total_records} records ({percentage:.1f}%)")

            try:
                results = self.sf.bulk.Account.insert(batch)

                # Update SQLite with Salesforce IDs
                update_cursor = self.db_conn.cursor()
                for record, result in zip(batch, results):
                    if result['success']:
                        success_count += 1
                        update_cursor.execute(
                            "UPDATE Person SET SFID = ? WHERE ID = ?",
                            (result['id'], record['External_Id__pc'])
                        )
                    else:
                        error_count += 1
                        self.logger.error(f"Error inserting person: {result}")

                self.db_conn.commit()

            except Exception as e:
                self.logger.error(f"Batch error: {str(e)}")
                error_count += len(batch)

        self.logger.info(f"Person Account load complete. Successes: {success_count}, Errors: {error_count}")

    def load_salaries(self):
        """Load Salary records to Salesforce"""
        cursor = self.db_conn.cursor()
        cursor.execute("""
            SELECT s.ID, p.SFID as PersonSFID, s.Title, s.Employer, s.Salary, s.Bonus,
                   s.TotalPay, s.EntryDate, s.SourceFile, s.LineNumber
            FROM Salary s
            INNER JOIN Person p ON s.PersonID = p.ID
            WHERE s.SFID IS NULL
            AND p.SFID IS NOT NULL
        """)

        records = []
        for row in cursor.fetchall():
            records.append({
                'External_Id__c': str(row[0]),
                'Person__c': row[1],  # Now using Person's SFID directly
                'Title__c': row[2],
                'Employer__c': row[3],
                'Salary__c': row[4],
                'Bonus__c': row[5],
                'TotalPay__c': row[6],
                'EntryDate__c': row[7],
                'SourceFile__c': row[8],
                'LineNumber__c': row[9]
            })

        success_count = 0
        error_count = 0

        total_records = len(records)
        for i, batch in enumerate(self.chunk_data(records, self.batch_size)):
            processed = min((i + 1) * self.batch_size, total_records)
            percentage = (processed / total_records) * 100
            print(f"Processing salary batch {i+1}: {processed}/{total_records} records ({percentage:.1f}%)")

            try:
                results = self.sf.bulk.Salary_History__c.insert(batch)

                # Update SQLite with Salesforce IDs
                update_cursor = self.db_conn.cursor()
                for record, result in zip(batch, results):
                    if result['success']:
                        success_count += 1
                        update_cursor.execute(
                            "UPDATE Salary SET SFID = ? WHERE ID = ?",
                            (result['id'], record['External_Id__c'])
                        )
                    else:
                        error_count += 1
                        self.logger.error(f"Error inserting salary: {result}")

                self.db_conn.commit()

            except Exception as e:
                self.logger.error(f"Batch error: {str(e)}")
                error_count += len(batch)

        self.logger.info(f"Salary History load complete. Successes: {success_count}, Errors: {error_count}")

    def close(self):
        """Clean up connections"""
        if self.db_conn:
            self.db_conn.close()

def main():
    loader = SalesforceLoader()
    db_path = input("Enter the path to your SQLite database: ")

    limit_input = input("Enter maximum number of person records to load (press Enter for all): ").strip()
    record_limit = int(limit_input) if limit_input else None

    loader.connect(db_path)

    try:
        loader.load_persons(record_limit)
        print("\nAll person records have been loaded. Beginning salary records...")
        loader.load_salaries()
    except Exception as e:
        loader.logger.error(f"Fatal error: {str(e)}")
    finally:
        loader.close()

if __name__ == "__main__":
    main()
