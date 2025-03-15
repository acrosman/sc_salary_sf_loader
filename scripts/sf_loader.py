import sqlite3
from simple_salesforce import Salesforce
import getpass
import logging
from datetime import datetime
import sys
from typing import Any
import json
from collections.abc import Iterator

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

    def __init__(self, batch_size: int = 200):
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

    def get_credentials(self) -> tuple:
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
            self.db_conn = sqlite3.connect(db_path)
            self.logger.info(f"Connected to SQLite database: {db_path}")

        except Exception as e:
            self.logger.error(f"Connection error: {str(e)}")
            sys.exit(1)

    def chunk_data(self, data: list[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
        """Split data into chunks for batch processing"""
        for i in range(0, len(data), size):
            yield data[i:i + size]

    def load_persons(self):
        """Load Person records to Salesforce Person Accounts"""
        cursor = self.db_conn.cursor()
        cursor.execute("""
            SELECT ID, FirstName, LastName
            FROM Person
        """)

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

        for batch in self.chunk_data(records, self.batch_size):
            try:
                results = self.sf.bulk.Account.upsert(
                    batch, 'External_Id__pc', batch_size=self.batch_size
                )

                for result in results:
                    if result['success']:
                        success_count += 1
                    else:
                        error_count += 1
                        self.logger.error(f"Error upserting person: {result}")

            except Exception as e:
                self.logger.error(f"Batch error: {str(e)}")
                error_count += len(batch)

        self.logger.info(f"Person Account load complete. Successes: {success_count}, Errors: {error_count}")

    def load_salaries(self):
        """Load Salary records to Salesforce"""
        cursor = self.db_conn.cursor()
        cursor.execute("""
            SELECT ID, PersonID, Title, Employer, Salary, Bonus,
                   TotalPay, EntryDate, SourceFile, LineNumber
            FROM Salary
        """)

        records = []
        for row in cursor.fetchall():
            records.append({
                'External_Id__c': str(row[0]),
                'Person__c': str(row[1]),
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

        for batch in self.chunk_data(records, self.batch_size):
            try:
                results = self.sf.bulk.Salary_History__c.upsert(
                    batch, 'External_Id__c', batch_size=self.batch_size
                )

                for result in results:
                    if result['success']:
                        success_count += 1
                    else:
                        error_count += 1
                        self.logger.error(f"Error upserting salary: {result}")

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
    loader.connect(db_path)

    try:
        loader.load_persons()
        loader.load_salaries()
    except Exception as e:
        loader.logger.error(f"Fatal error: {str(e)}")
    finally:
        loader.close()

if __name__ == "__main__":
    main()
