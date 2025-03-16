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
- SQLite database from [SC Salary Data](https://github.com/acrosman/sc_salary_data)
- Salesforce Developer Account with DevHub enabled

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
sf org create scratch -f config/project-scratch-def.json -a sc_salary_loader

# Push the metadata
sf project deploy start

# Open the org
sf org open
```

### 4. Running the Data Loader

1. Ensure you have the SQLite database from the SC Salary Data project
2. Run the loader script:
```bash
python scripts/sf_loader.py
```

3. When prompted:
   - Enter the path to your SQLite database
   - Provide your Salesforce credentials
   - Select the appropriate domain (test/login)

## Project Structure

```
├── config/                     # Salesforce project configuration
├── force-app/                  # Salesforce metadata
│   └── main/default/
│       ├── objects/           # Custom object definitions
│       └── fields/            # Custom field definitions
├── scripts/                   # Python scripts
│   └── sf_loader.py          # Main data loading script
├── requirements.txt           # Python dependencies
└── README.md                 # This file
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
