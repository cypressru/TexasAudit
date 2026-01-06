# TexasAudit

**Government Spending Fraud Detection System for Texas**

TexasAudit is an open-source tool that aggregates Texas state government spending data from multiple sources and applies fraud detection algorithms to identify suspicious patterns, duplicate payments, contract splitting, and vendors on federal exclusion lists.

## Features

- **Multi-Source Data Ingestion**
  - Texas Comptroller payments (data.texas.gov)
  - Centralized Master Bidders List (CMBL)
  - LBB Contract Database
  - USASpending.gov federal grants
  - TxSmartBuy purchase orders
  - SAM.gov federal exclusions (debarred contractors)
  - Texas Ethics Commission campaign finance
  - State employee salary data
  - Sales tax permits

- **Fraud Detection Rules**
  - Contract splitting (clustering payments below thresholds)
  - Duplicate payment detection
  - Vendor name/address clustering
  - Debarred vendor screening (SAM.gov cross-reference)
  - Ghost vendor detection
  - Employee-vendor self-dealing
  - Pay-to-play pattern detection
  - Fiscal year-end spending rush
  - Related party transactions

- **Terminal UI Dashboard**
  - Real-time data visualization
  - Alert management
  - Vendor investigation tools
  - Sync status monitoring

## Quick Start

### One-Line Install (Linux)

```bash
git clone https://github.com/yourusername/texasaudit.git
cd texasaudit
./run.sh
```

The setup script will:
1. Install PostgreSQL if needed
2. Create the database
3. Set up Python virtual environment
4. **Prompt for API keys** (SAM.gov, Socrata)
5. Initialize database tables
6. Launch the terminal UI

### API Keys

TexasAudit works best with these API keys (the setup will prompt you):

| Key | Required | Purpose | Get It |
|-----|----------|---------|--------|
| **SAM.gov API Key** | Recommended | Federal exclusions list | [sam.gov/data-services](https://sam.gov/data-services/) |
| **Socrata App Token** | Optional | Faster data.texas.gov access | [data.texas.gov](https://data.texas.gov/profile/edit/developer_settings) |

Without the SAM.gov key, you can manually download the exclusions ZIP file.

## Usage

### Terminal UI

```bash
./run.sh
# Select option 1 or just press Enter
```

Keyboard shortcuts:
- `d` - Dashboard
- `v` - Vendors
- `a` - Alerts
- `s` - Sync status
- `q` - Quit

### CLI Commands

```bash
# Activate virtual environment
source .venv/bin/activate

# Sync data sources
texasaudit sync run              # Smart sync (skip completed)
texasaudit sync run --all        # Force full sync
texasaudit sync run -s cmbl      # Sync specific source

# Run fraud detection
texasaudit analyze run           # Run all detection rules
texasaudit analyze run -r debarment  # Run specific rule
texasaudit analyze rules         # List all detection rules

# Manage alerts
texasaudit alerts list
texasaudit alerts list --severity high
texasaudit alerts show 123

# Search vendors
texasaudit vendors search "ACME"
texasaudit vendors show 1234567890000

# View configuration
texasaudit config --show
```

## Installation (Manual)

### Requirements

- Python 3.10+
- PostgreSQL 14+
- Linux (tested on Arch, Ubuntu, Fedora)

### Step by Step

```bash
# Clone repository
git clone https://github.com/yourusername/texasaudit.git
cd texasaudit

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e .

# Set up PostgreSQL
sudo systemctl start postgresql
createdb texasaudit

# Copy and edit config
cp config.yaml.example config.yaml
# Edit config.yaml with your settings

# Or use environment variables / .env file
export SAM_API_KEY="your_key"
export TEXASAUDIT_SOCRATA_TOKEN="your_token"

# Initialize database
texasaudit init

# Run sync and analysis
texasaudit sync run
texasaudit analyze run

# Start UI
texasaudit tui
```

## Configuration

Create a `.env` file or set environment variables:

```bash
# API Keys
SAM_API_KEY=your_sam_gov_api_key
TEXASAUDIT_SOCRATA_TOKEN=your_socrata_token

# Database (optional, defaults work for local PostgreSQL)
TEXASAUDIT_DB_HOST=/run/postgresql
TEXASAUDIT_DB_NAME=texasaudit
TEXASAUDIT_DB_USER=your_username
```

Or edit `config.yaml`:

```yaml
database:
  host: /run/postgresql  # Unix socket path or hostname
  port: 5432
  name: texasaudit
  user: your_username

sync:
  sources:
    - cmbl
    - socrata_payments
    - sam_exclusions
    # ... more sources

detection:
  thresholds:
    contract_splitting_min: 45000
    contract_splitting_max: 50000
    vendor_name_similarity: 0.85
```

## Project Structure

```
texasaudit/
├── texasaudit/
│   ├── ingestion/      # Data source ingestors
│   │   ├── cmbl.py           # Centralized Master Bidders List
│   │   ├── socrata.py        # data.texas.gov payments
│   │   ├── sam_exclusions.py # Federal exclusions
│   │   ├── salaries.py       # State employee salaries
│   │   ├── ethics.py         # Campaign finance
│   │   └── ...
│   ├── detection/      # Fraud detection rules
│   │   ├── contract_splitting.py
│   │   ├── debarment.py
│   │   ├── duplicates.py
│   │   ├── employee_vendor.py
│   │   ├── ghost_vendors.py
│   │   └── ...
│   ├── database/       # SQLAlchemy models
│   ├── tui/            # Terminal UI (Textual)
│   ├── normalization/  # Name/address normalization
│   └── cli.py          # Command line interface
├── config.yaml         # Configuration
├── run.sh              # Setup and launch script
└── data/               # Downloaded files and reports
```

## Data Sources

| Source | Records | Update Frequency |
|--------|---------|------------------|
| CMBL (Vendors) | ~55,000 | Daily |
| State Payments | ~2,000,000 | Weekly |
| LBB Contracts | ~15,000 | Weekly |
| SAM.gov Exclusions | ~140,000 | Daily |
| Campaign Finance | Variable | As filed |
| Employee Salaries | ~300,000 | Annually |

## Detection Rules

| Rule | Description | Alert Severity |
|------|-------------|----------------|
| `debarment` | Vendor matches federal exclusion list | HIGH |
| `contract-splitting` | Payments clustered below $50K threshold | MEDIUM-HIGH |
| `duplicate-payments` | Same vendor/amount/date payments | MEDIUM |
| `vendor-clustering` | Related vendors by name/address | LOW-MEDIUM |
| `ghost-vendors` | Payments to vendors not in CMBL | MEDIUM |
| `employee-vendor` | Employee names matching vendor owners | HIGH |
| `pay-to-play` | Campaign contributions correlating with contracts | HIGH |
| `fiscal-year-rush` | Year-end spending spikes | MEDIUM |

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `pytest`
5. Submit a pull request

## Legal Notice

This tool is for research and transparency purposes. Users are responsible for:
- Verifying findings before taking action
- Following applicable laws regarding public records
- Using data ethically and responsibly

False positives are expected - always investigate before drawing conclusions.

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

- Texas Comptroller of Public Accounts
- data.texas.gov (Socrata)
- SAM.gov
- USASpending.gov
- Texas Ethics Commission
