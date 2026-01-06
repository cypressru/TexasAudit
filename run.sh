#!/bin/bash
# Texas Audit - Startup Script
# Handles PostgreSQL, database setup, API keys, and launching the application

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

echo -e "${CYAN}"
echo "╔════════════════════════════════════════════════════════════╗"
echo "║                    TEXAS AUDIT                             ║"
echo "║        Government Spending Fraud Detection System          ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# =============================================================================
# API KEY CONFIGURATION
# =============================================================================

ENV_FILE="$SCRIPT_DIR/.env"
CONFIG_FILE="$SCRIPT_DIR/config.yaml"

# Load existing .env if present
if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
fi

# Function to prompt for API key with instructions
prompt_api_key() {
    local key_name="$1"
    local env_var="$2"
    local current_value="$3"
    local instructions="$4"
    local url="$5"
    local required="$6"

    if [ -n "$current_value" ] && [ "$current_value" != "null" ] && [ "$current_value" != "" ]; then
        echo -e "${GREEN}$key_name: [Already configured]${NC}"
        return 0
    fi

    echo ""
    echo -e "${BOLD}$key_name${NC}"
    echo -e "${CYAN}$instructions${NC}"
    if [ -n "$url" ]; then
        echo -e "  ${YELLOW}$url${NC}"
    fi
    echo ""

    if [ "$required" = "required" ]; then
        echo -e "  ${RED}(Required for automatic data sync)${NC}"
    else
        echo -e "  ${YELLOW}(Optional - improves rate limits)${NC}"
    fi

    read -p "  Enter API key (or press Enter to skip): " api_key

    if [ -n "$api_key" ]; then
        # Save to .env file
        if grep -q "^$env_var=" "$ENV_FILE" 2>/dev/null; then
            sed -i "s|^$env_var=.*|$env_var=$api_key|" "$ENV_FILE"
        else
            echo "$env_var=$api_key" >> "$ENV_FILE"
        fi
        export "$env_var=$api_key"
        echo -e "  ${GREEN}Saved!${NC}"
        return 0
    else
        echo -e "  ${YELLOW}Skipped${NC}"
        return 1
    fi
}

# Function to check and configure API keys
configure_api_keys() {
    echo -e "\n${CYAN}[API Configuration]${NC}"
    echo "TexasAudit can collect data from multiple sources."
    echo "Some sources require API keys for full functionality."
    echo ""

    # Create .env file if it doesn't exist
    touch "$ENV_FILE"

    # Check each API key
    local all_configured=true

    # 1. Socrata (data.texas.gov) App Token
    local socrata_token="${TEXASAUDIT_SOCRATA_TOKEN:-}"
    if [ -z "$socrata_token" ]; then
        # Check config.yaml
        socrata_token=$(grep -A1 "api_keys:" "$CONFIG_FILE" 2>/dev/null | grep "socrata:" | sed 's/.*socrata: *"\([^"]*\)".*/\1/' | head -1)
    fi

    prompt_api_key \
        "Data.Texas.Gov (Socrata) App Token" \
        "TEXASAUDIT_SOCRATA_TOKEN" \
        "$socrata_token" \
        "Increases rate limits for state spending data downloads." \
        "Get one at: https://data.texas.gov/profile/edit/developer_settings" \
        "optional"

    # 2. SAM.gov API Key
    local sam_key="${SAM_API_KEY:-}"

    prompt_api_key \
        "SAM.gov API Key" \
        "SAM_API_KEY" \
        "$sam_key" \
        "Required to download federal exclusions (debarred contractors)." \
        "Register at: https://sam.gov/data-services/" \
        "required"

    if [ -z "$sam_key" ] && [ -z "${SAM_API_KEY:-}" ]; then
        echo ""
        echo -e "${YELLOW}Note: Without SAM.gov API key, you can still manually download:${NC}"
        echo "  1. Go to https://sam.gov/data-services/"
        echo "  2. Download 'SAM_Exclusions_Public_Extract_V2.ZIP'"
        echo "  3. Place it in: $SCRIPT_DIR/data/"
    fi

    echo ""
}

# =============================================================================
# POSTGRESQL FUNCTIONS
# =============================================================================

check_postgres_installed() {
    if command -v initdb &> /dev/null || [ -f /usr/bin/initdb ]; then
        return 0
    else
        return 1
    fi
}

check_postgres_initialized() {
    if sudo test -d "/var/lib/postgres/data/base"; then
        return 0
    else
        return 1
    fi
}

check_postgres_running() {
    if systemctl is-active --quiet postgresql; then
        return 0
    else
        return 1
    fi
}

init_postgres() {
    echo -e "${YELLOW}Initializing PostgreSQL database cluster...${NC}"

    INITDB=$(which initdb 2>/dev/null || echo "/usr/bin/initdb")

    if ! id "postgres" &>/dev/null; then
        echo -e "${YELLOW}Creating postgres system user...${NC}"
        sudo useradd -r -m -d /var/lib/postgres -s /bin/bash postgres
    fi

    if sudo test -d "/var/lib/postgres/data" && ! sudo test -d "/var/lib/postgres/data/base"; then
        echo -e "${YELLOW}Cleaning incomplete data directory...${NC}"
        sudo rm -rf /var/lib/postgres/data
    fi

    sudo mkdir -p /var/lib/postgres/data
    sudo chown postgres:postgres /var/lib/postgres/data
    sudo -u postgres "$INITDB" -D /var/lib/postgres/data

    echo -e "${GREEN}PostgreSQL initialized${NC}"
}

install_postgres() {
    echo -e "${YELLOW}Installing PostgreSQL...${NC}"

    # Detect package manager
    if command -v pacman &> /dev/null; then
        sudo pacman -S --noconfirm postgresql
    elif command -v apt-get &> /dev/null; then
        sudo apt-get update
        sudo apt-get install -y postgresql postgresql-contrib
    elif command -v dnf &> /dev/null; then
        sudo dnf install -y postgresql-server postgresql-contrib
        sudo postgresql-setup --initdb
    elif command -v yum &> /dev/null; then
        sudo yum install -y postgresql-server postgresql-contrib
        sudo postgresql-setup initdb
    else
        echo -e "${RED}Could not detect package manager. Please install PostgreSQL manually.${NC}"
        exit 1
    fi

    init_postgres

    echo -e "${YELLOW}Starting PostgreSQL service...${NC}"
    sudo systemctl enable postgresql
    sudo systemctl start postgresql
    sleep 2

    echo -e "${GREEN}PostgreSQL installed and started${NC}"
}

start_postgres() {
    echo -e "${YELLOW}Starting PostgreSQL...${NC}"
    sudo systemctl start postgresql
    sleep 2

    if check_postgres_running; then
        echo -e "${GREEN}PostgreSQL started successfully${NC}"
    else
        echo -e "${RED}Failed to start PostgreSQL${NC}"
        echo "Try: sudo systemctl status postgresql"
        exit 1
    fi
}

# =============================================================================
# MAIN SETUP
# =============================================================================

# Check if this is first run
FIRST_RUN=false
if [ ! -f "$ENV_FILE" ] && [ ! -d "$SCRIPT_DIR/.venv" ]; then
    FIRST_RUN=true
    echo -e "${YELLOW}First time setup detected!${NC}"
    echo ""
fi

# Step 1: Check/Install/Start PostgreSQL
echo -e "\n${CYAN}[1/6] Checking PostgreSQL...${NC}"

if ! check_postgres_installed; then
    echo -e "${YELLOW}PostgreSQL is not installed${NC}"
    read -p "Install PostgreSQL now? [Y/n]: " install_choice
    install_choice=${install_choice:-Y}

    if [[ "$install_choice" =~ ^[Yy]$ ]]; then
        install_postgres
    else
        echo -e "${RED}PostgreSQL is required. Please install it manually.${NC}"
        exit 1
    fi
elif ! check_postgres_initialized; then
    echo -e "${YELLOW}PostgreSQL is installed but not initialized${NC}"
    init_postgres
    start_postgres
elif check_postgres_running; then
    echo -e "${GREEN}PostgreSQL is running${NC}"
else
    echo -e "${YELLOW}PostgreSQL is installed but not running${NC}"
    start_postgres
fi

# Step 2: Create database and user role if needed
echo -e "\n${CYAN}[2/6] Checking database...${NC}"

CURRENT_USER=$(whoami)
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='$CURRENT_USER'" | grep -q 1; then
    echo -e "${YELLOW}Creating PostgreSQL role for '$CURRENT_USER'...${NC}"
    sudo -u postgres createuser -s "$CURRENT_USER"
    echo -e "${GREEN}Role created${NC}"
fi

if psql -lqt 2>/dev/null | cut -d \| -f 1 | grep -qw texasaudit; then
    echo -e "${GREEN}Database 'texasaudit' exists${NC}"
else
    echo -e "${YELLOW}Creating database 'texasaudit'...${NC}"
    createdb texasaudit
    echo -e "${GREEN}Database created${NC}"
fi

sed -i "s/user: postgres/user: $CURRENT_USER/" "$SCRIPT_DIR/config.yaml" 2>/dev/null || true

# Step 3: Check/Install Python dependencies
echo -e "\n${CYAN}[3/6] Checking Python virtual environment...${NC}"

VENV_DIR="$SCRIPT_DIR/.venv"

if [ ! -d "$VENV_DIR" ]; then
    echo -e "${YELLOW}Creating Python virtual environment...${NC}"
    python -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
echo -e "${GREEN}Virtual environment active${NC}"

if python -c "import flask, sqlalchemy, sodapy, click" 2>/dev/null; then
    echo -e "${GREEN}Dependencies already installed${NC}"
else
    echo -e "${YELLOW}Installing dependencies...${NC}"
    pip install -e . --quiet
    echo -e "${GREEN}Dependencies installed${NC}"
fi

# Step 4: Configure API keys (on first run or if requested)
echo -e "\n${CYAN}[4/6] Checking API configuration...${NC}"

# Load .env if it exists
if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
fi

# Check if keys are configured
SOCRATA_OK=false
SAM_OK=false

if [ -n "${TEXASAUDIT_SOCRATA_TOKEN:-}" ]; then
    SOCRATA_OK=true
fi

if [ -n "${SAM_API_KEY:-}" ]; then
    SAM_OK=true
fi

# Also check for manual SAM download
SAM_MANUAL=false
if [ -f "$SCRIPT_DIR/data/SAM_Exclusions_Public_Extract_V2.ZIP" ] || [ -f "$SCRIPT_DIR/data/SAM_Exclusions.csv" ]; then
    SAM_MANUAL=true
fi

if [ "$FIRST_RUN" = true ]; then
    configure_api_keys
elif [ "$SOCRATA_OK" = false ] || ([ "$SAM_OK" = false ] && [ "$SAM_MANUAL" = false ]); then
    echo -e "${YELLOW}Some API keys are not configured.${NC}"
    read -p "Configure now? [Y/n]: " config_choice
    config_choice=${config_choice:-Y}

    if [[ "$config_choice" =~ ^[Yy]$ ]]; then
        configure_api_keys
    fi
else
    echo -e "${GREEN}API keys configured${NC}"
fi

# Create data directory if needed
mkdir -p "$SCRIPT_DIR/data/reports"

# Step 5: Initialize database tables
echo -e "\n${CYAN}[5/6] Initializing database tables...${NC}"

TABLE_COUNT=$(psql -d texasaudit -tAc "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>/dev/null || echo "0")
EXPECTED_TABLES=15  # Updated count with debarred_entities

if [ "$TABLE_COUNT" -ge "$EXPECTED_TABLES" ]; then
    echo -e "${GREEN}All database tables exist ($TABLE_COUNT tables)${NC}"
else
    echo -e "${YELLOW}Creating/updating tables ($TABLE_COUNT -> $EXPECTED_TABLES)...${NC}"
    python -c "from texasaudit.database import init_db; init_db()"
    NEW_COUNT=$(psql -d texasaudit -tAc "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>/dev/null || echo "0")
    echo -e "${GREEN}Tables ready ($NEW_COUNT tables)${NC}"
fi

# =============================================================================
# LAUNCH OPTIONS
# =============================================================================

smart_sync() {
    echo -e "${CYAN}Running parallel sync with live progress...${NC}"
    echo ""
    python -c "from texasaudit.ingestion.runner import run_sync; run_sync(smart=True)"
}

parallel_full_sync() {
    echo -e "${CYAN}Running full parallel sync with live progress...${NC}"
    echo ""
    python -c "from texasaudit.ingestion.runner import run_sync; run_sync(smart=False)"
}

# Step 6: Launch options
echo -e "\n${CYAN}[6/6] Ready to launch!${NC}"
echo ""
echo "What would you like to do?"
echo ""
echo "  1) Start terminal UI (default)"
echo "  2) Smart sync (skip completed), then start TUI"
echo "  3) Just smart sync (skip completed, no UI)"
echo "  4) Force full re-sync of all sources"
echo "  5) Configure API keys"
echo "  6) Exit"
echo ""
read -p "Choice [1]: " choice
choice=${choice:-1}

case $choice in
    1)
        echo -e "\n${GREEN}Starting Terminal UI...${NC}"
        echo "Press 'q' to quit, 'd' for dashboard, 'v' for vendors, 'a' for alerts"
        echo ""
        texasaudit tui
        ;;
    2)
        smart_sync
        echo -e "\n${YELLOW}Running fraud detection...${NC}"
        texasaudit analyze run
        echo -e "\n${GREEN}Starting Terminal UI...${NC}"
        texasaudit tui
        ;;
    3)
        smart_sync
        echo -e "\n${YELLOW}Running fraud detection...${NC}"
        texasaudit analyze run
        echo -e "\n${GREEN}Sync complete!${NC}"
        echo "Run './run.sh' again to start the UI"
        ;;
    4)
        parallel_full_sync
        echo -e "\n${YELLOW}Running fraud detection...${NC}"
        texasaudit analyze run
        echo -e "\n${GREEN}Full sync complete!${NC}"
        echo "Run './run.sh' again to start the UI"
        ;;
    5)
        configure_api_keys
        echo -e "\n${GREEN}API keys updated!${NC}"
        echo "Run './run.sh' again to continue"
        ;;
    6)
        echo "Goodbye!"
        exit 0
        ;;
    *)
        echo -e "${RED}Invalid choice${NC}"
        exit 1
        ;;
esac
