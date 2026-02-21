#!/usr/bin/env bash

set -euo pipefail

# -------- Config --------
SCRIPT_NAME="setup_db.py"

# -------- Validate python + script --------
if ! command -v python3 &> /dev/null; then
  echo "❌ python3 is required but not found on your PATH."
  exit 1
fi

if [ ! -f "$SCRIPT_NAME" ]; then
  echo "❌ Could not find $SCRIPT_NAME in the current directory."
  echo "   Make sure you are running this script from the repo root."
  exit 1
fi

# -------- Read arguments --------
# First arg is GNAF base path, second arg (optional) is output DB path
GNAF_BASE_PATH="${1:-}"
DB_PATH="${2:-out/gnaf.db}"

if [ -z "$GNAF_BASE_PATH" ]; then
  echo "Usage: ./run_setup.sh <gnaf_base_path> [output_db_path]"
  echo "Example:"
  echo "  ./run_setup.sh 'g-naf_nov25_allstates_gda2020_psv_1021/G-NAF/G-NAF NOVEMBER 2025'"
  echo "  ./run_setup.sh 'g-naf_nov25_allstates_gda2020_psv_1021/G-NAF/G-NAF NOVEMBER 2025' out/gnaf.sqlite"
  echo ""
  echo "Arguments:"
  echo "  gnaf_base_path   - Path to the GNAF base directory (containing 'Standard' and 'Authority Code' subdirectories)"
  echo "  output_db_path   - Path where the SQLite database will be created (default: out/gnaf.db)"
  exit 1
fi

# -------- Validate GNAF base path --------
if [ ! -d "$GNAF_BASE_PATH" ]; then
  echo "❌ Error: GNAF base path does not exist:"
  echo "   $GNAF_BASE_PATH"
  exit 1
fi

if [ ! -d "$GNAF_BASE_PATH/Standard" ]; then
  echo "❌ Error: 'Standard' subdirectory not found in GNAF base path:"
  echo "   $GNAF_BASE_PATH/Standard"
  echo ""
  echo "   Please ensure the GNAF base path contains a 'Standard' subdirectory."
  exit 1
fi

if [ ! -d "$GNAF_BASE_PATH/Authority Code" ]; then
  echo "❌ Error: 'Authority Code' subdirectory not found in GNAF base path:"
  echo "   $GNAF_BASE_PATH/Authority Code"
  echo ""
  echo "   Please ensure the GNAF base path contains an 'Authority Code' subdirectory."
  exit 1
fi

# -------- Warnings --------
echo ""
echo "⚠️  WARNING: DATABASE OVERWRITE"
echo "---------------------------------------------"
echo "If a database already exists at:"
echo "  $DB_PATH"
echo "it will be OVERWRITTEN."
echo ""

echo "⚠️  WARNING: LONG RUNNING PROCESS"
echo "---------------------------------------------"
echo "This process can take approximately ~1 hour to complete."
echo "There is currently NO checkpointing or resume capability."
echo "If you interrupt the process, you will need to start again from scratch."
echo "Resulting SQLite DB will be ~18GB - ensure you have disk space."
echo ""

# -------- Confirm --------
read -p "Do you want to proceed? Type 'Y' to continue: " CONFIRM

if [[ "$CONFIRM" != "Y" ]]; then
  echo "❌ Aborted by user."
  exit 0
fi

# -------- Ensure output directory exists --------
mkdir -p "$(dirname "$DB_PATH")"

echo ""
echo "🚀 Starting database build..."
echo "Output DB: $DB_PATH"
echo "GNAF base path: $GNAF_BASE_PATH"
echo ""

# -------- Execute python script --------
# Pass DB path and GNAF base path as arguments
python3 "$SCRIPT_NAME" "$DB_PATH" "$GNAF_BASE_PATH"

echo ""
echo "✅ Database build complete!"
echo "Location: $DB_PATH"
echo ""

# -------- Post-processing options --------
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Post-Processing Options:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1) Exit now"
echo "2) Generate list of freestanding property addresses"
echo ""
read -p "Choose an option (1 or 2): " POST_OPTION

if [[ "$POST_OPTION" == "2" ]]; then
  echo ""
  echo "🏠 Generating freestanding property addresses..."
  echo ""

  # Check if the additional script exists
  if [ ! -f "additional/process_addresses.py" ]; then
    echo "❌ Error: additional/process_addresses.py not found"
    exit 1
  fi

  # Execute the address processing script with the database path
  python3 "additional/process_addresses.py" "$DB_PATH"

  echo ""
  echo "✅ Address processing complete!"
elif [[ "$POST_OPTION" == "1" ]]; then
  echo ""
  echo "👋 Exiting..."
else
  echo ""
  echo "⚠️  Invalid option. Exiting..."
fi
