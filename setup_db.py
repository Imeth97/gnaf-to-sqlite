from create_database import create_database
from load_data import load_data, load_authority_codes
from add_indexes import add_indexes
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3
import sys
import os

STATES = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA', 'OT']
UNIFIED_DB_NAME = 'gnaf_addresses.db'
SILENT_MODE = True  # Set to False to see detailed progress logs

def get_loaded_states(db_name):
    """Check which states already have data loaded in the database."""
    if not os.path.exists(db_name):
        return set()

    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Check if tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ADDRESS_DETAIL'")
        if not cursor.fetchone():
            conn.close()
            return set()

        # Get distinct states from ADDRESS_DETAIL table
        cursor.execute("SELECT DISTINCT STATE FROM ADDRESS_DETAIL")
        loaded_states = {row[0] for row in cursor.fetchall()}
        conn.close()

        return loaded_states
    except sqlite3.Error:
        return set()

def process_state_data(state):
    """Load a single state's data into the unified database."""
    try:
        if not SILENT_MODE:
            print(f"Loading data for {state}...")
        load_data(state, UNIFIED_DB_NAME, silent_mode=SILENT_MODE)
        return state, True
    except Exception as e:
        return state, f"Error loading {state}: {str(e)}"


if __name__ == "__main__":
    # Check which states are already loaded
    loaded_states = get_loaded_states(UNIFIED_DB_NAME)

    if not SILENT_MODE:
        if loaded_states:
            print(f"Found existing database with states: {', '.join(sorted(loaded_states))}")
        else:
            print("No existing database found or database is empty")

    # Create schema if database doesn't exist
    if not os.path.exists(UNIFIED_DB_NAME):
        if not SILENT_MODE:
            print("\nStep 1: Creating unified database schema (35 tables)...")
        create_database(UNIFIED_DB_NAME)
        if not SILENT_MODE:
            print(f"✓ Schema created in {UNIFIED_DB_NAME}")

        if not SILENT_MODE:
            print("\nStep 2: Loading authority code tables (16 lookup tables)...")
        load_authority_codes(UNIFIED_DB_NAME, silent_mode=SILENT_MODE)
        if not SILENT_MODE:
            print(f"✓ Authority codes loaded\n")
    else:
        if not SILENT_MODE:
            print(f"✓ Using existing database: {UNIFIED_DB_NAME}\n")
        # Check if authority codes are loaded
        try:
            conn = sqlite3.connect(UNIFIED_DB_NAME)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM ADDRESS_TYPE_AUT")
            count = cursor.fetchone()[0]
            conn.close()
            if count == 0:
                if not SILENT_MODE:
                    print("Step 2: Loading authority code tables (missing)...")
                load_authority_codes(UNIFIED_DB_NAME, silent_mode=SILENT_MODE)
                if not SILENT_MODE:
                    print(f"✓ Authority codes loaded\n")
        except sqlite3.Error:
            # Authority tables might not exist or might be empty
            pass

    # Determine which states need to be loaded
    states_to_load = [state for state in STATES if state not in loaded_states]

    if not states_to_load:
        if not SILENT_MODE:
            print("✓ All states already loaded!")
            print("\nStep 4: Creating indexes for performance...")
        add_indexes(UNIFIED_DB_NAME)
        if not SILENT_MODE:
            print(f"\n✓✓✓ Database setup complete! ✓✓✓")
            print(f"Database location: {UNIFIED_DB_NAME}")
            print(f"Total tables: 35 (16 authority code + 19 state tables × {len(STATES)} states)")
        sys.exit(0)

    if not SILENT_MODE:
        print(f"Step 3: Loading state data for {len(states_to_load)} state(s): {', '.join(states_to_load)}")
        print(f"        (19 tables per state in dependency order)\n")

    # Use 1 worker - SQLite only supports one writer at a time (even with WAL mode)
    with ThreadPoolExecutor(max_workers=1) as executor:
        # Submit tasks only for states that need loading
        future_to_state = {executor.submit(process_state_data, state): state for state in states_to_load}

        # Process results as they complete
        try:
            completed = 0
            for future in as_completed(future_to_state):
                state, result = future.result()
                if isinstance(result, str):  # Error occurred
                    print(f"\n✗ Error: {result}")
                    executor.shutdown(wait=False, cancel_futures=True)
                    sys.exit(1)
                else:
                    completed += 1
                    if not SILENT_MODE:
                        print(f"✓ Completed {state} ({completed}/{len(states_to_load)})")

            if not SILENT_MODE:
                print("\nStep 4: Creating indexes for performance...")
            add_indexes(UNIFIED_DB_NAME)

            
            print(f"\n✓✓✓ Database setup complete! ✓✓✓")
            print(f"Database location: {UNIFIED_DB_NAME}")
            print(f"Total tables: 35 (16 authority code + 19 state tables)")
            print(f"States loaded: {', '.join(sorted(loaded_states.union(set(states_to_load))))}")

        except Exception as e:
            print(f"\nUnexpected error: {str(e)}")
            executor.shutdown(wait=False, cancel_futures=True)
            sys.exit(1)