import os
import sqlite3
import csv
from progress_tracker import ProgressTracker, AuthorityProgressTracker

# Constants
DATA_DIR = 'g-naf_nov25_allstates_gda2020_psv_1021/G-NAF/G-NAF NOVEMBER 2025/Standard'
AUTHORITY_CODE_DIR = 'g-naf_nov25_allstates_gda2020_psv_1021/G-NAF/G-NAF NOVEMBER 2025/Authority Code'

# Authority code tables - NO STATE column, load once
AUTHORITY_CODE_TABLES = [
    'ADDRESS_ALIAS_TYPE_AUT', 'ADDRESS_CHANGE_TYPE_AUT', 'ADDRESS_TYPE_AUT',
    'FLAT_TYPE_AUT', 'GEOCODE_RELIABILITY_AUT', 'GEOCODE_TYPE_AUT',
    'GEOCODED_LEVEL_TYPE_AUT', 'LEVEL_TYPE_AUT', 'LOCALITY_ALIAS_TYPE_AUT',
    'LOCALITY_CLASS_AUT', 'MB_MATCH_CODE_AUT', 'PS_JOIN_TYPE_AUT',
    'STREET_CLASS_AUT', 'STREET_LOCALITY_ALIAS_TYPE_AUT', 'STREET_SUFFIX_AUT',
    'STREET_TYPE_AUT'
]

# State tables in dependency order - WITH STATE column
STATE_TABLES_ORDERED = [
    # Tier 1
    'STATE', 'MB_2016', 'MB_2021',
    # Tier 2
    'LOCALITY', 'ADDRESS_SITE',
    # Tier 3
    'STREET_LOCALITY', 'LOCALITY_ALIAS', 'LOCALITY_NEIGHBOUR',
    'LOCALITY_POINT', 'ADDRESS_SITE_GEOCODE',
    # Tier 4
    'ADDRESS_DETAIL', 'STREET_LOCALITY_ALIAS', 'STREET_LOCALITY_POINT',
    # Tier 5
    'ADDRESS_ALIAS', 'ADDRESS_DEFAULT_GEOCODE', 'ADDRESS_FEATURE',
    'ADDRESS_MESH_BLOCK_2016', 'ADDRESS_MESH_BLOCK_2021', 'PRIMARY_SECONDARY'
]


def load_authority_codes(db_name='gnaf_addresses.db', silent_mode=False):
    """Load authority code tables once (not per state).

    These are reference/lookup tables that apply to all states.
    They have no STATE column.

    Args:
        db_name: Path to the database file
        silent_mode: If True, suppress progress messages
    """
    conn = sqlite3.connect(db_name, timeout=600.0)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=600000")
    conn.execute("PRAGMA synchronous=NORMAL")  # Safe with WAL, 2-3x faster
    conn.execute("PRAGMA cache_size=-64000")   # 64MB cache (default ~2MB)
    conn.execute("PRAGMA temp_store=MEMORY")   # Use RAM for temp tables
    cursor = conn.cursor()

    if not silent_mode:
        print("\n=== Loading Authority Code Tables (16) ===")

    # Initialize progress tracker for authority codes (pre-scans files to count total rows)
    # Disable progress in test environment to avoid cluttering test output
    show_progress = os.environ.get('GNAF_SHOW_PROGRESS', '1') == '1'
    progress = AuthorityProgressTracker(AUTHORITY_CODE_TABLES, AUTHORITY_CODE_DIR) if show_progress else None

    for table_index, table_name in enumerate(AUTHORITY_CODE_TABLES):
        file_path = os.path.join(AUTHORITY_CODE_DIR, f'Authority_Code_{table_name}_psv.psv')
        if not silent_mode:
            print(f"Loading authority code table {table_name}...")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='|')
            header = next(reader)
            columns = [col for col in header if col]
            placeholders = ', '.join(['?' for _ in columns])

            # Prepare batch insert
            batch = []
            BATCH_SIZE = 5000
            count = 0
            skipped = 0

            for row in reader:
                values = row[:len(columns)]
                values = [None if v == '' else v for v in values]
                batch.append(values)

                # Process batch when it reaches BATCH_SIZE
                if len(batch) >= BATCH_SIZE:
                    try:
                        cursor.executemany(
                            f"INSERT OR IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})",
                            batch
                        )
                        rows_inserted = cursor.rowcount
                        count += rows_inserted

                        # Update progress tracker
                        if progress:
                            progress.update(table_name, table_index, rows_inserted)

                        batch = []
                    except sqlite3.IntegrityError as e:
                        # Fall back to row-by-row for this batch
                        batch_rows_inserted = 0
                        for single_row in batch:
                            try:
                                cursor.execute(
                                    f"INSERT OR IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})",
                                    single_row
                                )
                                if cursor.rowcount > 0:
                                    count += 1
                                    batch_rows_inserted += 1
                            except sqlite3.IntegrityError:
                                skipped += 1
                                if skipped <= 3:
                                    print(f"  Skipped row due to integrity error")

                        # Update progress tracker for row-by-row batch
                        if batch_rows_inserted > 0 and progress:
                            progress.update(table_name, table_index, batch_rows_inserted)

                        batch = []

            # Process remaining batch
            if batch:
                try:
                    cursor.executemany(
                        f"INSERT OR IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})",
                        batch
                    )
                    rows_inserted = cursor.rowcount
                    count += rows_inserted

                    # Update progress tracker for final batch
                    if progress:
                        progress.update(table_name, table_index, rows_inserted)
                except sqlite3.IntegrityError as e:
                    # Fall back to row-by-row for final batch
                    batch_rows_inserted = 0
                    for single_row in batch:
                        try:
                            cursor.execute(
                                f"INSERT OR IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})",
                                single_row
                            )
                            if cursor.rowcount > 0:
                                count += 1
                                batch_rows_inserted += 1
                        except sqlite3.IntegrityError:
                            skipped += 1

                    # Update progress tracker for final row-by-row batch
                    if batch_rows_inserted > 0 and progress:
                        progress.update(table_name, table_index, batch_rows_inserted)

            conn.commit()
            if not silent_mode:
                print(f"  Loaded {count} rows into {table_name}")

    # Finish progress tracking (prints final newline)
    if progress:
        progress.finish()

    conn.close()
    if not silent_mode:
        print("=== Authority code tables loaded successfully ===\n")


def load_data(state, db_name='gnaf_addresses.db', silent_mode=False):
    """Load data from PSV files into the unified SQLite database.

    Loads all 19 state tables in dependency order for the specified state.
    Authority code tables should be loaded separately using load_authority_codes().

    Args:
        state: State abbreviation (e.g., 'ACT', 'NSW')
        db_name: Path to the database file
        silent_mode: If True, suppress progress messages
    """
    # Connect to the database with a 600-second timeout for large state loads
    conn = sqlite3.connect(db_name, timeout=600.0)
    # Enable foreign key constraints and WAL mode for better concurrency
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=600000")
    conn.execute("PRAGMA synchronous=NORMAL")  # Safe with WAL, 2-3x faster
    conn.execute("PRAGMA cache_size=-64000")   # 64MB cache (default ~2MB)
    conn.execute("PRAGMA temp_store=MEMORY")   # Use RAM for temp tables
    cursor = conn.cursor()

    if not silent_mode:
        print(f"\n=== Loading State Tables for {state} (19 tables) ===")

    # Initialize progress tracker (pre-scans files to count total rows)
    # Disable progress in test environment to avoid cluttering test output
    show_progress = os.environ.get('GNAF_SHOW_PROGRESS', '1') == '1'
    progress = ProgressTracker(state, STATE_TABLES_ORDERED, DATA_DIR) if show_progress else None

    # Load all state tables in dependency order
    for table_index, table_name in enumerate(STATE_TABLES_ORDERED):
        psv_file = f'{state}_{table_name}_psv.psv'
        file_path = os.path.join(DATA_DIR, psv_file)
        if not silent_mode:
            print(f"Loading {state} data into {table_name}...")

        # Read the file
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='|')
            header = next(reader)

            # Filter out empty column names
            columns = [col for col in header if col]

            # Prepend STATE to columns for insert
            insert_columns = ['STATE'] + columns
            placeholders = ', '.join(['?' for _ in insert_columns])

            # Prepare batch insert
            batch = []
            BATCH_SIZE = 5000  # Process 5000 rows at a time
            count = 0
            skipped = 0

            for row in reader:
                # Only use as many values as we have columns
                values = row[:len(columns)]
                # Convert empty strings to None (NULL in SQLite)
                values = [None if v == '' else v for v in values]
                # Prepend state value
                insert_values = [state] + values

                batch.append(insert_values)

                # Process batch when it reaches BATCH_SIZE
                if len(batch) >= BATCH_SIZE:
                    try:
                        cursor.executemany(
                            f"INSERT OR IGNORE INTO {table_name} ({', '.join(insert_columns)}) VALUES ({placeholders})",
                            batch
                        )
                        # rowcount for executemany() returns total rows affected
                        rows_inserted = cursor.rowcount
                        count += rows_inserted

                        # Update progress tracker
                        if progress:
                            progress.update(table_name, table_index, rows_inserted)

                        batch = []

                        # Commit every 50,000 rows (10 batches)
                        if count % 50000 == 0 and count > 0:
                            conn.commit()
                            if not silent_mode:
                                print(f"  Loaded {count} rows for {state}...")
                    except sqlite3.IntegrityError as e:
                        # If batch fails, fall back to row-by-row for this batch
                        batch_rows_inserted = 0
                        for single_row in batch:
                            try:
                                cursor.execute(
                                    f"INSERT OR IGNORE INTO {table_name} ({', '.join(insert_columns)}) VALUES ({placeholders})",
                                    single_row
                                )
                                if cursor.rowcount > 0:
                                    count += 1
                                    batch_rows_inserted += 1
                            except sqlite3.IntegrityError:
                                skipped += 1
                                if skipped <= 5:
                                    print(f"  Skipped row due to integrity error")
                                elif skipped == 6:
                                    print("  Further integrity errors will not be shown...")

                        # Update progress tracker for row-by-row batch
                        if batch_rows_inserted > 0 and progress:
                            progress.update(table_name, table_index, batch_rows_inserted)

                        batch = []

            # Process remaining batch
            if batch:
                try:
                    cursor.executemany(
                        f"INSERT OR IGNORE INTO {table_name} ({', '.join(insert_columns)}) VALUES ({placeholders})",
                        batch
                    )
                    rows_inserted = cursor.rowcount
                    count += rows_inserted

                    # Update progress tracker for final batch
                    if progress:
                        progress.update(table_name, table_index, rows_inserted)
                except sqlite3.IntegrityError as e:
                    # Fall back to row-by-row for final batch
                    batch_rows_inserted = 0
                    for single_row in batch:
                        try:
                            cursor.execute(
                                f"INSERT OR IGNORE INTO {table_name} ({', '.join(insert_columns)}) VALUES ({placeholders})",
                                single_row
                            )
                            if cursor.rowcount > 0:
                                count += 1
                                batch_rows_inserted += 1
                        except sqlite3.IntegrityError:
                            skipped += 1

                    # Update progress tracker for final row-by-row batch
                    if batch_rows_inserted > 0 and progress:
                        progress.update(table_name, table_index, batch_rows_inserted)

            conn.commit()
            if not silent_mode:
                print(f"  Completed: {count} rows for {state} in {table_name} (skipped {skipped})")

    # Finish progress tracking (prints final newline)
    if progress:
        progress.finish()

    conn.close()
    if not silent_mode:
        print(f"=== Data loading complete for {state} ===\n")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        state = sys.argv[1]
        load_data(state)
    else:
        print("Usage: python load_data.py <STATE>")
        print("Example: python load_data.py ACT") 