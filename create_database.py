import os
import sqlite3
import csv

STATES = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA', 'OT']

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

# Primary keys for each table
PRIMARY_KEYS = {
    'LOCALITY': 'LOCALITY_PID',
    'STREET_LOCALITY': 'STREET_LOCALITY_PID',
    'ADDRESS_DETAIL': 'ADDRESS_DETAIL_PID',
    'ADDRESS_ALIAS': 'ADDRESS_ALIAS_PID',
    'ADDRESS_DEFAULT_GEOCODE': 'ADDRESS_DEFAULT_GEOCODE_PID',
    'ADDRESS_FEATURE': 'ADDRESS_FEATURE_ID',
    'ADDRESS_MESH_BLOCK_2016': 'ADDRESS_MESH_BLOCK_2016_PID',
    'ADDRESS_MESH_BLOCK_2021': 'ADDRESS_MESH_BLOCK_2021_PID',
    'ADDRESS_SITE': 'ADDRESS_SITE_PID',
    'ADDRESS_SITE_GEOCODE': 'ADDRESS_SITE_GEOCODE_PID',
    'LOCALITY_ALIAS': 'LOCALITY_ALIAS_PID',
    'LOCALITY_NEIGHBOUR': 'LOCALITY_NEIGHBOUR_PID',
    'LOCALITY_POINT': 'LOCALITY_POINT_PID',
    'MB_2016': 'MB_2016_PID',
    'MB_2021': 'MB_2021_PID',
    'PRIMARY_SECONDARY': 'PRIMARY_SECONDARY_PID',
    'STATE': 'STATE_PID',
    'STREET_LOCALITY_ALIAS': 'STREET_LOCALITY_ALIAS_PID',
    'STREET_LOCALITY_POINT': 'STREET_LOCALITY_POINT_PID',
    # All authority codes use CODE as PK
    **{table: 'CODE' for table in AUTHORITY_CODE_TABLES}
}

# Foreign key relationships - ALL FKs from add_fk_constraints.sql
# Format: (column, referenced_table, referenced_column)
FOREIGN_KEYS = {
    'ADDRESS_SITE': [
        ('ADDRESS_TYPE', 'ADDRESS_TYPE_AUT', 'CODE')
    ],
    'LOCALITY': [
        ('GNAF_RELIABILITY_CODE', 'GEOCODE_RELIABILITY_AUT', 'CODE'),
        ('LOCALITY_CLASS_CODE', 'LOCALITY_CLASS_AUT', 'CODE'),
        ('STATE_PID', 'STATE', 'STATE_PID')
    ],
    'STREET_LOCALITY': [
        ('GNAF_RELIABILITY_CODE', 'GEOCODE_RELIABILITY_AUT', 'CODE'),
        ('LOCALITY_PID', 'LOCALITY', 'LOCALITY_PID'),
        ('STREET_CLASS_CODE', 'STREET_CLASS_AUT', 'CODE'),
        ('STREET_SUFFIX_CODE', 'STREET_SUFFIX_AUT', 'CODE'),
        ('STREET_TYPE_CODE', 'STREET_TYPE_AUT', 'CODE')
    ],
    'LOCALITY_ALIAS': [
        ('ALIAS_TYPE_CODE', 'LOCALITY_ALIAS_TYPE_AUT', 'CODE'),
        ('LOCALITY_PID', 'LOCALITY', 'LOCALITY_PID')
    ],
    'LOCALITY_NEIGHBOUR': [
        ('LOCALITY_PID', 'LOCALITY', 'LOCALITY_PID'),
        ('NEIGHBOUR_LOCALITY_PID', 'LOCALITY', 'LOCALITY_PID')
    ],
    'LOCALITY_POINT': [
        ('LOCALITY_PID', 'LOCALITY', 'LOCALITY_PID')
    ],
    'ADDRESS_SITE_GEOCODE': [
        ('ADDRESS_SITE_PID', 'ADDRESS_SITE', 'ADDRESS_SITE_PID'),
        ('GEOCODE_TYPE_CODE', 'GEOCODE_TYPE_AUT', 'CODE'),
        ('RELIABILITY_CODE', 'GEOCODE_RELIABILITY_AUT', 'CODE')
    ],
    'ADDRESS_DETAIL': [
        ('ADDRESS_SITE_PID', 'ADDRESS_SITE', 'ADDRESS_SITE_PID'),
        ('FLAT_TYPE_CODE', 'FLAT_TYPE_AUT', 'CODE'),
        ('LEVEL_GEOCODED_CODE', 'GEOCODED_LEVEL_TYPE_AUT', 'CODE'),
        ('LEVEL_TYPE_CODE', 'LEVEL_TYPE_AUT', 'CODE'),
        ('LOCALITY_PID', 'LOCALITY', 'LOCALITY_PID'),
        ('STREET_LOCALITY_PID', 'STREET_LOCALITY', 'STREET_LOCALITY_PID')
    ],
    'STREET_LOCALITY_ALIAS': [
        ('ALIAS_TYPE_CODE', 'STREET_LOCALITY_ALIAS_TYPE_AUT', 'CODE'),
        ('STREET_LOCALITY_PID', 'STREET_LOCALITY', 'STREET_LOCALITY_PID'),
        ('STREET_SUFFIX_CODE', 'STREET_SUFFIX_AUT', 'CODE'),
        ('STREET_TYPE_CODE', 'STREET_TYPE_AUT', 'CODE')
    ],
    'STREET_LOCALITY_POINT': [
        ('STREET_LOCALITY_PID', 'STREET_LOCALITY', 'STREET_LOCALITY_PID')
    ],
    'ADDRESS_ALIAS': [
        ('ALIAS_PID', 'ADDRESS_DETAIL', 'ADDRESS_DETAIL_PID'),
        ('ALIAS_TYPE_CODE', 'ADDRESS_ALIAS_TYPE_AUT', 'CODE'),
        ('PRINCIPAL_PID', 'ADDRESS_DETAIL', 'ADDRESS_DETAIL_PID')
    ],
    'ADDRESS_DEFAULT_GEOCODE': [
        ('ADDRESS_DETAIL_PID', 'ADDRESS_DETAIL', 'ADDRESS_DETAIL_PID'),
        ('GEOCODE_TYPE_CODE', 'GEOCODE_TYPE_AUT', 'CODE')
    ],
    'ADDRESS_FEATURE': [
        ('ADDRESS_CHANGE_TYPE_CODE', 'ADDRESS_CHANGE_TYPE_AUT', 'CODE'),
        ('ADDRESS_DETAIL_PID', 'ADDRESS_DETAIL', 'ADDRESS_DETAIL_PID')
    ],
    'ADDRESS_MESH_BLOCK_2016': [
        ('ADDRESS_DETAIL_PID', 'ADDRESS_DETAIL', 'ADDRESS_DETAIL_PID'),
        ('MB_2016_PID', 'MB_2016', 'MB_2016_PID'),
        ('MB_MATCH_CODE', 'MB_MATCH_CODE_AUT', 'CODE')
    ],
    'ADDRESS_MESH_BLOCK_2021': [
        ('ADDRESS_DETAIL_PID', 'ADDRESS_DETAIL', 'ADDRESS_DETAIL_PID'),
        ('MB_2021_PID', 'MB_2021', 'MB_2021_PID'),
        ('MB_MATCH_CODE', 'MB_MATCH_CODE_AUT', 'CODE')
    ],
    'PRIMARY_SECONDARY': [
        ('PRIMARY_PID', 'ADDRESS_DETAIL', 'ADDRESS_DETAIL_PID'),
        ('PS_JOIN_TYPE_CODE', 'PS_JOIN_TYPE_AUT', 'CODE'),
        ('SECONDARY_PID', 'ADDRESS_DETAIL', 'ADDRESS_DETAIL_PID')
    ]
}



def create_table_from_psv_header(cursor, table_name, psv_path, include_state_column=True):
    """Create a table by reading PSV header and applying primary key and foreign keys.

    Args:
        cursor: SQLite cursor
        table_name: Name of the table to create
        psv_path: Path to the PSV file to read headers from
        include_state_column: Whether to include STATE column (True for state tables, False for authority)
    """
    with open(psv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='|')
        header = next(reader)

        columns = []
        if include_state_column:
            columns.append('"STATE" TEXT NOT NULL')

        pk_column = PRIMARY_KEYS.get(table_name)
        for col in header:
            if col:
                if col.upper() == pk_column:
                    columns.append(f'"{col}" TEXT PRIMARY KEY')
                else:
                    columns.append(f'"{col}" TEXT')

        # Add foreign key constraints if defined for this table
        fk_constraints = []
        if table_name in FOREIGN_KEYS:
            for fk_column, ref_table, ref_column in FOREIGN_KEYS[table_name]:
                fk_constraints.append(f'FOREIGN KEY ("{fk_column}") REFERENCES {ref_table}("{ref_column}")')

        # Build CREATE TABLE SQL
        all_definitions = columns + fk_constraints
        create_sql = f'CREATE TABLE {table_name} ({", ".join(all_definitions)});'
        cursor.execute(create_sql)

        fk_note = f" (with {len(fk_constraints)} FKs)" if fk_constraints else ""
        print(f"Created table: {table_name}{fk_note}")


def create_database(db_name='gnaf_addresses.db', data_dir=None, authority_code_dir=None, reference_state='ACT'):
    """Create a new SQLite database with unified schema for all states.

    Creates 35 tables total:
    - 16 authority code tables (no STATE column)
    - 19 state-specific tables (with STATE column)

    Tables are created in dependency order, then foreign keys are applied.

    Args:
        db_name: Path to the database file to create
        data_dir: Path to the Standard data directory
        authority_code_dir: Path to the Authority Code directory
        reference_state: State to use for reading PSV headers (default: 'ACT')
    """
    if data_dir is None:
        raise ValueError("data_dir parameter is required")
    if authority_code_dir is None:
        raise ValueError("authority_code_dir parameter is required")
    # Remove existing database if it exists
    if os.path.exists(db_name):
        os.remove(db_name)
        print(f"Removed existing database: {db_name}")

    # Connect to the database
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    print("\n=== Creating Authority Code Tables (16) ===")
    # Create authority code tables first (no dependencies, no STATE column)
    for table_name in AUTHORITY_CODE_TABLES:
        psv_path = os.path.join(authority_code_dir, f'Authority_Code_{table_name}_psv.psv')
        create_table_from_psv_header(cursor, table_name, psv_path, include_state_column=False)

    print("\n=== Creating State Tables (19) ===")
    # Create state tables in dependency order (with STATE column)
    # Foreign keys are included in CREATE TABLE statements
    for table_name in STATE_TABLES_ORDERED:
        psv_path = os.path.join(data_dir, f'{reference_state}_{table_name}_psv.psv')
        create_table_from_psv_header(cursor, table_name, psv_path, include_state_column=True)

    # Enable WAL mode for better concurrent write support
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=30000")  # 30 second timeout

    conn.commit()
    conn.close()

    print(f"\n=== Database Created Successfully ===")
    print(f"Location: {db_name}")
    print(f"Total tables: 35 (16 authority code + 19 state tables)")
    print(f"WAL mode enabled for concurrent writes")

if __name__ == "__main__":
    create_database('gnaf_addresses.db') 