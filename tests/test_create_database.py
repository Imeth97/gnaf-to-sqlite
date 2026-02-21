import os
import sqlite3
import pytest
from create_database import (
    create_database, AUTHORITY_CODE_TABLES, STATE_TABLES_ORDERED,
    PRIMARY_KEYS, FOREIGN_KEYS
)


def test_database_file_created(mock_data_dir, clean_test_db):
    """Test that the database file is created."""
    create_database(clean_test_db, reference_state='TEST')

    assert os.path.exists(clean_test_db), "Database file should be created"


def test_tables_created(mock_data_dir, clean_test_db):
    """Test that all 35 tables are created (16 authority + 19 state)."""
    create_database(clean_test_db, reference_state='TEST')

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()

    # Verify core tables still present
    assert 'LOCALITY' in tables
    assert 'STREET_LOCALITY' in tables
    assert 'ADDRESS_DETAIL' in tables

    # Verify all 35 tables exist
    assert len(tables) == 35, f"Expected 35 tables, got {len(tables)}: {sorted(tables)}"

    # Verify all authority code tables
    for table in AUTHORITY_CODE_TABLES:
        assert table in tables, f"Authority code table {table} should exist"

    # Verify all state tables
    for table in STATE_TABLES_ORDERED:
        assert table in tables, f"State table {table} should exist"


def test_primary_keys(mock_data_dir, clean_test_db):
    """Test that primary key constraints are applied to all tables."""
    create_database(clean_test_db, reference_state='TEST')

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()

    # Check LOCALITY table PK
    cursor.execute("PRAGMA table_info(LOCALITY)")
    locality_columns = cursor.fetchall()
    locality_pk = [col for col in locality_columns if col[5] == 1]  # col[5] is pk flag
    assert len(locality_pk) == 1
    assert locality_pk[0][1] == 'LOCALITY_PID'

    # Check STREET_LOCALITY table PK
    cursor.execute("PRAGMA table_info(STREET_LOCALITY)")
    street_columns = cursor.fetchall()
    street_pk = [col for col in street_columns if col[5] == 1]
    assert len(street_pk) == 1
    assert street_pk[0][1] == 'STREET_LOCALITY_PID'

    # Check ADDRESS_DETAIL table PK
    cursor.execute("PRAGMA table_info(ADDRESS_DETAIL)")
    address_columns = cursor.fetchall()
    address_pk = [col for col in address_columns if col[5] == 1]
    assert len(address_pk) == 1
    assert address_pk[0][1] == 'ADDRESS_DETAIL_PID'

    conn.close()


def test_foreign_keys(mock_data_dir, clean_test_db):
    """Test that foreign key constraints are defined on ADDRESS_DETAIL."""
    create_database(clean_test_db, reference_state='TEST')

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()

    # Check foreign keys on ADDRESS_DETAIL
    cursor.execute("PRAGMA foreign_key_list(ADDRESS_DETAIL)")
    foreign_keys = cursor.fetchall()

    conn.close()

    # ADDRESS_DETAIL has 6 FKs: ADDRESS_SITE, FLAT_TYPE_AUT, GEOCODED_LEVEL_TYPE_AUT,
    # LEVEL_TYPE_AUT, LOCALITY, STREET_LOCALITY
    assert len(foreign_keys) == 6, f"Expected 6 FKs, got {len(foreign_keys)}"

    fk_tables = [fk[2] for fk in foreign_keys]
    assert 'STREET_LOCALITY' in fk_tables
    assert 'LOCALITY' in fk_tables
    assert 'ADDRESS_SITE' in fk_tables
    assert 'FLAT_TYPE_AUT' in fk_tables
    assert 'GEOCODED_LEVEL_TYPE_AUT' in fk_tables
    assert 'LEVEL_TYPE_AUT' in fk_tables


def test_column_count(mock_data_dir, clean_test_db):
    """Test that tables have the correct number of columns (including STATE column)."""
    create_database(clean_test_db, reference_state='TEST')

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()

    # LOCALITY should have 10 columns (STATE + 9 original)
    cursor.execute("PRAGMA table_info(LOCALITY)")
    locality_cols = cursor.fetchall()
    assert len(locality_cols) == 10

    # STREET_LOCALITY should have 12 columns (STATE + 11 original)
    cursor.execute("PRAGMA table_info(STREET_LOCALITY)")
    street_cols = cursor.fetchall()
    assert len(street_cols) == 12

    # ADDRESS_DETAIL should have 36 columns (STATE + 35 original)
    cursor.execute("PRAGMA table_info(ADDRESS_DETAIL)")
    address_cols = cursor.fetchall()
    assert len(address_cols) == 36

    conn.close()


def test_existing_db_removed(mock_data_dir, tmp_path):
    """Test that an existing database is removed before creating a new one."""
    db_path = tmp_path / "test_overwrite.db"

    # Create an initial database
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE old_table (id INTEGER)")
    conn.commit()
    conn.close()

    # Now call create_database which should remove the old one
    create_database(str(db_path), reference_state='TEST')

    # Verify the old table doesn't exist
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='old_table'")
    result = cursor.fetchall()
    conn.close()

    assert len(result) == 0, "Old table should not exist after database recreation"


def test_all_columns_text_type(mock_data_dir, clean_test_db):
    """Test that all columns are TEXT type."""
    create_database(clean_test_db, reference_state='TEST')

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()

    # Check LOCALITY columns
    cursor.execute("PRAGMA table_info(LOCALITY)")
    locality_cols = cursor.fetchall()
    for col in locality_cols:
        assert col[2] == 'TEXT', f"Column {col[1]} should be TEXT type"

    # Check STREET_LOCALITY columns
    cursor.execute("PRAGMA table_info(STREET_LOCALITY)")
    street_cols = cursor.fetchall()
    for col in street_cols:
        assert col[2] == 'TEXT', f"Column {col[1]} should be TEXT type"

    # Check ADDRESS_DETAIL columns
    cursor.execute("PRAGMA table_info(ADDRESS_DETAIL)")
    address_cols = cursor.fetchall()
    for col in address_cols:
        assert col[2] == 'TEXT', f"Column {col[1]} should be TEXT type"

    conn.close()


def test_state_column_exists(mock_data_dir, clean_test_db):
    """Test that STATE column exists in all tables and is NOT NULL."""
    create_database(clean_test_db, reference_state='TEST')

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()

    # Check LOCALITY has STATE column
    cursor.execute("PRAGMA table_info(LOCALITY)")
    locality_cols = cursor.fetchall()
    state_col = [col for col in locality_cols if col[1] == 'STATE']
    assert len(state_col) == 1, "STATE column should exist in LOCALITY"
    assert state_col[0][3] == 1, "STATE column should be NOT NULL"  # col[3] is notnull flag

    # Check STREET_LOCALITY has STATE column
    cursor.execute("PRAGMA table_info(STREET_LOCALITY)")
    street_cols = cursor.fetchall()
    state_col = [col for col in street_cols if col[1] == 'STATE']
    assert len(state_col) == 1, "STATE column should exist in STREET_LOCALITY"
    assert state_col[0][3] == 1, "STATE column should be NOT NULL"

    # Check ADDRESS_DETAIL has STATE column
    cursor.execute("PRAGMA table_info(ADDRESS_DETAIL)")
    address_cols = cursor.fetchall()
    state_col = [col for col in address_cols if col[1] == 'STATE']
    assert len(state_col) == 1, "STATE column should exist in ADDRESS_DETAIL"
    assert state_col[0][3] == 1, "STATE column should be NOT NULL"

    conn.close()


def test_authority_code_tables_no_state_column(mock_data_dir, clean_test_db):
    """Test that all 16 authority code tables have no STATE column."""
    create_database(clean_test_db, reference_state='TEST')

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()

    for table in AUTHORITY_CODE_TABLES:
        cursor.execute(f"PRAGMA table_info({table})")
        cols = cursor.fetchall()
        col_names = [col[1] for col in cols]
        assert 'STATE' not in col_names, f"Authority table {table} should NOT have STATE column"

    conn.close()


def test_state_tables_have_state_column(mock_data_dir, clean_test_db):
    """Test that all 19 state tables have a NOT NULL STATE column."""
    create_database(clean_test_db, reference_state='TEST')

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()

    for table in STATE_TABLES_ORDERED:
        cursor.execute(f"PRAGMA table_info({table})")
        cols = cursor.fetchall()
        state_col = [col for col in cols if col[1] == 'STATE']
        assert len(state_col) == 1, f"State table {table} should have STATE column"
        assert state_col[0][3] == 1, f"STATE column in {table} should be NOT NULL"

    conn.close()


def test_all_primary_keys_defined(mock_data_dir, clean_test_db):
    """Test that all 35 tables have the correct primary key column."""
    create_database(clean_test_db, reference_state='TEST')

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()

    for table, pk_col in PRIMARY_KEYS.items():
        cursor.execute(f"PRAGMA table_info({table})")
        cols = cursor.fetchall()
        pk_cols = [col[1] for col in cols if col[5] == 1]  # col[5] is pk flag
        assert len(pk_cols) == 1, f"Table {table} should have exactly 1 primary key"
        assert pk_cols[0].upper() == pk_col.upper(), (
            f"Table {table} PK should be {pk_col}, got {pk_cols[0]}"
        )

    conn.close()


def test_all_foreign_keys_defined(mock_data_dir, clean_test_db):
    """Test that all foreign key relationships are defined correctly."""
    create_database(clean_test_db, reference_state='TEST')

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()

    for table, fk_list in FOREIGN_KEYS.items():
        cursor.execute(f"PRAGMA foreign_key_list({table})")
        defined_fks = cursor.fetchall()
        defined_refs = {(fk[3].upper(), fk[2]) for fk in defined_fks}  # (column, ref_table)

        for fk_col, ref_table, ref_col in fk_list:
            assert (fk_col.upper(), ref_table) in defined_refs, (
                f"FK {table}.{fk_col} -> {ref_table} not found in PRAGMA output"
            )

    conn.close()


def test_authority_code_table_pk_is_code(mock_data_dir, clean_test_db):
    """Test that all authority code tables use CODE as primary key."""
    create_database(clean_test_db, reference_state='TEST')

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()

    for table in AUTHORITY_CODE_TABLES:
        cursor.execute(f"PRAGMA table_info({table})")
        cols = cursor.fetchall()
        pk_cols = [col[1] for col in cols if col[5] == 1]
        assert len(pk_cols) == 1, f"Authority table {table} should have exactly 1 PK"
        assert pk_cols[0].upper() == 'CODE', (
            f"Authority table {table} PK should be CODE, got {pk_cols[0]}"
        )

    conn.close()


def test_table_count_by_type(mock_data_dir, clean_test_db):
    """Test that database has exactly 16 authority + 19 state = 35 tables."""
    create_database(clean_test_db, reference_state='TEST')

    assert len(AUTHORITY_CODE_TABLES) == 16, f"Should have 16 authority tables, got {len(AUTHORITY_CODE_TABLES)}"
    assert len(STATE_TABLES_ORDERED) == 19, f"Should have 19 state tables, got {len(STATE_TABLES_ORDERED)}"

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}
    conn.close()

    authority_in_db = tables.intersection(set(AUTHORITY_CODE_TABLES))
    state_in_db = tables.intersection(set(STATE_TABLES_ORDERED))

    assert len(authority_in_db) == 16, f"Expected 16 authority tables in DB, got {len(authority_in_db)}"
    assert len(state_in_db) == 19, f"Expected 19 state tables in DB, got {len(state_in_db)}"
    assert len(tables) == 35


def test_wal_mode_enabled(mock_data_dir, clean_test_db):
    """Test that WAL mode is enabled after database creation."""
    create_database(clean_test_db, reference_state='TEST')

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode")
    mode = cursor.fetchone()[0]
    conn.close()

    assert mode == 'wal', f"Expected WAL mode, got {mode}"
