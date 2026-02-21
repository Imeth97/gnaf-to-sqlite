import os
import sqlite3
import pytest
from create_database import create_database, AUTHORITY_CODE_TABLES, STATE_TABLES_ORDERED
from load_data import load_data, load_authority_codes


@pytest.fixture
def prepared_db(mock_data_dir, clean_test_db):
    """Create a database with schema and authority codes loaded (but no state data)."""
    create_database(clean_test_db, reference_state='TEST')
    load_authority_codes(clean_test_db)
    return clean_test_db


def test_load_locality_data(mock_data_dir, prepared_db):
    """Test that LOCALITY data is loaded correctly."""
    load_data('TEST', prepared_db)

    conn = sqlite3.connect(prepared_db)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM LOCALITY WHERE STATE = 'TEST'")
    count = cursor.fetchone()[0]

    cursor.execute("SELECT STATE, LOCALITY_PID, LOCALITY_NAME FROM LOCALITY WHERE STATE = 'TEST' ORDER BY LOCALITY_PID")
    rows = cursor.fetchall()

    conn.close()

    assert count == 3, "Should load 3 LOCALITY rows"
    assert rows[0][0] == 'TEST'
    assert rows[0][1] == 'loc_test_001'
    assert rows[0][2] == 'TESTVILLE'
    assert rows[1][1] == 'loc_test_002'
    assert rows[2][1] == 'loc_test_003'


def test_load_street_locality_data(mock_data_dir, prepared_db):
    """Test that STREET_LOCALITY data is loaded correctly."""
    load_data('TEST', prepared_db)

    conn = sqlite3.connect(prepared_db)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM STREET_LOCALITY WHERE STATE = 'TEST'")
    count = cursor.fetchone()[0]

    cursor.execute("SELECT STATE, STREET_LOCALITY_PID, STREET_NAME, STREET_TYPE_CODE FROM STREET_LOCALITY WHERE STATE = 'TEST' ORDER BY STREET_LOCALITY_PID")
    rows = cursor.fetchall()

    conn.close()

    assert count == 3, "Should load 3 STREET_LOCALITY rows"
    assert rows[0][0] == 'TEST'
    assert rows[0][1] == 'TST0000001'
    assert rows[0][2] == 'TEST'
    assert rows[0][3] == 'STREET'
    assert rows[1][2] == 'SAMPLE'
    assert rows[2][2] == 'MOCK'


def test_load_address_detail_data(mock_data_dir, prepared_db):
    """Test that ADDRESS_DETAIL data is loaded correctly."""
    load_data('TEST', prepared_db)

    conn = sqlite3.connect(prepared_db)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM ADDRESS_DETAIL WHERE STATE = 'TEST'")
    count = cursor.fetchone()[0]

    cursor.execute("SELECT STATE, ADDRESS_DETAIL_PID, NUMBER_FIRST, POSTCODE FROM ADDRESS_DETAIL WHERE STATE = 'TEST' ORDER BY ADDRESS_DETAIL_PID")
    rows = cursor.fetchall()

    conn.close()

    assert count == 3, "Should load 3 ADDRESS_DETAIL rows"
    assert rows[0][0] == 'TEST'
    assert rows[0][1] == 'GANTEST001'
    assert rows[0][2] == '1'
    assert rows[0][3] == '2000'
    assert rows[1][2] == '2'
    assert rows[2][2] == '3'


def test_foreign_key_enforcement(mock_data_dir, prepared_db):
    """Test that foreign key constraints are enforced during loading."""
    # First load the data normally
    load_data('TEST', prepared_db)

    conn = sqlite3.connect(prepared_db)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    # Try to insert an ADDRESS_DETAIL row with invalid LOCALITY_PID
    with pytest.raises(sqlite3.IntegrityError):
        cursor.execute("""
            INSERT INTO ADDRESS_DETAIL
            (STATE, ADDRESS_DETAIL_PID, NUMBER_FIRST, STREET_LOCALITY_PID, LOCALITY_PID, POSTCODE)
            VALUES ('TEST', 'GANTEST999', '999', 'TST0000001', 'invalid_locality', '9999')
        """)

    # Try to insert an ADDRESS_DETAIL row with invalid STREET_LOCALITY_PID
    with pytest.raises(sqlite3.IntegrityError):
        cursor.execute("""
            INSERT INTO ADDRESS_DETAIL
            (STATE, ADDRESS_DETAIL_PID, NUMBER_FIRST, STREET_LOCALITY_PID, LOCALITY_PID, POSTCODE)
            VALUES ('TEST', 'GANTEST998', '998', 'invalid_street', 'loc_test_001', '9998')
        """)

    conn.close()


def test_duplicate_pk_handling(mock_data_dir, prepared_db):
    """Test that duplicate primary keys are handled (skipped)."""
    # Load data once
    load_data('TEST', prepared_db)

    conn = sqlite3.connect(prepared_db)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM LOCALITY WHERE STATE = 'TEST'")
    count_before = cursor.fetchone()[0]

    conn.close()

    # Load data again - duplicates should be skipped
    load_data('TEST', prepared_db)

    conn = sqlite3.connect(prepared_db)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM LOCALITY WHERE STATE = 'TEST'")
    count_after = cursor.fetchone()[0]

    conn.close()

    # Count should remain the same (duplicates skipped)
    assert count_before == count_after == 3


def test_empty_fields(mock_data_dir, prepared_db):
    """Test that empty PSV fields are handled correctly (stored as NULL)."""
    load_data('TEST', prepared_db)

    conn = sqlite3.connect(prepared_db)
    cursor = conn.cursor()

    # Check that DATE_RETIRED is NULL in LOCALITY
    cursor.execute("SELECT DATE_RETIRED FROM LOCALITY WHERE LOCALITY_PID = 'loc_test_001' AND STATE = 'TEST'")
    date_retired = cursor.fetchone()[0]

    # Check that STREET_SUFFIX_CODE is NULL in STREET_LOCALITY
    cursor.execute("SELECT STREET_SUFFIX_CODE FROM STREET_LOCALITY WHERE STREET_LOCALITY_PID = 'TST0000001' AND STATE = 'TEST'")
    suffix = cursor.fetchone()[0]

    # Check that multiple empty fields are NULL in ADDRESS_DETAIL
    cursor.execute("SELECT BUILDING_NAME, LOT_NUMBER_PREFIX FROM ADDRESS_DETAIL WHERE ADDRESS_DETAIL_PID = 'GANTEST001' AND STATE = 'TEST'")
    building, lot_prefix = cursor.fetchone()

    conn.close()

    # Empty PSV fields should be stored as NULL (None in Python)
    assert date_retired is None
    assert suffix is None
    assert building is None
    assert lot_prefix is None


def test_encoding_utf8(mock_data_dir, prepared_db):
    """Test that UTF-8 encoding is handled correctly."""
    load_data('TEST', prepared_db)

    conn = sqlite3.connect(prepared_db)
    cursor = conn.cursor()

    # Query all locality names
    cursor.execute("SELECT LOCALITY_NAME FROM LOCALITY WHERE STATE = 'TEST' ORDER BY LOCALITY_PID")
    names = [row[0] for row in cursor.fetchall()]

    conn.close()

    # Should successfully read all names as strings
    assert all(isinstance(name, str) for name in names)
    assert 'TESTVILLE' in names
    assert 'SAMPLETOWN' in names
    assert 'MOCKVILLE' in names


def test_loading_order(mock_data_dir, prepared_db):
    """Test that tables are loaded in correct dependency order."""
    # This test verifies that if we load in the correct order,
    # foreign key relationships are satisfied

    conn = sqlite3.connect(prepared_db)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.close()

    # Should complete without foreign key errors
    load_data('TEST', prepared_db)

    conn = sqlite3.connect(prepared_db)
    cursor = conn.cursor()

    # Verify all tables have data
    cursor.execute("SELECT COUNT(*) FROM LOCALITY WHERE STATE = 'TEST'")
    locality_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM STREET_LOCALITY WHERE STATE = 'TEST'")
    street_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM ADDRESS_DETAIL WHERE STATE = 'TEST'")
    address_count = cursor.fetchone()[0]

    conn.close()

    assert locality_count == 3
    assert street_count == 3
    assert address_count == 3


def test_load_authority_codes(mock_data_dir, clean_test_db):
    """Test that authority code tables are loaded with correct row counts."""
    from create_database import create_database
    create_database(clean_test_db, reference_state='TEST')
    load_authority_codes(clean_test_db)

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()

    for table in AUTHORITY_CODE_TABLES:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        assert count == 3, f"Authority table {table} should have 3 rows, got {count}"

    conn.close()


def test_authority_codes_no_state_column_data(mock_data_dir, clean_test_db):
    """Test that authority code data has no STATE column and loads correctly."""
    from create_database import create_database
    create_database(clean_test_db, reference_state='TEST')
    load_authority_codes(clean_test_db)

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()

    # Verify ADDRESS_TYPE_AUT was loaded and has expected codes
    cursor.execute("SELECT CODE FROM ADDRESS_TYPE_AUT ORDER BY CODE")
    codes = [row[0] for row in cursor.fetchall()]
    assert 'R' in codes, "ADDRESS_TYPE_AUT should have code R"
    assert 'S' in codes, "ADDRESS_TYPE_AUT should have code S"

    # Verify no STATE column exists in authority tables
    cursor.execute("PRAGMA table_info(ADDRESS_TYPE_AUT)")
    col_names = [row[1] for row in cursor.fetchall()]
    assert 'STATE' not in col_names

    conn.close()


def test_load_all_19_state_tables(mock_data_dir, prepared_db):
    """Test that all 19 state tables are loaded with data for TEST state."""
    load_data('TEST', prepared_db)

    conn = sqlite3.connect(prepared_db)
    cursor = conn.cursor()

    for table in STATE_TABLES_ORDERED:
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE STATE = 'TEST'")
        count = cursor.fetchone()[0]
        assert count == 3, f"State table {table} should have 3 rows for TEST, got {count}"

    conn.close()


def test_state_column_populated(mock_data_dir, prepared_db):
    """Test that the STATE column is correctly populated for all state tables."""
    load_data('TEST', prepared_db)

    conn = sqlite3.connect(prepared_db)
    cursor = conn.cursor()

    for table in STATE_TABLES_ORDERED:
        cursor.execute(f"SELECT DISTINCT STATE FROM {table}")
        states = [row[0] for row in cursor.fetchall()]
        assert states == ['TEST'], (
            f"Table {table} should only have STATE='TEST', got {states}"
        )

    conn.close()


def test_load_order_dependency(mock_data_dir, prepared_db):
    """Test that tier 5 tables (depending on ADDRESS_DETAIL) load correctly after tier 4."""
    # Tier 5 tables that depend on ADDRESS_DETAIL
    tier5_tables = [
        'ADDRESS_ALIAS', 'ADDRESS_DEFAULT_GEOCODE', 'ADDRESS_FEATURE',
        'ADDRESS_MESH_BLOCK_2016', 'ADDRESS_MESH_BLOCK_2021', 'PRIMARY_SECONDARY'
    ]

    # Load all state data in order
    load_data('TEST', prepared_db)

    conn = sqlite3.connect(prepared_db)
    cursor = conn.cursor()

    for table in tier5_tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE STATE = 'TEST'")
        count = cursor.fetchone()[0]
        assert count == 3, f"Tier 5 table {table} should have 3 rows after loading, got {count}"

    conn.close()


def test_authority_codes_idempotent(mock_data_dir, clean_test_db):
    """Test that loading authority codes twice doesn't duplicate rows (INSERT OR IGNORE)."""
    from create_database import create_database
    create_database(clean_test_db, reference_state='TEST')

    load_authority_codes(clean_test_db)
    load_authority_codes(clean_test_db)  # Load again

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()

    for table in AUTHORITY_CODE_TABLES:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        assert count == 3, f"Authority table {table} should still have 3 rows after double load"

    conn.close()


def test_all_tables_have_data_after_full_load(mock_data_dir, prepared_db):
    """Test that all 35 tables have data after loading authority codes and state """
    load_data('TEST', prepared_db)

    conn = sqlite3.connect(prepared_db)
    cursor = conn.cursor()

    # All authority code tables should have data (loaded in prepared_db fixture)
    for table in AUTHORITY_CODE_TABLES:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        assert count > 0, f"Authority table {table} should have rows"

    # All state tables should have data
    for table in STATE_TABLES_ORDERED:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        assert count > 0, f"State table {table} should have rows"

    conn.close()


def test_address_detail_site_fk(mock_data_dir, prepared_db):
    """Test that ADDRESS_DETAIL.ADDRESS_SITE_PID references valid ADDRESS_SITE rows."""
    load_data('TEST', prepared_db)

    conn = sqlite3.connect(prepared_db)
    cursor = conn.cursor()

    # Verify ADDRESS_DETAIL references valid ADDRESS_SITE rows
    cursor.execute("""
        SELECT COUNT(*) FROM ADDRESS_DETAIL A
        JOIN ADDRESS_SITE S ON A.ADDRESS_SITE_PID = S.ADDRESS_SITE_PID
        WHERE A.STATE = 'TEST' AND A.ADDRESS_SITE_PID IS NOT NULL
    """)
    valid_site_refs = cursor.fetchone()[0]

    # All 3 rows in the fixture have ADDRESS_SITE_PID set
    assert valid_site_refs == 3, f"All ADDRESS_DETAIL rows should reference valid ADDRESS_SITE"

    conn.close()


def test_multi_state_load_isolation(mock_multi_state_dir, clean_test_db):
    """Test that loading two states doesn't mix data (STATE column isolates them)."""
    from create_database import create_database
    create_database(clean_test_db, reference_state='TEST')
    load_authority_codes(clean_test_db)
    load_data('TEST', clean_test_db)
    load_data('OT1', clean_test_db)

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT STATE FROM LOCALITY ORDER BY STATE")
    states = [row[0] for row in cursor.fetchall()]

    # Both states should be present
    assert 'TEST' in states
    assert 'OT1' in states

    # Each should have 3 rows
    cursor.execute("SELECT COUNT(*) FROM LOCALITY WHERE STATE = 'TEST'")
    assert cursor.fetchone()[0] == 3

    cursor.execute("SELECT COUNT(*) FROM LOCALITY WHERE STATE = 'OT1'")
    assert cursor.fetchone()[0] == 3

    conn.close()
