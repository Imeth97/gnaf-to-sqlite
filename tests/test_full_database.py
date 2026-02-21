"""Integration tests for the full GNAF database setup (35 tables)."""
import sqlite3
import pytest
from create_database import create_database, AUTHORITY_CODE_TABLES, STATE_TABLES_ORDERED
from load_data import load_data, load_authority_codes
from add_indexes import add_indexes


def test_full_database_creation(mock_data_dir, clean_test_db):
    """Test end-to-end database creation with all 35 tables."""
    create_database(clean_test_db, reference_state='TEST')

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
    count = cursor.fetchone()[0]
    conn.close()

    assert count == 35, f"Expected 35 tables, got {count}"


def test_authority_codes_load_once(mock_data_dir, clean_test_db):
    """Test that authority codes load correctly and only once (idempotent)."""
    create_database(clean_test_db, reference_state='TEST')
    load_authority_codes(clean_test_db)
    load_authority_codes(clean_test_db)  # Second call - should not duplicate

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()

    for table in AUTHORITY_CODE_TABLES:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        assert count == 3, f"Table {table} should have exactly 3 rows after double load, got {count}"

    conn.close()


def test_single_state_full_load(mock_data_dir, clean_test_db):
    """Test loading all 19 state tables for one state."""
    create_database(clean_test_db, reference_state='TEST')
    load_authority_codes(clean_test_db)
    load_data('TEST', clean_test_db)

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()

    for table in STATE_TABLES_ORDERED:
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE STATE = 'TEST'")
        count = cursor.fetchone()[0]
        assert count == 3, f"Table {table} should have 3 rows for TEST state, got {count}"

    conn.close()


def test_multi_state_load_differentiation(mock_multi_state_dir, clean_test_db):
    """Test loading two states and verify STATE column isolates data correctly."""
    create_database(clean_test_db, reference_state='TEST')
    load_authority_codes(clean_test_db)
    load_data('TEST', clean_test_db)
    load_data('OT1', clean_test_db)

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()

    # Verify both states present and separate
    for table in STATE_TABLES_ORDERED:
        cursor.execute(f"SELECT STATE, COUNT(*) FROM {table} GROUP BY STATE ORDER BY STATE")
        state_counts = dict(cursor.fetchall())
        assert 'TEST' in state_counts, f"{table} should have TEST data"
        assert 'OT1' in state_counts, f"{table} should have OT1 data"
        assert state_counts['TEST'] == 3
        assert state_counts['OT1'] == 3

    conn.close()


def test_all_indexes_created(mock_data_dir, clean_test_db):
    """Test that all performance indexes are created by add_indexes."""
    create_database(clean_test_db, reference_state='TEST')
    load_authority_codes(clean_test_db)
    load_data('TEST', clean_test_db)
    add_indexes(clean_test_db)

    conn = sqlite3.connect(clean_test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'")
    indexes = [row[0] for row in cursor.fetchall()]
    conn.close()

    # Should have a large number of indexes
    assert len(indexes) >= 30, f"Expected at least 30 indexes, got {len(indexes)}"

    # Verify specific critical indexes exist
    assert any('locality_state' in idx for idx in indexes), "Should have LOCALITY STATE index"
    assert any('address_detail' in idx.lower() and 'state' in idx.lower() for idx in indexes), (
        "Should have ADDRESS_DETAIL STATE index"
    )
    assert any('address_alias' in idx.lower() for idx in indexes), "Should have ADDRESS_ALIAS indexes"


def test_query_all_relationships(full_test_db):
    """Test a complex multi-table query joining 8+ tables across all dependency tiers."""
    conn = sqlite3.connect(full_test_db)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            A.ADDRESS_DETAIL_PID,
            S.STATE_ABBREVIATION,
            L.LOCALITY_NAME,
            SL.STREET_NAME,
            SL.STREET_TYPE_CODE,
            SITE.ADDRESS_SITE_NAME,
            ADG.GEOCODE_TYPE_CODE,
            AMB16.MB_2016_PID,
            AMB21.MB_2021_PID
        FROM ADDRESS_DETAIL A
        JOIN LOCALITY L ON A.LOCALITY_PID = L.LOCALITY_PID
        JOIN STATE S ON L.STATE_PID = S.STATE_PID
        JOIN STREET_LOCALITY SL ON A.STREET_LOCALITY_PID = SL.STREET_LOCALITY_PID
        JOIN ADDRESS_SITE SITE ON A.ADDRESS_SITE_PID = SITE.ADDRESS_SITE_PID
        JOIN ADDRESS_DEFAULT_GEOCODE ADG ON A.ADDRESS_DETAIL_PID = ADG.ADDRESS_DETAIL_PID
        JOIN ADDRESS_MESH_BLOCK_2016 AMB16 ON A.ADDRESS_DETAIL_PID = AMB16.ADDRESS_DETAIL_PID
        JOIN ADDRESS_MESH_BLOCK_2021 AMB21 ON A.ADDRESS_DETAIL_PID = AMB21.ADDRESS_DETAIL_PID
        WHERE A.STATE = 'TEST'
        ORDER BY A.ADDRESS_DETAIL_PID
    """)
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 3, f"Expected 3 rows from 8-table join, got {len(rows)}"
    assert rows[0][0] == 'GANTEST001'
    assert rows[0][2] == 'TESTVILLE'
    assert rows[0][3] == 'TEST'


def test_data_integrity_constraints(full_test_db):
    """Test that no orphaned records exist across all FK relationships."""
    conn = sqlite3.connect(full_test_db)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    # Verify ADDRESS_ALIAS has no orphaned PRINCIPAL_PID references
    cursor.execute("""
        SELECT COUNT(*) FROM ADDRESS_ALIAS AA
        LEFT JOIN ADDRESS_DETAIL A ON AA.PRINCIPAL_PID = A.ADDRESS_DETAIL_PID
        WHERE AA.STATE = 'TEST' AND A.ADDRESS_DETAIL_PID IS NULL
    """)
    orphaned = cursor.fetchone()[0]
    assert orphaned == 0, "No orphaned PRINCIPAL_PID references in ADDRESS_ALIAS"

    # Verify ADDRESS_MESH_BLOCK_2016 has no orphaned MB_2016_PID references
    cursor.execute("""
        SELECT COUNT(*) FROM ADDRESS_MESH_BLOCK_2016 AMB
        LEFT JOIN MB_2016 MB ON AMB.MB_2016_PID = MB.MB_2016_PID
        WHERE AMB.STATE = 'TEST' AND MB.MB_2016_PID IS NULL
    """)
    orphaned = cursor.fetchone()[0]
    assert orphaned == 0, "No orphaned MB_2016_PID references in ADDRESS_MESH_BLOCK_2016"

    # Verify LOCALITY_NEIGHBOUR has no orphaned NEIGHBOUR_LOCALITY_PID references
    cursor.execute("""
        SELECT COUNT(*) FROM LOCALITY_NEIGHBOUR LN
        LEFT JOIN LOCALITY L ON LN.NEIGHBOUR_LOCALITY_PID = L.LOCALITY_PID
        WHERE LN.STATE = 'TEST' AND L.LOCALITY_PID IS NULL
    """)
    orphaned = cursor.fetchone()[0]
    assert orphaned == 0, "No orphaned NEIGHBOUR_LOCALITY_PID references in LOCALITY_NEIGHBOUR"

    conn.close()
