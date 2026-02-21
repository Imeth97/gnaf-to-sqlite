import os
import sqlite3
import pytest
from create_database import create_database
from load_data import load_data, load_authority_codes


@pytest.fixture
def loaded_db(mock_data_dir, clean_test_db):
    """Create a fully populated test database."""
    create_database(clean_test_db, data_dir=mock_data_dir, authority_code_dir=mock_data_dir, reference_state='TEST')
    load_authority_codes(clean_test_db, authority_code_dir=mock_data_dir)
    load_data('TEST', clean_test_db, data_dir=mock_data_dir)
    return clean_test_db


def test_address_references_locality(loaded_db):
    """Test that ADDRESS_DETAIL correctly references LOCALITY."""
    conn = sqlite3.connect(loaded_db)
    cursor = conn.cursor()

    # Query ADDRESS_DETAIL with its LOCALITY_PID
    cursor.execute("""
        SELECT A.ADDRESS_DETAIL_PID, A.LOCALITY_PID, L.LOCALITY_NAME
        FROM ADDRESS_DETAIL A
        JOIN LOCALITY L ON A.LOCALITY_PID = L.LOCALITY_PID
        WHERE A.ADDRESS_DETAIL_PID = 'GANTEST001'
    """)
    row = cursor.fetchone()

    conn.close()

    assert row is not None
    assert row[0] == 'GANTEST001'
    assert row[1] == 'loc_test_001'
    assert row[2] == 'TESTVILLE'


def test_address_references_street(loaded_db):
    """Test that ADDRESS_DETAIL correctly references STREET_LOCALITY."""
    conn = sqlite3.connect(loaded_db)
    cursor = conn.cursor()

    # Query ADDRESS_DETAIL with its STREET_LOCALITY_PID
    cursor.execute("""
        SELECT A.ADDRESS_DETAIL_PID, A.STREET_LOCALITY_PID, S.STREET_NAME, S.STREET_TYPE_CODE
        FROM ADDRESS_DETAIL A
        JOIN STREET_LOCALITY S ON A.STREET_LOCALITY_PID = S.STREET_LOCALITY_PID
        WHERE A.ADDRESS_DETAIL_PID = 'GANTEST002'
    """)
    row = cursor.fetchone()

    conn.close()

    assert row is not None
    assert row[0] == 'GANTEST002'
    assert row[1] == 'TST0000002'
    assert row[2] == 'SAMPLE'
    assert row[3] == 'AVENUE'


def test_street_references_locality(loaded_db):
    """Test that STREET_LOCALITY correctly references LOCALITY."""
    conn = sqlite3.connect(loaded_db)
    cursor = conn.cursor()

    # Query STREET_LOCALITY with its LOCALITY_PID
    cursor.execute("""
        SELECT S.STREET_LOCALITY_PID, S.LOCALITY_PID, L.LOCALITY_NAME
        FROM STREET_LOCALITY S
        JOIN LOCALITY L ON S.LOCALITY_PID = L.LOCALITY_PID
        WHERE S.STREET_LOCALITY_PID = 'TST0000003'
    """)
    row = cursor.fetchone()

    conn.close()

    assert row is not None
    assert row[0] == 'TST0000003'
    assert row[1] == 'loc_test_003'
    assert row[2] == 'MOCKVILLE'


def test_join_queries(loaded_db):
    """Test that LEFT JOIN queries return correct data across all three tables."""
    conn = sqlite3.connect(loaded_db)
    cursor = conn.cursor()

    # Perform a three-table join similar to process_addresses_with_joins.py
    cursor.execute("""
        SELECT A.NUMBER_FIRST, S.STREET_NAME, S.STREET_TYPE_CODE, L.LOCALITY_NAME, A.POSTCODE
        FROM ADDRESS_DETAIL A
        LEFT JOIN STREET_LOCALITY S ON A.STREET_LOCALITY_PID = S.STREET_LOCALITY_PID
        LEFT JOIN LOCALITY L ON A.LOCALITY_PID = L.LOCALITY_PID
        WHERE A.STATE = 'TEST'
        ORDER BY A.ADDRESS_DETAIL_PID
    """)
    rows = cursor.fetchall()

    conn.close()

    assert len(rows) == 3

    # Verify first row
    assert rows[0][0] == '1'  # NUMBER_FIRST
    assert rows[0][1] == 'TEST'  # STREET_NAME
    assert rows[0][2] == 'STREET'  # STREET_TYPE_CODE
    assert rows[0][3] == 'TESTVILLE'  # LOCALITY_NAME
    assert rows[0][4] == '2000'  # POSTCODE

    # Verify second row
    assert rows[1][0] == '2'
    assert rows[1][1] == 'SAMPLE'
    assert rows[1][2] == 'AVENUE'
    assert rows[1][3] == 'SAMPLETOWN'
    assert rows[1][4] == '2100'

    # Verify third row
    assert rows[2][0] == '3'
    assert rows[2][1] == 'MOCK'
    assert rows[2][2] == 'ROAD'
    assert rows[2][3] == 'MOCKVILLE'
    assert rows[2][4] == '2200'


def test_invalid_fk_fails(loaded_db):
    """Test that inserting invalid foreign key values raises IntegrityError."""
    conn = sqlite3.connect(loaded_db)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    # Try to insert ADDRESS_DETAIL with non-existent LOCALITY_PID
    with pytest.raises(sqlite3.IntegrityError):
        cursor.execute("""
            INSERT INTO ADDRESS_DETAIL
            (STATE, ADDRESS_DETAIL_PID, NUMBER_FIRST, STREET_LOCALITY_PID, LOCALITY_PID, POSTCODE)
            VALUES ('TEST', 'GANTEST999', '999', 'TST0000001', 'loc_nonexistent', '9999')
        """)

    # Try to insert ADDRESS_DETAIL with non-existent STREET_LOCALITY_PID
    with pytest.raises(sqlite3.IntegrityError):
        cursor.execute("""
            INSERT INTO ADDRESS_DETAIL
            (STATE, ADDRESS_DETAIL_PID, NUMBER_FIRST, STREET_LOCALITY_PID, LOCALITY_PID, POSTCODE)
            VALUES ('TEST', 'GANTEST998', '998', 'TST_nonexistent', 'loc_test_001', '9998')
        """)

    # Note: STREET_LOCALITY does not have a FK constraint to LOCALITY in current schema,
    # so we don't test that here

    conn.close()


def test_cascade_integrity(loaded_db):
    """Test that FK constraints prevent orphaned records."""
    conn = sqlite3.connect(loaded_db)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    # Try to delete a LOCALITY that is referenced by ADDRESS_DETAIL
    # This should fail or cascade depending on FK constraints
    # Since our schema doesn't define CASCADE, this should fail
    with pytest.raises(sqlite3.IntegrityError):
        cursor.execute("DELETE FROM LOCALITY WHERE LOCALITY_PID = 'loc_test_001' AND STATE = 'TEST'")

    conn.close()


def test_all_addresses_have_references(loaded_db):
    """Test that all ADDRESS_DETAIL rows have valid foreign key references."""
    conn = sqlite3.connect(loaded_db)
    cursor = conn.cursor()

    # Count addresses
    cursor.execute("SELECT COUNT(*) FROM ADDRESS_DETAIL WHERE STATE = 'TEST'")
    total_addresses = cursor.fetchone()[0]

    # Count addresses with valid LOCALITY references
    cursor.execute("""
        SELECT COUNT(*)
        FROM ADDRESS_DETAIL A
        JOIN LOCALITY L ON A.LOCALITY_PID = L.LOCALITY_PID
        WHERE A.STATE = 'TEST'
    """)
    valid_locality_refs = cursor.fetchone()[0]

    # Count addresses with valid STREET_LOCALITY references
    cursor.execute("""
        SELECT COUNT(*)
        FROM ADDRESS_DETAIL A
        JOIN STREET_LOCALITY S ON A.STREET_LOCALITY_PID = S.STREET_LOCALITY_PID
        WHERE A.STATE = 'TEST'
    """)
    valid_street_refs = cursor.fetchone()[0]

    conn.close()

    # All addresses should have valid references
    assert total_addresses == 3
    assert valid_locality_refs == 3
    assert valid_street_refs == 3


def test_left_join_handles_nulls(loaded_db):
    """Test that LEFT JOINs properly handle NULL foreign keys."""
    conn = sqlite3.connect(loaded_db)
    cursor = conn.cursor()

    # Insert an ADDRESS_DETAIL with NULL STREET_LOCALITY_PID (if schema allows)
    # Note: Our current schema might not allow this, so this is a theoretical test
    # For now, we'll just verify that LEFT JOIN returns all rows

    cursor.execute("""
        SELECT COUNT(*)
        FROM ADDRESS_DETAIL A
        LEFT JOIN STREET_LOCALITY S ON A.STREET_LOCALITY_PID = S.STREET_LOCALITY_PID
        WHERE A.STATE = 'TEST'
    """)
    left_join_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM ADDRESS_DETAIL WHERE STATE = 'TEST'")
    address_count = cursor.fetchone()[0]

    conn.close()

    # LEFT JOIN should return all ADDRESS_DETAIL rows
    assert left_join_count == address_count == 3


def test_locality_to_state_relationship(loaded_db):
    """Test that LOCALITY correctly references STATE via STATE_PID."""
    conn = sqlite3.connect(loaded_db)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT L.LOCALITY_PID, L.LOCALITY_NAME, S.STATE_ABBREVIATION
        FROM LOCALITY L
        JOIN STATE S ON L.STATE_PID = S.STATE_PID
        WHERE L.STATE = 'TEST'
        ORDER BY L.LOCALITY_PID
    """)
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 3, "All 3 localities should join to STATE"
    for row in rows:
        assert row[2] == 'TEST', "All localities should reference the TEST state"


def test_address_detail_to_address_site_join(loaded_db):
    """Test that ADDRESS_DETAIL correctly references ADDRESS_SITE."""
    conn = sqlite3.connect(loaded_db)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT A.ADDRESS_DETAIL_PID, A.ADDRESS_SITE_PID, S.ADDRESS_SITE_NAME
        FROM ADDRESS_DETAIL A
        JOIN ADDRESS_SITE S ON A.ADDRESS_SITE_PID = S.ADDRESS_SITE_PID
        WHERE A.STATE = 'TEST'
        ORDER BY A.ADDRESS_DETAIL_PID
    """)
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 3
    assert rows[0][1] == 'site_001'
    assert rows[1][1] == 'site_002'
    assert rows[2][1] == 'site_003'


def test_address_alias_self_referential(loaded_db):
    """Test that ADDRESS_ALIAS references ADDRESS_DETAIL twice (principal and alias PIDs)."""
    conn = sqlite3.connect(loaded_db)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT AA.ADDRESS_ALIAS_PID, P.ADDRESS_DETAIL_PID AS PRINCIPAL, A.ADDRESS_DETAIL_PID AS ALIAS
        FROM ADDRESS_ALIAS AA
        JOIN ADDRESS_DETAIL P ON AA.PRINCIPAL_PID = P.ADDRESS_DETAIL_PID
        JOIN ADDRESS_DETAIL A ON AA.ALIAS_PID = A.ADDRESS_DETAIL_PID
        WHERE AA.STATE = 'TEST'
        ORDER BY AA.ADDRESS_ALIAS_PID
    """)
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 3
    assert rows[0][1] == 'GANTEST001' and rows[0][2] == 'GANTEST002'
    assert rows[1][1] == 'GANTEST002' and rows[1][2] == 'GANTEST003'
    assert rows[2][1] == 'GANTEST003' and rows[2][2] == 'GANTEST001'


def test_primary_secondary_self_referential(loaded_db):
    """Test that PRIMARY_SECONDARY references ADDRESS_DETAIL twice."""
    conn = sqlite3.connect(loaded_db)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT PS.PRIMARY_SECONDARY_PID, P.ADDRESS_DETAIL_PID AS PRIMARY_ADDR, S.ADDRESS_DETAIL_PID AS SECONDARY_ADDR
        FROM PRIMARY_SECONDARY PS
        JOIN ADDRESS_DETAIL P ON PS.PRIMARY_PID = P.ADDRESS_DETAIL_PID
        JOIN ADDRESS_DETAIL S ON PS.SECONDARY_PID = S.ADDRESS_DETAIL_PID
        WHERE PS.STATE = 'TEST'
        ORDER BY PS.PRIMARY_SECONDARY_PID
    """)
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 3


def test_locality_neighbour_self_referential(loaded_db):
    """Test that LOCALITY_NEIGHBOUR references LOCALITY table twice."""
    conn = sqlite3.connect(loaded_db)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT LN.LOCALITY_NEIGHBOUR_PID, L1.LOCALITY_NAME AS LOC, L2.LOCALITY_NAME AS NEIGHBOUR
        FROM LOCALITY_NEIGHBOUR LN
        JOIN LOCALITY L1 ON LN.LOCALITY_PID = L1.LOCALITY_PID
        JOIN LOCALITY L2 ON LN.NEIGHBOUR_LOCALITY_PID = L2.LOCALITY_PID
        WHERE LN.STATE = 'TEST'
        ORDER BY LN.LOCALITY_NEIGHBOUR_PID
    """)
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 3
    assert rows[0][1] == 'TESTVILLE' and rows[0][2] == 'SAMPLETOWN'
    assert rows[1][1] == 'SAMPLETOWN' and rows[1][2] == 'MOCKVILLE'
    assert rows[2][1] == 'MOCKVILLE' and rows[2][2] == 'TESTVILLE'


def test_mesh_block_2016_relationships(loaded_db):
    """Test that ADDRESS_MESH_BLOCK_2016 correctly references ADDRESS_DETAIL and MB_2016."""
    conn = sqlite3.connect(loaded_db)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT AMB.ADDRESS_MESH_BLOCK_2016_PID, A.ADDRESS_DETAIL_PID, MB.MB_2016_CODE
        FROM ADDRESS_MESH_BLOCK_2016 AMB
        JOIN ADDRESS_DETAIL A ON AMB.ADDRESS_DETAIL_PID = A.ADDRESS_DETAIL_PID
        JOIN MB_2016 MB ON AMB.MB_2016_PID = MB.MB_2016_PID
        WHERE AMB.STATE = 'TEST'
        ORDER BY AMB.ADDRESS_MESH_BLOCK_2016_PID
    """)
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 3
    assert rows[0][1] == 'GANTEST001'
    assert rows[1][1] == 'GANTEST002'
    assert rows[2][1] == 'GANTEST003'


def test_mesh_block_2021_relationships(loaded_db):
    """Test that ADDRESS_MESH_BLOCK_2021 correctly references ADDRESS_DETAIL and MB_2021."""
    conn = sqlite3.connect(loaded_db)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT AMB.ADDRESS_MESH_BLOCK_2021_PID, A.ADDRESS_DETAIL_PID, MB.MB_2021_CODE
        FROM ADDRESS_MESH_BLOCK_2021 AMB
        JOIN ADDRESS_DETAIL A ON AMB.ADDRESS_DETAIL_PID = A.ADDRESS_DETAIL_PID
        JOIN MB_2021 MB ON AMB.MB_2021_PID = MB.MB_2021_PID
        WHERE AMB.STATE = 'TEST'
        ORDER BY AMB.ADDRESS_MESH_BLOCK_2021_PID
    """)
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 3


def test_authority_code_references(loaded_db):
    """Test that state tables can join to authority code reference tables."""
    conn = sqlite3.connect(loaded_db)
    cursor = conn.cursor()

    # LOCALITY_ALIAS references LOCALITY_ALIAS_TYPE_AUT
    cursor.execute("""
        SELECT LA.LOCALITY_ALIAS_PID, AUT.NAME AS ALIAS_TYPE_NAME
        FROM LOCALITY_ALIAS LA
        JOIN LOCALITY_ALIAS_TYPE_AUT AUT ON LA.ALIAS_TYPE_CODE = AUT.CODE
        WHERE LA.STATE = 'TEST'
    """)
    rows = cursor.fetchall()
    assert len(rows) == 3, "All 3 locality aliases should join to authority code"

    # ADDRESS_DEFAULT_GEOCODE references GEOCODE_TYPE_AUT
    cursor.execute("""
        SELECT ADG.ADDRESS_DEFAULT_GEOCODE_PID, AUT.NAME AS GEOCODE_TYPE
        FROM ADDRESS_DEFAULT_GEOCODE ADG
        JOIN GEOCODE_TYPE_AUT AUT ON ADG.GEOCODE_TYPE_CODE = AUT.CODE
        WHERE ADG.STATE = 'TEST'
    """)
    rows = cursor.fetchall()
    assert len(rows) == 3, "All 3 geocodes should join to authority code"

    conn.close()


def test_complex_multi_tier_join(loaded_db):
    """Test a complex 5-tier join spanning all dependency levels."""
    conn = sqlite3.connect(loaded_db)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            A.ADDRESS_DETAIL_PID,
            S.STATE_ABBREVIATION,
            L.LOCALITY_NAME,
            SL.STREET_NAME,
            SITE.ADDRESS_SITE_NAME,
            ADG.GEOCODE_TYPE_CODE
        FROM ADDRESS_DETAIL A
        JOIN LOCALITY L ON A.LOCALITY_PID = L.LOCALITY_PID
        JOIN STATE S ON L.STATE_PID = S.STATE_PID
        JOIN STREET_LOCALITY SL ON A.STREET_LOCALITY_PID = SL.STREET_LOCALITY_PID
        JOIN ADDRESS_SITE SITE ON A.ADDRESS_SITE_PID = SITE.ADDRESS_SITE_PID
        JOIN ADDRESS_DEFAULT_GEOCODE ADG ON A.ADDRESS_DETAIL_PID = ADG.ADDRESS_DETAIL_PID
        WHERE A.STATE = 'TEST'
        ORDER BY A.ADDRESS_DETAIL_PID
    """)
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 3, "All 3 addresses should appear in complex join"
    assert rows[0][0] == 'GANTEST001'
    assert rows[0][2] == 'TESTVILLE'
    assert rows[0][3] == 'TEST'


def test_street_locality_alias_relationships(loaded_db):
    """Test that STREET_LOCALITY_ALIAS correctly references STREET_LOCALITY."""
    conn = sqlite3.connect(loaded_db)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT SLA.STREET_LOCALITY_ALIAS_PID, SL.STREET_NAME, AUT.NAME AS ALIAS_TYPE
        FROM STREET_LOCALITY_ALIAS SLA
        JOIN STREET_LOCALITY SL ON SLA.STREET_LOCALITY_PID = SL.STREET_LOCALITY_PID
        JOIN STREET_LOCALITY_ALIAS_TYPE_AUT AUT ON SLA.ALIAS_TYPE_CODE = AUT.CODE
        WHERE SLA.STATE = 'TEST'
        ORDER BY SLA.STREET_LOCALITY_ALIAS_PID
    """)
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 3
    assert rows[0][1] == 'TEST'
    assert rows[1][1] == 'SAMPLE'
    assert rows[2][1] == 'MOCK'


def test_orphaned_foreign_keys_rejected(loaded_db):
    """Test that inserting rows with non-existent FK values is rejected."""
    conn = sqlite3.connect(loaded_db)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    # Try to insert ADDRESS_ALIAS with non-existent PRINCIPAL_PID
    with pytest.raises(sqlite3.IntegrityError):
        cursor.execute("""
            INSERT INTO ADDRESS_ALIAS (STATE, ADDRESS_ALIAS_PID, PRINCIPAL_PID, ALIAS_PID, ALIAS_TYPE_CODE)
            VALUES ('TEST', 'FAKE_ALIAS', 'NONEXISTENT_PID', 'GANTEST001', 'RA')
        """)

    # Try to insert into LOCALITY_NEIGHBOUR with non-existent NEIGHBOUR_LOCALITY_PID
    with pytest.raises(sqlite3.IntegrityError):
        cursor.execute("""
            INSERT INTO LOCALITY_NEIGHBOUR (STATE, LOCALITY_NEIGHBOUR_PID, LOCALITY_PID, NEIGHBOUR_LOCALITY_PID)
            VALUES ('TEST', 'FAKE_NEIGHBOUR', 'loc_test_001', 'NONEXISTENT_LOC')
        """)

    conn.close()
