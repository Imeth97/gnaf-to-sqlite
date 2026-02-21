"""Tests to validate that all 35 test fixture files are correctly structured."""
import os
import csv
import pytest
from create_database import AUTHORITY_CODE_TABLES, STATE_TABLES_ORDERED


FIXTURES_DIR = os.path.join(os.path.dirname(__file__), 'fixtures')


def test_all_35_fixture_files_exist():
    """Test that all 35 required fixture files exist."""
    missing = []

    # Check 16 authority code fixtures
    for table in AUTHORITY_CODE_TABLES:
        fname = f"Authority_Code_{table}_psv.psv"
        fpath = os.path.join(FIXTURES_DIR, fname)
        if not os.path.exists(fpath):
            missing.append(fname)

    # Check 19 state table fixtures
    for table in STATE_TABLES_ORDERED:
        fname = f"TEST_{table}_psv.psv"
        fpath = os.path.join(FIXTURES_DIR, fname)
        if not os.path.exists(fpath):
            missing.append(fname)

    assert not missing, f"Missing fixture files: {missing}"


def test_fixture_format_valid():
    """Test that all fixture files are valid PSV format with pipe delimiter."""
    for table in AUTHORITY_CODE_TABLES:
        fpath = os.path.join(FIXTURES_DIR, f"Authority_Code_{table}_psv.psv")
        with open(fpath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='|')
            header = next(reader)
            assert len(header) >= 2, f"{table} should have at least 2 columns in header"
            rows = list(reader)
            non_empty = [r for r in rows if any(r)]
            assert len(non_empty) == 3, (
                f"Authority fixture {table} should have 3 data rows, got {len(non_empty)}"
            )

    for table in STATE_TABLES_ORDERED:
        fpath = os.path.join(FIXTURES_DIR, f"TEST_{table}_psv.psv")
        with open(fpath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='|')
            header = next(reader)
            assert len(header) >= 2, f"{table} should have at least 2 columns in header"
            rows = list(reader)
            non_empty = [r for r in rows if any(r)]
            assert len(non_empty) == 3, (
                f"State fixture {table} should have 3 data rows, got {len(non_empty)}"
            )


def test_authority_fixtures_no_state_prefix():
    """Test that authority code fixture files are named with 'Authority_Code_' prefix."""
    for table in AUTHORITY_CODE_TABLES:
        fname = f"Authority_Code_{table}_psv.psv"
        fpath = os.path.join(FIXTURES_DIR, fname)
        assert os.path.exists(fpath), f"Authority fixture should be named '{fname}'"
        # Verify the opposite - there should be no TEST_{table} variant
        state_fname = f"TEST_{table}_psv.psv"
        state_fpath = os.path.join(FIXTURES_DIR, state_fname)
        assert not os.path.exists(state_fpath), (
            f"Authority table {table} should not have a TEST_ prefixed fixture"
        )


def test_state_fixtures_have_state_prefix():
    """Test that state table fixture files are named with 'TEST_' prefix."""
    for table in STATE_TABLES_ORDERED:
        fname = f"TEST_{table}_psv.psv"
        fpath = os.path.join(FIXTURES_DIR, fname)
        assert os.path.exists(fpath), f"State fixture should be named '{fname}'"
        # Verify the opposite - there should be no Authority_Code_{table} variant
        auth_fname = f"Authority_Code_{table}_psv.psv"
        auth_fpath = os.path.join(FIXTURES_DIR, auth_fname)
        assert not os.path.exists(auth_fpath), (
            f"State table {table} should not have an Authority_Code_ prefixed fixture"
        )


def test_fixture_foreign_keys_valid():
    """Test that fixture data has internally consistent foreign key references."""
    # Load fixture data into dicts for cross-referencing
    def load_fixture(filename):
        fpath = os.path.join(FIXTURES_DIR, filename)
        with open(fpath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='|')
            return list(reader)

    locality_rows = load_fixture('TEST_LOCALITY_psv.psv')
    street_rows = load_fixture('TEST_STREET_LOCALITY_psv.psv')
    address_rows = load_fixture('TEST_ADDRESS_DETAIL_psv.psv')
    state_rows = load_fixture('TEST_STATE_psv.psv')
    address_site_rows = load_fixture('TEST_ADDRESS_SITE_psv.psv')
    address_alias_rows = load_fixture('TEST_ADDRESS_ALIAS_psv.psv')
    mb2016_rows = load_fixture('TEST_MB_2016_psv.psv')
    mb2021_rows = load_fixture('TEST_MB_2021_psv.psv')
    mesh2016_rows = load_fixture('TEST_ADDRESS_MESH_BLOCK_2016_psv.psv')
    mesh2021_rows = load_fixture('TEST_ADDRESS_MESH_BLOCK_2021_psv.psv')

    # Build PID sets
    locality_pids = {r['LOCALITY_PID'] for r in locality_rows}
    street_pids = {r['STREET_LOCALITY_PID'] for r in street_rows}
    address_pids = {r['ADDRESS_DETAIL_PID'] for r in address_rows}
    state_pids = {r['STATE_PID'] for r in state_rows}
    site_pids = {r['ADDRESS_SITE_PID'] for r in address_site_rows}
    mb2016_pids = {r['MB_2016_PID'] for r in mb2016_rows}
    mb2021_pids = {r['MB_2021_PID'] for r in mb2021_rows}

    # Verify ADDRESS_DETAIL foreign keys
    for row in address_rows:
        assert row['LOCALITY_PID'] in locality_pids, (
            f"ADDRESS_DETAIL.LOCALITY_PID={row['LOCALITY_PID']} not in LOCALITY fixture"
        )
        assert row['STREET_LOCALITY_PID'] in street_pids, (
            f"ADDRESS_DETAIL.STREET_LOCALITY_PID={row['STREET_LOCALITY_PID']} not in STREET_LOCALITY fixture"
        )
        if row.get('ADDRESS_SITE_PID'):
            assert row['ADDRESS_SITE_PID'] in site_pids, (
                f"ADDRESS_DETAIL.ADDRESS_SITE_PID={row['ADDRESS_SITE_PID']} not in ADDRESS_SITE fixture"
            )

    # Verify STREET_LOCALITY references LOCALITY
    for row in street_rows:
        assert row['LOCALITY_PID'] in locality_pids, (
            f"STREET_LOCALITY.LOCALITY_PID={row['LOCALITY_PID']} not in LOCALITY fixture"
        )

    # Verify LOCALITY references STATE
    for row in locality_rows:
        assert row['STATE_PID'] in state_pids, (
            f"LOCALITY.STATE_PID={row['STATE_PID']} not in STATE fixture"
        )

    # Verify ADDRESS_ALIAS references ADDRESS_DETAIL (both columns)
    for row in address_alias_rows:
        assert row['PRINCIPAL_PID'] in address_pids, (
            f"ADDRESS_ALIAS.PRINCIPAL_PID={row['PRINCIPAL_PID']} not in ADDRESS_DETAIL fixture"
        )
        assert row['ALIAS_PID'] in address_pids, (
            f"ADDRESS_ALIAS.ALIAS_PID={row['ALIAS_PID']} not in ADDRESS_DETAIL fixture"
        )

    # Verify ADDRESS_MESH_BLOCK_2016 references ADDRESS_DETAIL and MB_2016
    for row in mesh2016_rows:
        assert row['ADDRESS_DETAIL_PID'] in address_pids
        assert row['MB_2016_PID'] in mb2016_pids

    # Verify ADDRESS_MESH_BLOCK_2021 references ADDRESS_DETAIL and MB_2021
    for row in mesh2021_rows:
        assert row['ADDRESS_DETAIL_PID'] in address_pids
        assert row['MB_2021_PID'] in mb2021_pids
