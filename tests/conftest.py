import os
import sys
import tempfile
import shutil
import pytest

# Add the parent directory to sys.path so we can import from data/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Disable progress tracker during tests to avoid cluttering test output
os.environ['GNAF_SHOW_PROGRESS'] = '0'


@pytest.fixture
def test_fixtures_dir():
    """Return the path to test fixture PSV files."""
    return os.path.join(os.path.dirname(__file__), 'fixtures')


@pytest.fixture
def tmp_db_path(tmp_path):
    """Provide a temporary database file path that gets cleaned up automatically."""
    db_file = tmp_path / "test_addresses.db"
    yield str(db_file)
    # Cleanup happens automatically with tmp_path


@pytest.fixture
def mock_data_dir(test_fixtures_dir, monkeypatch):
    """Mock DATA_DIR and AUTHORITY_CODE_DIR constants to point to test fixtures."""
    import create_database as create_db
    import load_data as load_data_module

    monkeypatch.setattr(create_db, 'DATA_DIR', test_fixtures_dir)
    monkeypatch.setattr(create_db, 'AUTHORITY_CODE_DIR', test_fixtures_dir)
    monkeypatch.setattr(load_data_module, 'DATA_DIR', test_fixtures_dir)
    monkeypatch.setattr(load_data_module, 'AUTHORITY_CODE_DIR', test_fixtures_dir)

    return test_fixtures_dir


@pytest.fixture
def clean_test_db():
    """Create a temporary database that gets cleaned up after the test."""
    db_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    db_path = db_file.name
    db_file.close()

    yield db_path

    # Cleanup
    if os.path.exists(db_path):
        os.remove(db_path)


@pytest.fixture
def full_test_db(mock_data_dir, clean_test_db):
    """Create a fully populated test database: schema + authority codes + TEST state """
    from create_database import create_database
    from load_data import load_authority_codes, load_data

    create_database(clean_test_db, reference_state='TEST')
    load_authority_codes(clean_test_db)
    load_data('TEST', clean_test_db)

    return clean_test_db


# Substitutions applied to TEST_ fixture content when creating OT1_ copies.
# Ensures all primary keys are unique so INSERT OR IGNORE doesn't silently skip OT1 rows.
_OT1_PID_REPLACEMENTS = [
    # More specific patterns first to avoid partial-string collisions
    ('loc_test_001', 'loc_ot1_001'),
    ('loc_test_002', 'loc_ot1_002'),
    ('loc_test_003', 'loc_ot1_003'),
    ('GANTEST001',   'GANOT1_001'),
    ('GANTEST002',   'GANOT1_002'),
    ('GANTEST003',   'GANOT1_003'),
    ('TST0000001',   'OT10000001'),
    ('TST0000002',   'OT10000002'),
    ('TST0000003',   'OT10000003'),
    ('sitegeo_001',  'ot1sgeo_001'),
    ('sitegeo_002',  'ot1sgeo_002'),
    ('sitegeo_003',  'ot1sgeo_003'),
    ('site_001',     'ot1site_001'),
    ('site_002',     'ot1site_002'),
    ('site_003',     'ot1site_003'),
    ('MB16_001',     'MB16OT1_001'),
    ('MB16_002',     'MB16OT1_002'),
    ('MB16_003',     'MB16OT1_003'),
    ('MB21_001',     'MB21OT1_001'),
    ('MB21_002',     'MB21OT1_002'),
    ('MB21_003',     'MB21OT1_003'),
    ('amb16_001',    'ot1amb16_001'),
    ('amb16_002',    'ot1amb16_002'),
    ('amb16_003',    'ot1amb16_003'),
    ('amb21_001',    'ot1amb21_001'),
    ('amb21_002',    'ot1amb21_002'),
    ('amb21_003',    'ot1amb21_003'),
    ('addralias_001', 'ot1alias_001'),
    ('addralias_002', 'ot1alias_002'),
    ('addralias_003', 'ot1alias_003'),
    ('adg_001',      'ot1adg_001'),
    ('adg_002',      'ot1adg_002'),
    ('adg_003',      'ot1adg_003'),
    ('feat_001',     'ot1feat_001'),
    ('feat_002',     'ot1feat_002'),
    ('feat_003',     'ot1feat_003'),
    ('localis_001',  'ot1localis_001'),
    ('localis_002',  'ot1localis_002'),
    ('localis_003',  'ot1localis_003'),
    ('locneigh_001', 'ot1locneigh_001'),
    ('locneigh_002', 'ot1locneigh_002'),
    ('locneigh_003', 'ot1locneigh_003'),
    ('locpt_001',    'ot1locpt_001'),
    ('locpt_002',    'ot1locpt_002'),
    ('locpt_003',    'ot1locpt_003'),
    ('slalias_001',  'ot1slal_001'),
    ('slalias_002',  'ot1slal_002'),
    ('slalias_003',  'ot1slal_003'),
    ('slpt_001',     'ot1slpt_001'),
    ('slpt_002',     'ot1slpt_002'),
    ('slpt_003',     'ot1slpt_003'),
    ('ps_001',       'ot1ps_001'),
    ('ps_002',       'ot1ps_002'),
    ('ps_003',       'ot1ps_003'),
    # STATE_PID: now uses string PIDs to avoid ambiguous integer replacement
    ('STEST001', 'SOT1_001'),
    ('STEST002', 'SOT1_002'),
    ('STEST003', 'SOT1_003'),
]


@pytest.fixture
def multi_state_fixtures_dir(test_fixtures_dir, tmp_path):
    """Create a temp directory with TEST and OT1 fixture files for multi-state tests.

    Copies all Authority_Code_* and TEST_* fixtures, and creates OT1_* copies
    of all TEST_* state fixtures with PID substitutions to avoid PK collisions.
    """
    for fname in os.listdir(test_fixtures_dir):
        src = os.path.join(test_fixtures_dir, fname)
        dst = os.path.join(str(tmp_path), fname)
        shutil.copy(src, dst)
        if fname.startswith('TEST_'):
            ot1_fname = 'OT1_' + fname[5:]
            with open(src, 'r', encoding='utf-8') as f:
                content = f.read()
            for old, new in _OT1_PID_REPLACEMENTS:
                content = content.replace(old, new)
            with open(os.path.join(str(tmp_path), ot1_fname), 'w', encoding='utf-8') as f:
                f.write(content)

    return str(tmp_path)


@pytest.fixture
def mock_multi_state_dir(multi_state_fixtures_dir, monkeypatch):
    """Mock DATA_DIR and AUTHORITY_CODE_DIR to the multi-state fixtures directory."""
    import create_database as create_db
    import load_data as load_data_module

    monkeypatch.setattr(create_db, 'DATA_DIR', multi_state_fixtures_dir)
    monkeypatch.setattr(create_db, 'AUTHORITY_CODE_DIR', multi_state_fixtures_dir)
    monkeypatch.setattr(load_data_module, 'DATA_DIR', multi_state_fixtures_dir)
    monkeypatch.setattr(load_data_module, 'AUTHORITY_CODE_DIR', multi_state_fixtures_dir)

    return multi_state_fixtures_dir
