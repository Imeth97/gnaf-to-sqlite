import sqlite3

def add_indexes(db_name='gnaf_addresses.db'):
    """Add performance indexes to existing database with 35 tables."""
    print("Creating indexes on database...")
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # STATE indexes for all 19 state tables (improves filtering by state)
    print("  Creating STATE indexes for all state tables (19)...")
    state_tables = [
        'STATE', 'MB_2016', 'MB_2021', 'LOCALITY', 'ADDRESS_SITE',
        'STREET_LOCALITY', 'LOCALITY_ALIAS', 'LOCALITY_NEIGHBOUR',
        'LOCALITY_POINT', 'ADDRESS_SITE_GEOCODE', 'ADDRESS_DETAIL',
        'STREET_LOCALITY_ALIAS', 'STREET_LOCALITY_POINT', 'ADDRESS_ALIAS',
        'ADDRESS_DEFAULT_GEOCODE', 'ADDRESS_FEATURE', 'ADDRESS_MESH_BLOCK_2016',
        'ADDRESS_MESH_BLOCK_2021', 'PRIMARY_SECONDARY'
    ]
    for table in state_tables:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table.lower()}_state ON {table}(STATE)")

    # Foreign key indexes for improved JOIN performance
    print("  Creating foreign key indexes for relationships...")

    # Tier 2 tables
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_locality_state_fk ON LOCALITY(STATE_PID)")

    # Tier 3 tables
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_street_locality_fk ON STREET_LOCALITY(LOCALITY_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_locality_alias_locality_fk ON LOCALITY_ALIAS(LOCALITY_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_locality_neighbour_locality_fk ON LOCALITY_NEIGHBOUR(LOCALITY_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_locality_neighbour_neighbour_fk ON LOCALITY_NEIGHBOUR(NEIGHBOUR_LOCALITY_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_locality_point_locality_fk ON LOCALITY_POINT(LOCALITY_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_address_site_geocode_site_fk ON ADDRESS_SITE_GEOCODE(ADDRESS_SITE_PID)")

    # Tier 4 tables
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_address_detail_locality_fk ON ADDRESS_DETAIL(LOCALITY_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_address_detail_street_fk ON ADDRESS_DETAIL(STREET_LOCALITY_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_address_detail_site_fk ON ADDRESS_DETAIL(ADDRESS_SITE_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_street_alias_street_fk ON STREET_LOCALITY_ALIAS(STREET_LOCALITY_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_street_point_street_fk ON STREET_LOCALITY_POINT(STREET_LOCALITY_PID)")

    # Tier 5 tables - all reference ADDRESS_DETAIL
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_address_alias_principal_fk ON ADDRESS_ALIAS(PRINCIPAL_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_address_alias_alias_fk ON ADDRESS_ALIAS(ALIAS_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_address_geocode_address_fk ON ADDRESS_DEFAULT_GEOCODE(ADDRESS_DETAIL_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_address_feature_address_fk ON ADDRESS_FEATURE(ADDRESS_DETAIL_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_address_mb2016_address_fk ON ADDRESS_MESH_BLOCK_2016(ADDRESS_DETAIL_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_address_mb2016_mb_fk ON ADDRESS_MESH_BLOCK_2016(MB_2016_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_address_mb2021_address_fk ON ADDRESS_MESH_BLOCK_2021(ADDRESS_DETAIL_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_address_mb2021_mb_fk ON ADDRESS_MESH_BLOCK_2021(MB_2021_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_primary_secondary_primary_fk ON PRIMARY_SECONDARY(PRIMARY_PID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_primary_secondary_secondary_fk ON PRIMARY_SECONDARY(SECONDARY_PID)")

    # Composite indexes for common query patterns
    print("  Creating composite indexes for common queries...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_address_state_postcode ON ADDRESS_DETAIL(STATE, POSTCODE)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_locality_state_name ON LOCALITY(STATE, LOCALITY_NAME)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_street_state_name ON STREET_LOCALITY(STATE, STREET_NAME)")

    conn.commit()
    conn.close()
    print("✓ All indexes created successfully (40+ indexes)")

if __name__ == "__main__":
    add_indexes()
