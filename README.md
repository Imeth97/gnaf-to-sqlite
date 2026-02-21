# GNAF to SQL

## Usage

```bash
./run_setup.sh <gnaf_base_path> [output_db_path]
```

**Example:**
```bash
./run_setup.sh 'g-naf_nov25_allstates_gda2020_psv_1021/G-NAF/G-NAF NOVEMBER 2025'
./run_setup.sh 'g-naf_nov25_allstates_gda2020_psv_1021/G-NAF/G-NAF NOVEMBER 2025' out/gnaf.sqlite
```

**Arguments:**
- `gnaf_base_path` - Path to the GNAF base directory (must contain 'Standard' and 'Authority Code' subdirectories)
- `output_db_path` - (Optional) Path where the SQLite database will be created (default: `out/gnaf.db`)

**Notes:**
- Process takes approximately 1 hour to complete
- No checkpointing - interruption requires restart from scratch
- Any existing database at output path will be overwritten 
