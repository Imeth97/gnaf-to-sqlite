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

## Post-Processing: Freestanding Property Addresses

After the database build completes, you'll be prompted with two options:
1. Exit now
2. Generate list of freestanding property addresses

If you choose option 2, the script will automatically:
- Process all addresses from the database (excluding apartments/flats/units)
- Generate formatted address strings in the format: `STATE | ADDRESS_DETAIL_PID | formatted-address`
- Save the output to `prefixes/address_prefixes.txt`

**Example output format:**
```
NSW | GANSW123456 | 42-smith-street-sydney-nsw-2000
VIC | GAVIC789012 | 15a-jones-road-melbourne-vic-3000
```

This file contains only detached/freestanding properties (addresses where `FLAT_TYPE_CODE IS NULL`), making it useful for identifying standalone residential properties. 
