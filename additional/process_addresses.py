import sqlite3
import os
import sys

# Constants
OUTPUT_FILE = 'prefixes/address_prefixes.txt'

def get_address_prefix(address_data):
    """
    Process address data and return a formatted address line.

    Args:
        address_data: A sqlite3.Row object containing address fields

    Returns:
        A string in format: "STATE | ADDRESS_DETAIL_PID | formatted-address"
    """
    # Extract fields from the Row object
    state = address_data['STATE']
    address_detail_pid = address_data['ADDRESS_DETAIL_PID']
    number_first = address_data['NUMBER_FIRST'] or ''
    number_first_suffix = address_data['NUMBER_FIRST_SUFFIX'] or ''
    street_name = address_data['STREET_NAME'] or ''
    street_type_code = address_data['STREET_TYPE_CODE'] or ''
    locality_name = address_data['LOCALITY_NAME'] or ''
    postcode = address_data['POSTCODE'] or ''

    # Format components to lowercase with hyphens
    street_name_formatted = street_name.lower().replace(" ", "-")
    street_type_code_formatted = street_type_code.lower().replace(" ", "-")
    locality_name_formatted = locality_name.lower().replace(" ", "-")
    state_formatted = state.lower() if state else 'act'

    # Build the formatted address
    formatted_address = f"{number_first}{number_first_suffix}-{street_name_formatted}-{street_type_code_formatted}-{locality_name_formatted}-{state_formatted}-{postcode}"

    # Return in format: STATE | ADDRESS_DETAIL_PID | formatted_address
    return f"{state} | {address_detail_pid} | {formatted_address}"

def process_all_addresses(database_path):
    """
    Process all addresses with related data and write results to a file.

    Args:
        database_path: Path to the SQLite database file
    """
    # Make sure the output directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    # Connect to the database
    conn = sqlite3.connect(database_path)
    
    # Configure connection to return rows as dictionaries
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Query with joins to get related data
    # Excludes apartments/flats/units (non-detached dwellings)
    query = """
    SELECT A.STATE, A.ADDRESS_DETAIL_PID, A.NUMBER_FIRST, A.NUMBER_FIRST_SUFFIX,
           S.STREET_NAME, S.STREET_TYPE_CODE, L.LOCALITY_NAME, A.POSTCODE
    FROM ADDRESS_DETAIL A
    LEFT JOIN STREET_LOCALITY S
        ON A.STREET_LOCALITY_PID = S.STREET_LOCALITY_PID
    LEFT JOIN LOCALITY L
        ON A.LOCALITY_PID = L.LOCALITY_PID
    WHERE A.FLAT_TYPE_CODE IS NULL
    """
    
    cursor.execute(query)

    # Open the output file
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as outfile:
        # Process each address row
        count = 0
        for row in cursor:
            # Get the address prefix
            prefix = get_address_prefix(row)

            # Write it to the file
            outfile.write(f"{prefix}\n")

            # Update counter and show progress
            count += 1
            if count % 10000 == 0:
                print(f"Processed {count} addresses...")
    
    # Close the database connection
    conn.close()
    
    print(f"Processing complete. {count} addresses written to {OUTPUT_FILE}")

if __name__ == "__main__":
    # Check for database path argument
    if len(sys.argv) < 2:
        print("Usage: python process_addresses.py <database_path>")
        print("Example: python process_addresses.py out/gnaf.db")
        sys.exit(1)

    database_path = sys.argv[1]

    # Validate database file exists
    if not os.path.exists(database_path):
        print(f"❌ Error: Database file not found: {database_path}")
        sys.exit(1)

    process_all_addresses(database_path) 