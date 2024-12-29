from hbase import HBase
import json

def decode_dict(d):
    # Decode the dictionary by stripping 'info:' from the column family and decoding the value
    return {k.split(':', 1)[1]: v.decode('utf-8') for k, v in d.items()}

def get_last_record_from_hbase():
    # Create a connection to HBase
    connection = HBase('localhost', 9090)  # Update with the correct HBase connection settings
    
    # Ensure the table exists
    if 'smartphone' not in connection.get_tables():
        print("Table 'smartphone' does not exist.")
        return None
    
    # Reference the 'smartphone' table
    table = connection.table('smartphone')

    # Initialize a scanner to fetch the latest record (HBase scan is by row key order)
    try:
        # Scan in reverse order to get the most recent entry
        scanner = table.scan(limit=1, reverse=True)

        # Initialize the result
        last_record = {}

        for key, data in scanner:
            # Decode the HBase result
            last_record = decode_dict(data)
            break  # Only take the first (last) record

        if not last_record:
            print("No records found.")
            return None

        return last_record

    except Exception as e:
        print(f"Error fetching last record from HBase: {e}")
        return None

    finally:
        # Close the connection
        connection.close()

# Example usage:
if __name__ == "__main__":
    last_record = get_last_record_from_hbase()
    if last_record:
        print(f"Last record from HBase: {json.dumps(last_record, indent=4)}")
