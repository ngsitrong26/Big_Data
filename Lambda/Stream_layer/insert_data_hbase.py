from datetime import datetime
import happybase

def insert_data_hbase(url, text, metadata):
    # Create a connection to HBase
    connection = happybase.Connection('localhost', 9090)  # Adjust HBase connection settings if needed

    # Get the list of tables (convert to string to avoid potential byte issues)
    tables = [table.decode('utf-8') for table in connection.tables()]

    # Check if the 'website_data' table exists
    if 'website_data' not in tables:
        # Create a table if it does not exist
        column_families = {
            'info': {},
            'metadata': {}
        }
        connection.create_table('website_data', column_families)
    
    # Reference the 'website_data' table
    table = connection.table('website_data')

    # Generate a unique row key based on URL and current timestamp
    row_key = f"{url}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

    # Prepare the data to be inserted into HBase (convert keys and values to bytes)
    data_to_insert = {
        b'info:date': datetime.now().strftime('%Y-%m-%d %H:%M:%S').encode('utf-8'),
        b'info:text': text.encode('utf-8'),  # Full text of the website content
        b'metadata:title': metadata['title'].encode('utf-8'),
        b'metadata:description': metadata['description'].encode('utf-8'),
        b'metadata:keywords': metadata['keywords'].encode('utf-8'),
    }

    # Insert the data into HBase
    table.put(row_key.encode('utf-8'), data_to_insert)

    print(f"Data inserted into HBase for URL: {url}")

    connection.close()
