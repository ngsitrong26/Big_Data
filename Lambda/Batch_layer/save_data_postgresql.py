import pandas as pd
from sqlalchemy import create_engine

def save_data(url, text, metadata):
    # Define columns for the website data
    columns = ['url', 'text', 'title', 'description', 'keywords']
    
    # Prepare the data to be stored (text and metadata)
    website_data = {
        'url': url,
        'text': text,
        'title': metadata['title'],
        'description': metadata['description'],
        'keywords': metadata['keywords']
    }

    # Convert data to DataFrame
    website_df = pd.DataFrame([website_data], columns=columns)

    # Create PostgreSQL engine
    password = 'Mop-391811'
    database = 'my_database'
    engine = create_engine('postgresql://postgres:{password}@localhost:5432/{my_database}')

    # Save data to PostgreSQL (replace table if it exists)
    website_df.to_sql('WebsiteData', engine, if_exists='replace', index=False)

    print("Website data stored in PostgreSQL")
