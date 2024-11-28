import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import json

# Load the database connection string from the environment
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

# Ensure that DATABASE_URL is loaded correctly
if DATABASE_URL is None:
    raise ValueError("DATABASE_URL is not set in the environment variables.")

# Establish database connection using SQLAlchemy
def get_db_connection():
    engine = create_engine(DATABASE_URL)
    return engine.connect()

# Function to fetch client and employee data
def get_client_data():
    # Get the current time and the time from 4 days ago
    now = datetime.now()
    time_4_days_ago = now - timedelta(days=4)
    
    # Query to fetch the required data
    query = """
    SELECT 
        cp.client_id, 
        cp.current_stage, 
        cp.created_on, 
        c.fullname AS client_fullname, 
        c.fphone1, 
        c.addresses,
        e.fullname AS assigned_employee_fullname
    FROM client_stage_progression cp
    JOIN client c ON cp.client_id = c.id
    LEFT JOIN employee e ON c.assigned_employee = e.id
    WHERE cp.current_stage > 4
    AND cp.created_on > %s;
    """
    
    # Convert the 4-day time to string format for the query
    time_4_days_ago_str = time_4_days_ago.strftime('%Y-%m-%d %H:%M:%S')

    # Fetch data from the database
    conn = get_db_connection()
    try:
        df = pd.read_sql(query, conn, params=(time_4_days_ago_str,))
    finally:
        conn.close()

    return df

# Process the client data to extract address information and handle unique client_ids
def process_data(df):
    # Extracting city, state, and street from the addresses column
    def extract_address(addresses):
        if isinstance(addresses, str):
            try:
                # Convert string to JSON list if needed
                addresses = json.loads(addresses)
            except json.JSONDecodeError:
                return "", "", ""
        
        if isinstance(addresses, list) and len(addresses) > 0:
            address = addresses[0]
            city = address.get("city", "")
            state = address.get("state", "")
            street = address.get("street", "")
            return city, state, street
        return "", "", ""

    # Add the extracted address columns to the DataFrame
    df[['city', 'state', 'street']] = df['addresses'].apply(extract_address).apply(pd.Series)

    # Remove duplicates based on client_id (Ensure no repetition of client_id)
    df_unique_clients = df.drop_duplicates(subset=['client_id'])

    return df_unique_clients

# Display the data in Streamlit with clickable phone numbers
def display_clients(df):
    # Streamlit display setup
    st.title("Clients with Current Stage > 4 (Last 4 Days)")
    
    if df.empty:
        st.write("No clients found matching the criteria.")
    else:
        # Add a clickable phone link column to the dataframe
        df.loc[:, 'phone_link'] = df['fphone1'].apply(lambda x: f'<a href="tel:{x}">{x}</a>' if pd.notnull(x) else '')
        
        # Create the HTML table
        html_table = df[['client_fullname', 'phone_link', 'assigned_employee_fullname', 'city', 'state', 'street']].to_html(escape=False, index=False)

        # Use st.markdown to render the HTML table with clickable links
        st.markdown(html_table, unsafe_allow_html=True)

# Main function to run the app
def main():
    # Get the data from the database
    df = get_client_data()
    
    # Process the data to extract necessary details and ensure uniqueness
    df_processed = process_data(df)
    
    # Display the data in Streamlit
    display_clients(df_processed)

if __name__ == '__main__':
    main()
