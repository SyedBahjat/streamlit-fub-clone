import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import json

# Streamlit page setup
st.set_page_config(page_title="Client Data", page_icon="📊", layout="wide")

# Load database credentials from Streamlit secrets
db_params = {
    'dbname': st.secrets["database"]["DB_NAME"],
    'user': st.secrets["database"]["DB_USER"],
    'password': st.secrets["database"]["DB_PASSWORD"],
    'host': st.secrets["database"]["DB_HOST"],
    'port': st.secrets["database"]["DB_PORT"]
}

# Function to establish connection and fetch data using psycopg2
def fetch_data(query, params=None):
    connection = None
    cursor = None
    try:
        # Connect to the database
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        cursor.execute(query, params)
        records = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(records, columns=column_names)
        return df
    except Exception as error:
        st.error(f"Error fetching records: {error}")
        return pd.DataFrame()  # Return an empty DataFrame on error
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Function to fetch client data for current_stage > 4
def get_client_data_stage_greater_than_4():
    # Get the current time and the time from 4 days ago
    now = datetime.now()
    time_4_days_ago = now - timedelta(days=4)
    
    # Query to fetch the required data for current_stage > 4
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
    return fetch_data(query, params=(time_4_days_ago_str,))

# Function to fetch client data for current_stage < 4
def get_client_data_stage_less_than_4():
    # Get the current time and the time from 4 days ago
    now = datetime.now()
    time_4_days_ago = now - timedelta(days=4)
    
    # Query to fetch the required data for current_stage < 4
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
    WHERE cp.current_stage < 4
    AND cp.created_on > %s;
    """
    
    # Convert the 4-day time to string format for the query
    time_4_days_ago_str = time_4_days_ago.strftime('%Y-%m-%d %H:%M:%S')

    # Fetch data from the database
    return fetch_data(query, params=(time_4_days_ago_str,))

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
def display_clients(df, title):
    # Streamlit display setup
    st.subheader(title)
    
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
    # Get the data for clients with current_stage > 4 and current_stage < 4
    df_stage_greater_than_4 = get_client_data_stage_greater_than_4()
    df_stage_less_than_4 = get_client_data_stage_less_than_4()
    
    # Process the data to extract necessary details and ensure uniqueness
    df_processed_greater_than_4 = process_data(df_stage_greater_than_4)
    df_processed_less_than_4 = process_data(df_stage_less_than_4)
    
    # Display the data in Streamlit
    display_clients(df_processed_greater_than_4, "Clients with Current Stage > 4 (Last 4 Days)")
    display_clients(df_processed_less_than_4, "Clients with Current Stage < 4 (Last 4 Days)")

if __name__ == '__main__':
    main()
