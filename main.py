import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import json

# Database connection parameters (using Streamlit secrets for secure connection)
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
    now = datetime.now()
    time_4_days_ago = now - timedelta(days=4)
    
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
    
    time_4_days_ago_str = time_4_days_ago.strftime('%Y-%m-%d %H:%M:%S')

    return fetch_data(query, params=(time_4_days_ago_str,))

# Function to fetch client data for current_stage < 4
def get_client_data_stage_less_than_4():
    now = datetime.now()
    time_4_days_ago = now - timedelta(days=4)
    
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
    
    time_4_days_ago_str = time_4_days_ago.strftime('%Y-%m-%d %H:%M:%S')

    return fetch_data(query, params=(time_4_days_ago_str,))

# Process the client data to extract address information and handle unique client_ids
def process_data(df):
    def extract_address(addresses):
        if isinstance(addresses, str):
            try:
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

    df[['city', 'state', 'street']] = df['addresses'].apply(extract_address).apply(pd.Series)

    # Remove duplicates based on client_id
    df_unique_clients = df.drop_duplicates(subset=['client_id'])

    return df_unique_clients

# Function to fetch chat data from the database
def fetch_chat_data(db_params, client_id):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        chat_query = """
            SELECT TO_CHAR(created, 'HH12:MM DD/MM/YYYY'), status, message
            FROM public.textmessage
            WHERE client_id = %s
            ORDER BY created ASC;
        """
        cur.execute(chat_query, (client_id,))
        chats = cur.fetchall()

        client_query = """
            SELECT fullname, id, assigned_employee_name
            FROM public.client
            WHERE id = %s;
        """
        cur.execute(client_query, (client_id,))
        client_info = cur.fetchone()

        chat_transcript = ""
        for timestamp, status, message in chats:
            if status == "Received":
                chat_transcript += f"[{timestamp}] Client: {message}\n"
            else:
                chat_transcript += f"[{timestamp}] Sales Rep: {message}\n"

        if client_info:
            client_name, client_id, assigned_employee_name = client_info
        else:
            client_name = "Client"
            client_id = None
            assigned_employee_name = None

        client_url = f"<https://services.followupboss.com/2/people/view/{client_id}|{client_name}>"

        cur.close()
        conn.close()

        return chat_transcript, client_name, client_url, assigned_employee_name

    except Exception as e:
        st.error(f"Error fetching chat data for client ID {client_id}: {e}")
        return "", "", "#", None

# Display the client data in Streamlit with clickable phone numbers and client links
def display_clients(df, title):
    st.subheader(title)
    
    if df.empty:
        st.write("No clients found matching the criteria.")
    else:
        df['client_url'] = df['client_id'].apply(lambda x: f'<a href="/?client_id={x}">{x}</a>')

        df.loc[:, 'phone_link'] = df['fphone1'].apply(lambda x: f'<a href="tel:{x}">{x}</a>' if pd.notnull(x) else '')
        
        html_table = df[['client_fullname', 'phone_link', 'client_url', 'assigned_employee_fullname', 'city', 'state', 'street']].to_html(escape=False, index=False)
        st.markdown(html_table, unsafe_allow_html=True)

# Main function to run the app
def main():
    # Get client_id from URL query params
    query_params = st.experimental_get_query_params()  # Get all query parameters
    client_id_str = query_params.get('client_id', [None])[0]

    if client_id_str:
        # Show chat transcript for selected client_id
        try:
            client_id = int(client_id_str)
            chat_transcript, client_name, client_url, assigned_employee_name = fetch_chat_data(db_params, client_id)
            st.title(f"Chat with {client_name}")
            st.markdown(f"Client: {client_url}")
            st.markdown(f"Assigned Employee: {assigned_employee_name}")

            if chat_transcript:
                st.text_area("Chat Transcript", value=chat_transcript, height=400, disabled=True)
            else:
                st.write("No chat history available.")
        except ValueError:
            st.error("Invalid client ID.")
    else:
        # Show client data tables
        df_stage_greater_than_4 = get_client_data_stage_greater_than_4()
        df_stage_less_than_4 = get_client_data_stage_less_than_4()

        df_processed_greater_than_4 = process_data(df_stage_greater_than_4)
        df_processed_less_than_4 = process_data(df_stage_less_than_4)

        display_clients(df_processed_greater_than_4, "Clients with Current Stage > 4 (Last 4 Days)")
        display_clients(df_processed_less_than_4, "Clients with Current Stage < 4 (Last 4 Days)")

if __name__ == '__main__':
    main()
