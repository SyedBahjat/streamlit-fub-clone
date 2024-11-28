import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import json
import time

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

        chat_transcript = []
        for timestamp, status, message in chats:
            if status == "Received":
                chat_transcript.append({"timestamp": timestamp, "role": "client", "message": message})
            else:
                chat_transcript.append({"timestamp": timestamp, "role": "sales_rep", "message": message})

        if client_info:
            client_name, client_id, assigned_employee_name = client_info
        else:
            client_name = "Client"
            assigned_employee_name = "Sales Rep"

        cur.close()
        conn.close()

        return chat_transcript, client_name, assigned_employee_name

    except Exception as e:
        st.error(f"Error fetching chat data for client ID {client_id}: {e}")
        return [], None, None

# Function to simulate a streaming response (sales rep)
def response_generator(message):
    response = f"Thank you for reaching out, {message}. I will get back to you soon."
    for word in response.split():
        yield word + " "
        time.sleep(0.1)

# Function to display chat UI with custom styling
def display_chat_ui(chat_transcript, client_name, assigned_employee_name):
    # Inject CSS to make chat bubbles
    st.markdown("""
    <style>
        .chat-box {
            border-radius: 8px;
            padding: 10px;
            margin-bottom: 10px;
            display: flex;
            flex-direction: column;
        }
        .client-message {
            background-color: #d1f7ff;
            align-self: flex-start;
            max-width: 60%;
            margin-bottom: 5px;
            padding: 10px;
            border-radius: 12px;
        }
        .sales-rep-message {
            background-color: #f3f3f3;
            align-self: flex-end;
            max-width: 60%;
            margin-bottom: 5px;
            padding: 10px;
            border-radius: 12px;
        }
        .timestamp {
            font-size: 0.8em;
            color: gray;
        }
    </style>
    """, unsafe_allow_html=True)

    # Display chat history
    for chat in chat_transcript:
        role = chat["role"]
        timestamp = chat["timestamp"]
        message = chat["message"]

        if role == "client":
            st.markdown(f"""
            <div class="chat-box">
                <div class="client-message">
                    <b>{client_name}</b> <span class="timestamp">({timestamp})</span>
                    <p>{message}</p>
                </div>
            </div>
            """, unsafe_allow_html=True)
        elif role == "sales_rep":
            st.markdown(f"""
            <div class="chat-box">
                <div class="sales-rep-message">
                    <b>{assigned_employee_name}</b> <span class="timestamp">({timestamp})</span>
                    <p>{message}</p>
                </div>
            </div>
            """, unsafe_allow_html=True)

    # Input box for new messages
    prompt = st.chat_input("Type your message...")

    if prompt:
        # Add new user message (client message) to chat history
        st.session_state.messages.append({"role": "client", "content": prompt})
        st.markdown(f"""
        <div class="chat-box">
            <div class="client-message">
                <b>{client_name}</b> <span class="timestamp">(Just now)</span>
                <p>{prompt}</p>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Display sales rep's simulated response (streaming)
        with st.chat_message("sales_rep"):
            for word in response_generator(prompt):
                st.markdown(word, unsafe_allow_html=True)
                st.experimental_rerun()  # To stream progressively

        # Add sales rep's response to the session state
        response = f"Thank you for reaching out, {prompt}. I will get back to you soon."
        st.session_state.messages.append({"role": "sales_rep", "content": response})

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
            chat_transcript, client_name, assigned_employee_name = fetch_chat_data(db_params, client_id)
            st.title(f"Chat with {client_name}")
            st.markdown(f"Assigned Employee: {assigned_employee_name}")

            if chat_transcript:
                display_chat_ui(chat_transcript, client_name, assigned_employee_name)
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
