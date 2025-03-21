# Import python packages
import streamlit as st
import snowflake.snowpark
from snowflake.snowpark.context import get_active_session
import toml
from snowflake.snowpark.session import Session





st.set_page_config(page_title="Access Requestor", page_icon="📋", layout="wide")

@st.cache_resource
def init_connection():
    # Connection
    try:
        session = get_active_session()
    except:
        conn = st.connection("snowflake")
        session = conn.session()
    return session


##snowflake connection info. This will get read in from the values submitted on the homepage
try:
    session = init_connection()
    #open the connection
except Exception as e:
        st.error("Connection Failed.  Please try again! The pages will not work unless a successfull connection is made" + '\n' + '\n' + "error: " + str(e))


st.header('Requirements:')

st.markdown(f""" 
            APP request  
                -- user puts in request  
                -- request written to log table  
                -- approver notified  
            APP approve  
                -- approver reviews open requests and accepts/ declines  
                -- request approval written to log table  
          
            If access granted  
                -- access granted written to log table  (straight away or via task)
                
            **task runs every x mins to check if access needs granted or revoked   
                -- granted/revoked access written to log table""")  