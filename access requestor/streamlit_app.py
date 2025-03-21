# Import python packages
import streamlit as st
import snowflake.snowpark
from snowflake.snowpark.context import get_active_session
import toml
from snowflake.snowpark.session import Session
import datetime as dt

from common.queries import get_user, get_access_roles

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



def insert_request_mins(session, database, schema, user, role_requested, mins_requested, request_reason):
    try:
        insert_request_sql = f"""INSERT INTO {database}.{schema}.ST_AR_ACCESS_REQUEST_LOG 
                    (REQUESTOR_USER_NAME, REQUESTED_ROLE_NAME, ROLE_REQUESTED_REASON, REQUESTED_TIME_PERIOD_MINS, CREATED_BY)
                        VALUES
                    ('{user}', '{role_requested}', '{request_reason}', {mins_requested}, '{user}');"""
        result = session.sql(insert_request_sql).collect()
        # return result
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in insert_request(): " + str(e))

def insert_request_dates(session, database, schema, user, role_requested, start_dt, start_ts, end_dt, end_ts, request_reason):
    try:
        start_datetime = dt.datetime.combine(start_dt, start_ts)
        end_datetime = dt.datetime.combine(end_dt, end_ts)
        
        insert_request_sql = f"""INSERT INTO {database}.{schema}.ST_AR_ACCESS_REQUEST_LOG 
                    (REQUESTOR_USER_NAME, REQUESTED_ROLE_NAME, ROLE_REQUESTED_REASON, REQUESTED_START_DT, REQUESTED_END_DT, CREATED_BY)
                        VALUES
                    ('{user}', '{role_requested}', '{request_reason}', '{start_datetime}','{end_datetime}' , '{user}');"""
        result = session.sql(insert_request_sql).collect()
        return result
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in insert_request(): " + str(e))


def get_open_requests_for_user(_session, database, schema, user):
    try:
        table_meta_sql = f"""SELECT * FROM {database}.{schema}.ST_AR_ACCESS_REQUEST_LOG 
                            WHERE REQUESTOR_USER_NAME = '{user}'  
                            AND CREATED_TS >= dateadd(day, -30, current_date())
                            ORDER BY CREATED_TS DESC"""
        table_meta_df = _session.sql(table_meta_sql).to_pandas()
        return table_meta_df
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in get_access_roles(): " + str(e))

def email_requestor():
    try:
        send_email_sql = F"""   CALL SYSTEM$SEND_EMAIL(
            'email_int',
            'andy.blaney@snowflake.com',
            'Access to Snowflake Requested',
            'Please log in here to review access request to Snowflake'
)           ;  """
        result = session.sql(send_email_sql).collect()
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in email_requestor(): " + str(e))

##snowflake connection info. This will get read in from the values submitted on the homepage
try:
    session = init_connection()
    #open the connection
except Exception as e:
        st.error("Connection Failed.  Please try again! The pages will not work unless a successfull connection is made" + '\n' + '\n' + "error: " + str(e))


sf_database = session.get_current_database()
sf_schema = session.get_current_schema()

# sf_database
# sf_schema

##add some markdown to the page with a desc
st.header("Access Requestor")
st.write('Please select role you require temporary access to:')
user = get_user()
st.write("Request for User: " +user)

df_roles = get_access_roles(session, sf_database, sf_schema)

role_requested = st.selectbox('Role Requested', df_roles["ROLE_NAME"])

on = st.toggle("Access Type")

if on:
    st.write('dates')
    col1, col2 = st.columns(2)
    with col1:
        start_dt = st.date_input('Enter Start date:', value='today')
        end_dt = st.date_input('Enter End date:', value='today', step= 1)
    with col2:
        start_ts = st.time_input('Enter Start time:', value='now')
        end_ts = st.time_input('Enter End time:', value='now', step = 1)
else:
    st.write('minutes')
    mins_requested = st.number_input("Time Length Requested (minutes)",1,180, step=1, value=1)





request_reason = st.text_area('Enter request reason')
submit_request = st.button('Submit Request')

if submit_request:
    if on:
        insert_request_dates(session, sf_database, sf_schema, user, role_requested, start_dt, start_ts, end_dt, end_ts, request_reason)
    else:
        insert_request_mins(session, sf_database, sf_schema, user, role_requested, mins_requested, request_reason)
    st.success('Request Submitted')
    email_requestor()

st.subheader('Requests from user - last 30 days')
df_open_requests = get_open_requests_for_user(session, sf_database, sf_schema, user)
df_col_list = list(df_open_requests)
df_col_list.remove("ID")
df_col_list.remove("CREATED_TS")
df_col_list.remove("CREATED_BY")
df_col_list.remove("UPDATED_TS")
df_col_list.remove("UPDATED_BY")

st.dataframe(df_open_requests, hide_index=True,column_order=df_col_list)