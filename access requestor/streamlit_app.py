# Import python packages
import streamlit as st
import snowflake.snowpark
from snowflake.snowpark.context import get_active_session
import toml
from snowflake.snowpark.session import Session
import datetime as dt
from datetime import timedelta
import pandas as pd

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

def get_approvers(_session, database, schema, role_name):
    try:
        table_sql = f"""SELECT * FROM {database}.{schema}.ST_AR_ROLE_APPROVERS 
                            WHERE ROLE_NAME = '{role_name}' """
        table_df = _session.sql(table_sql).to_pandas()
        return table_df
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in get_approvers(): " + str(e))

def email_approver(_session, df_approvers):
    try:
        # email_list = ''
        for index in range(len(df_approvers)):
            approver_email = df_approvers.iloc[index]
            # approver_email
            # desc_user_sql = F"""desc user {approver};"""
            # get_email_sql = F"""SELECT "value" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()) )WHERE "property" = 'EMAIL';"""
            # _session.sql(desc_user_sql).collect()
            # email_df =  _session.sql(get_email_sql).to_pandas() 
            # e= email_df["value"].iloc[0]
            
            send_email_sql = F"""   CALL SYSTEM$SEND_EMAIL(
                'approver_email_int',
                '{approver_email}',
                'Access to Snowflake Requested',
                'Please log into the app to review access request to Snowflake'
    )           ;  """
        result = _session.sql(send_email_sql).collect()
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in email_approver(): " + str(e))

##snowflake connection info. This will get read in from the values submitted on the homepage
try:
    session = init_connection()
    #open the connection
except Exception as e:
        st.error("Connection Failed.  Please try again! The pages will not work unless a successfull connection is made" + '\n' + '\n' + "error: " + str(e))


sf_database = session.get_current_database()
sf_schema = session.get_current_schema()






##add some markdown to the page with a desc
st.header("Access Requestor")
st.write('Please select role you require temporary access to:')
user = get_user()
st.write("Request for User: " +user)

# select box for roles requested
df_roles = get_access_roles(session, sf_database, sf_schema)
role_requested = st.selectbox('Role Requested', df_roles["ROLE_NAME"])



approvals_needed = df_roles["NUMBER_OF_APPROVALS"].loc[(df_roles["ROLE_NAME"] == role_requested) ].iloc[0]
st.write('Number of approvers required '+str(approvals_needed) )

df_approvers = get_approvers(session, sf_database, sf_schema, role_requested)

# st.dataframe(df_approvers['APPROVER_NAME'], hide_index=True)

# toggle for access type - minute or date/time based
access_type = st.toggle("Access Type")
# true = date/time
# false = mins
if access_type:
    st.write('dates')
    col1, col2 = st.columns(2)
    with col1:
        start_dt = st.date_input('Enter Start date:', value='today')
        end_dt = st.date_input('Enter End date:', value='today')
    with col2:
        start_t = dt.datetime.now() + dt.timedelta(minutes=5)
        end_t = dt.datetime.now() + dt.timedelta(minutes=35)
        start_ts = st.time_input('Enter Start time:', value=start_t, step= 60)
        end_ts = st.time_input('Enter End time:', value=end_t, step = 60)
else:
    st.write('minutes')
    mins_requested = st.number_input("Time Length Requested (minutes)",30,180, step=30, value=30)


# text area for request reason
request_reason = st.text_area('Enter request reason')
submit_request = st.button('Submit Request')

if submit_request:
    if access_type:
        start_dt_ts = dt.datetime.combine(start_dt, start_ts) 
        end_dt_ts = dt.datetime.combine(end_dt, end_ts) 
        if start_dt_ts <= dt.datetime.now() + timedelta(minutes = 5):
            st.error('request is in the past or less than in 5 minsutes time')
        elif end_dt_ts <= start_dt_ts:
            st.error('end time is before the start time')
        else:
            insert_request_dates(session, sf_database, sf_schema, user, role_requested, start_dt, start_ts, end_dt, end_ts, request_reason)
            st.success('Request Submitted')
            email_approver(session, df_approvers['APPROVER_EMAIL'])
    else:
        insert_request_mins(session, sf_database, sf_schema, user, role_requested, mins_requested, request_reason)
        st.success('Request Submitted')
        email_approver(session, df_approvers['APPROVER_EMAIL'])
    
    
# table of requests for user in last 30 days
st.subheader('Requests from user - last 30 days')
df_open_requests = get_open_requests_for_user(session, sf_database, sf_schema, user)
df_col_list = list(df_open_requests)
df_col_list.remove("ID")
df_col_list.remove("CREATED_TS")
df_col_list.remove("CREATED_BY")
df_col_list.remove("UPDATED_TS")
df_col_list.remove("UPDATED_BY")

st.dataframe(df_open_requests, hide_index=True,column_order=df_col_list)