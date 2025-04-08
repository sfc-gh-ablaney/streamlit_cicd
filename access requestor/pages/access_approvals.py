# Import python packages
import streamlit as st
import snowflake.snowpark
from snowflake.snowpark.context import get_active_session
import toml
from snowflake.snowpark.session import Session
import pandas as pd
import datetime as dt
from datetime import timedelta
from common.queries import get_user, get_access_roles, get_requests, get_user_grants



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


def get_open_requests(_session, database, schema, user):
    try:
        table_meta_sql = f"""SELECT L.* FROM {database}.{schema}.ST_AR_ACCESS_REQUEST_LOG L
                            JOIN ST_AR_ROLE_APPROVERS A ON L.REQUESTED_ROLE_NAME = A.ROLE_NAME
                            WHERE L.REQUEST_RESULT IS  NULL
                            AND A.APPROVER_NAME = '{user}' ;"""
        
        table_meta_df = _session.sql(table_meta_sql).to_pandas()
        return table_meta_df
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in get_access_roles(): " + str(e))

def update_decision(_session, user, database, schema, id, decision):
    try:
        
        table_meta_sql = f"""UPDATE {database}.{schema}.ST_AR_ACCESS_REQUEST_LOG 
                                SET
                                REQUEST_REVIEWED_BY = '{user}',
                                REQUEST_RESULT = '{decision}',
                                REQUEST_REVIEWED_TS = CURRENT_TIMESTAMP(),
                                UPDATED_TS = CURRENT_TIMESTAMP(),
                                UPDATED_BY = '{user}'
                                WHERE ID = '{id}';"""
        table_meta_df = _session.sql(table_meta_sql).collect()
        return table_meta_df
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in get_access_roles(): " + str(e))



def grant_access(_session, database, schema, id, mins):
    try:
        call_proc_sql = f"""CALL {database}.{schema}.ACCESS_GRANTER({id});"""
        result = _session.sql(call_proc_sql).collect()

        st.session_state.end_task_ts = dt.datetime.now() + timedelta(minutes = mins)
        return result
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in grant_access(): " + str(e))


        

def datetime_to_cron(dt):

  return f"""{dt.minute} {dt.hour} {dt.day} {dt.strftime('%b')} *"""

def create_grant_task(_session, database, schema, row):
    try:
        call_proc_stmt_sql = f"""CALL {database}.{schema}.ACCESS_GRANTER({row["ID"]}) ;"""
        start_cron = datetime_to_cron(row["REQUESTED_START_DT"])
        task_name = f"""TSK_AR_GRANT_{row["REQUESTED_ROLE_NAME"]}_{row["REQUESTOR_USER_NAME"]}"""
        create_task_sql = f"""CREATE OR REPLACE TASK {task_name} WAREHOUSE = SNOW_WH SCHEDULE = 'USING CRON {start_cron} Pacific/Auckland' AS {call_proc_stmt_sql};"""
        result = _session.sql(create_task_sql).collect()
        resume_task_sql = f""" ALTER TASK {task_name} RESUME; """
        result = _session.sql(resume_task_sql).collect()
        return result
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in create_grant_task(): " + str(e))

def create_revoke_task(_session, database, schema, row, type):
    try:
        call_proc_stmt_sql = f"""CALL {database}.{schema}.ACCESS_REVOKER({row["ID"]}) ;"""

        # if start end
        if type == 'ts':
            end_cron = datetime_to_cron(row["REQUESTED_END_DT"])

        # else if mins
        elif type == 'mins':
            end_cron = datetime_to_cron(st.session_state.end_task_ts)

        task_name = f"""TSK_AR_REVOKE_{row["REQUESTED_ROLE_NAME"]}_{row["REQUESTOR_USER_NAME"]}"""
        create_task_sql = f"""CREATE OR REPLACE TASK {task_name} WAREHOUSE = SNOW_WH SCHEDULE = 'USING CRON {end_cron} Pacific/Auckland' AS {call_proc_stmt_sql};"""
        result = _session.sql(create_task_sql).collect()
        resume_task_sql = f""" ALTER TASK {task_name} RESUME; """;
        result = _session.sql(resume_task_sql).collect()
        return result
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in create_revoke_task(): " + str(e))


##snowflake connection info. This will get read in from the values submitted on the homepage
try:
    session = init_connection()
    #open the connection
except Exception as e:
        st.error("Connection Failed.  Please try again! The pages will not work unless a successfull connection is made" + '\n' + '\n' + "error: " + str(e))


user = get_user()

if 'warehouse' not in st.session_state:
    st.session_state.warehouse = 'SNOW_WH'

st.session_state.end_task_ts = ''

sf_database = session.get_current_database()
sf_schema = session.get_current_schema()


st.header("Access Approvals")
st.write('Please select the row you want to approve/decline:')

df_open_requests = get_open_requests(session, sf_database, sf_schema, user)

df_col_list = list(df_open_requests)
df_col_list.remove("ID")
df_col_list.remove("CREATED_TS")
df_col_list.remove("CREATED_BY")
df_col_list.remove("UPDATED_TS")
df_col_list.remove("UPDATED_BY")



event = st.dataframe(
        df_open_requests,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        column_order=df_col_list
    )

request = event.selection.rows
filtered_df = df_open_requests.iloc[request]


options = ["Approve", "Decline"]
decision = st.selectbox('Decision', options)

submit = st.button('Submit')

if submit:
    update_decision(session, user, sf_database, sf_schema, filtered_df.iloc[0]["ID"], decision)
    # minute based access
    if decision == 'Approve' and not pd.isna(filtered_df.iloc[0]["REQUESTED_TIME_PERIOD_MINS"]):
        result = grant_access(session, sf_database, sf_schema, filtered_df.iloc[0]["ID"],int(filtered_df.iloc[0]["REQUESTED_TIME_PERIOD_MINS"]))
        result_rv = create_revoke_task(session, sf_database, sf_schema, filtered_df.iloc[0], 'mins')
        st.write('access granted')
    # start end date access
    elif decision == 'Approve' and not pd.isna(filtered_df.iloc[0]["REQUESTED_START_DT"]):
        result_cr = create_grant_task(session, sf_database, sf_schema, filtered_df.iloc[0])
        result_rv = create_revoke_task(session, sf_database, sf_schema, filtered_df.iloc[0], 'ts')
        
    st.rerun()
  

# st.subheader('List of all requests')

# df_requests = get_requests(session, sf_database,sf_schema)
# event = st.dataframe(
#         df_requests,
#         use_container_width=True,
#         hide_index=True,
#         column_order=df_col_list
#     )
if not filtered_df.empty :
    df_user_grants = get_user_grants(session, filtered_df.iloc[0]["REQUESTOR_USER_NAME"])
    st.subheader('Users current grants:')
    st.dataframe(df_user_grants, hide_index=True)

    # df_open_requests

# change task name to be unique - Timestamp

# pick user - based off log table
# show grants requested
# highlight grants open
# show user's current grants

# add check for approval if start end date have passed