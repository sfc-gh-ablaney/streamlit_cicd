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


def get_requests(_session, database, schema):
    try:
        table_meta_sql = f"""SELECT * ,
                        CASE WHEN request_grant_statement IS NOT NULL and request_revoke_statement IS NULL THEN 'ACTIVE' 
                         WHEN request_grant_statement IS NOT NULL and request_revoke_statement IS NOT NULL THEN 'CLOSED' 
                         WHEN request_result = 'Decline' THEN 'CLOSED' 
                        ELSE 'OPEN' END STATUS 
                        FROM {database}.{schema}.ST_AR_ACCESS_REQUEST_LOG ;"""
        
        table_meta_df = _session.sql(table_meta_sql).to_pandas()
        return table_meta_df
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in get_requests(): " + str(e))


def get_users_from_requests(_session, database, schema):
    try:
        table_meta_sql = f"""SELECT DISTINCT REQUESTOR_USER_NAME FROM {database}.{schema}.ST_AR_ACCESS_REQUEST_LOG ;"""
        
        table_meta_df = _session.sql(table_meta_sql).to_pandas()
        return table_meta_df
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in get_requests(): " + str(e))



##snowflake connection info. This will get read in from the values submitted on the homepage
try:
    session = init_connection()
    #open the connection
except Exception as e:
        st.error("Connection Failed.  Please try again! The pages will not work unless a successfull connection is made" + '\n' + '\n' + "error: " + str(e))


user = get_user()



sf_database = session.get_current_database()
sf_schema = session.get_current_schema()


st.header("Audit Approvals")
st.write('')

df_users = get_users_from_requests(session, sf_database, sf_schema)

user_selected = st.selectbox('Select User', df_users)
status_select = st.multiselect('Select Status', ['ACTIVE', 'CLOSED', 'OPEN'], default=['ACTIVE','CLOSED', 'OPEN'])

df_requests = get_requests(session, sf_database, sf_schema)

df_col_list = list(df_requests)
df_col_list.remove("ID")
df_col_list.remove("CREATED_TS")
df_col_list.remove("CREATED_BY")
df_col_list.remove("UPDATED_TS")
df_col_list.remove("UPDATED_BY")


df_filter = df_requests.loc[(df_requests["REQUESTOR_USER_NAME"] == user_selected) & (df_requests["STATUS"].isin(status_select))].sort_values('CREATED_TS', ascending=False)


df_ = df_filter[df_filter['STATUS'] == 'ACTIVE']
slice_ = pd.IndexSlice[df_.index, df_.columns]

st.subheader('Requests for user')

def highlighter(row):

    if row['STATUS'] == 'ACTIVE':
        return  ['background-color: lightgreen'] * len(row)
    elif row['STATUS'] == 'CLOSED':
        return  ['background-color: pink'] * len(row)
    elif row['STATUS'] == 'OPEN':
        return  ['background-color: lightblue'] * len(row)


# sl = ['STATUS']    

st.dataframe(df_filter.style.apply(highlighter, axis=1), use_container_width=True,
        hide_index=True,
        column_order=df_col_list)


df_user_grants = get_user_grants(session, user_selected)
d = pd.DataFrame(df_user_grants)


df_ = d[d['role'].isin(df_filter['REQUESTED_ROLE_NAME'])]
slice_ = pd.IndexSlice[df_.index, df_.columns]
s = d.style.set_properties(**{'background-color': 'lightgreen'}, subset=slice_)

st.subheader('Current grants for user')
st.dataframe(s, use_container_width=True, hide_index=True)

refresh = st.button('refresh')
if refresh:
    df_user_grants = get_user_grants(session, user_selected)

