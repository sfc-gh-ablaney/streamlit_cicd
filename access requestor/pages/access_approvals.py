# Import python packages
import streamlit as st
import snowflake.snowpark
from snowflake.snowpark.context import get_active_session
import toml
from snowflake.snowpark.session import Session

from common.queries import get_user, get_access_roles, get_requests



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


def get_open_requests(_session, database, schema):
    try:
        table_meta_sql = f"""SELECT * FROM {database}.{schema}.ST_AR_ACCESS_REQUEST_LOG 
                            WHERE REQUEST_RESULT IS  NULL"""
        
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

def grant_access(_session, database, schema, id):
    try:
        call_proc_sql = f"""CALL {database}.{schema}.ACCESS_GRANTER({id});"""
        result = _session.sql(call_proc_sql).collect()

        return result
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in grant_access(): " + str(e))


##snowflake connection info. This will get read in from the values submitted on the homepage
try:
    session = init_connection()
    #open the connection
except Exception as e:
        st.error("Connection Failed.  Please try again! The pages will not work unless a successfull connection is made" + '\n' + '\n' + "error: " + str(e))


user = get_user()

sf_database = session.get_current_database()
sf_schema = session.get_current_schema()

##add some markdown to the page with a desc
st.header("Access Approvals")
st.write('Please select the row you want to approve/decline:')

df_open_requests = get_open_requests(session, sf_database, sf_schema)

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

# st.header("Selected requests")
request = event.selection.rows
filtered_df = df_open_requests.iloc[request]
# st.dataframe(
#     filtered_df,
#     hide_index=True,
#     use_container_width=True,
#     column_order=df_col_list,
# )

options = ["Approve", "Decline"]
decision = st.selectbox('Decision', options)

submit = st.button('Submit')

if submit:
    update_decision(session, user, sf_database, sf_schema, filtered_df.iloc[0]["ID"], decision)
    if decision == 'Approve' and filtered_df.iloc[0]["REQUESTED_TIME_PERIOD_MINS"] == None:
        grant_access(session, sf_database, sf_schema, filtered_df.iloc[0]["ID"])
    st.rerun()
     

st.subheader('List of all requests')



df_requests = get_requests(session, sf_database,sf_schema)
event = st.dataframe(
        df_requests,
        use_container_width=True,
        hide_index=True,
        column_order=df_col_list
    )