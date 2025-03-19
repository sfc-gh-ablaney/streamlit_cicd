# Import python packages
import streamlit as st
import snowflake.snowpark
from snowflake.snowpark.context import get_active_session
import toml
from snowflake.snowpark.session import Session

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


def get_open_requests(_session, database, schema):
    try:
        table_meta_sql = f"""SELECT * FROM {database}.{schema}.ST_AR_ACCESS_REQUEST_LOG 
                            WHERE REQUEST_RESULT IS  NULL"""
        table_meta_df = _session.sql(table_meta_sql).to_pandas()
        return table_meta_df
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in get_access_roles(): " + str(e))


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
st.header("Access Approvals")
st.write('Please select the row you want to approve/decline:')
user = get_user()



df_open_requests = get_open_requests(session, sf_database, sf_schema)
df_cols = df_open_requests[["REQUESTED_ROLE_NAME" ,
"ROLE_REQUESTED_REASON" ,
"REQUESTED_TIME_PERIOD_MINS" ,
"REQUESTED_START_DT" ,
"REQUESTED_END_DT" ,
"REQUEST_REVIEWED_BY" ,
"REQUEST_RESULT" ,
"REQUEST_REVIEWED_TS" ,
"REQUEST_GRANT_TS"]]


event = st.dataframe(
        df_cols,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row",
    )

st.header("Selected requests")
request = event.selection.rows
filtered_df = df_cols.iloc[request]
st.dataframe(
    filtered_df,
    hide_index=True,
    use_container_width=True,
)

options = ["Approve", "Decline"]
selection = st.selectbox('Decision', options)

submit = st.button('Submit')

if submit:
     st.write('')