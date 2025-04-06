import streamlit as st
import time

@st.cache_resource
def get_user():
    try:
        user = st.experimental_user.user_name
    except:
        try:
            user = st.user.user_name
        except:
            user = 'ABLANEY'
    return user

@st.cache_data
def get_access_roles(_session, database, schema):
    try:
        table_meta_sql = f"""SELECT * FROM {database}.{schema}.ST_AR_ROLES"""
        table_meta_df = _session.sql(table_meta_sql).to_pandas()
        return table_meta_df
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in get_access_roles(): " + str(e))


def get_requests(_session, database, schema):
    try:
        table_requests_sql = f"""SELECT * FROM {database}.{schema}.ST_AR_ACCESS_REQUEST_LOG 
                            ORDER BY CREATED_TS DESC"""
        table_requests_df = _session.sql(table_requests_sql).to_pandas()
        return table_requests_df
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in get_access_roles(): " + str(e))

def get_user_grants(_session, user):
    try:
        # show_grants_sql = f"""show grants to user {user}"""
        # user_grants_df =  _session.sql(show_grants_sql).collect()
        # return user_grants_df


        # create task
        create_task_sql = f"""CREATE OR REPLACE TASK TSK_SHOW_GRANTS_{user} 
                            WAREHOUSE = SNOW_WH 
                            AS CALL SHOW_GRANTS_T('{user}'); """
        _session.sql(create_task_sql).collect()
        # execute task
        execute_task_sql = f"""EXECUTE TASK TSK_SHOW_GRANTS_{user}; """
        _session.sql(execute_task_sql).collect()
        # wait
        time.sleep(5)  # Wait 5 seconds
        # select from table
        table_meta_sql = f"""SELECT * FROM DB_SHOW_GRANTS"""
        table_meta_df = _session.sql(table_meta_sql).to_pandas()
        # drop task
        # drop proc
        
        
        
        return table_meta_df

    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in get_user_grants(): " + str(e))