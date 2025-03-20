import streamlit as st

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
