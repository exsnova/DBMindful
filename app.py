# app.py
import streamlit as st
from ui.dashboard import Dashboard
from config.settings import settings
from models.database import DatabaseConnection

def init_connection():
    """Initialize database connection with user input"""
    if 'database_url' not in st.session_state:
        st.session_state.database_url = None
        st.session_state.connection_tested = False

    # Connection input form
    with st.form("database_connection"):
        database_url = st.text_input(
            "Database Connection String",
            value=st.session_state.database_url or "",
            type="password",
            help="Format: postgresql://user:password@host:port/dbname"
        )
        submit = st.form_submit_button("Connect")
        
        if submit and database_url:
            settings.DATABASE_URL = database_url
            st.session_state.database_url = database_url
            
            # Test connection
            try:
                test_result = DatabaseConnection.test_connection()
                if "Successfully connected" in test_result:
                    st.success(test_result)
                    st.session_state.connection_tested = True
                else:
                    st.error(test_result)
                    st.session_state.connection_tested = False
            except Exception as e:
                st.error(f"Connection error: {str(e)}")
                st.session_state.connection_tested = False

def main():
    st.set_page_config(
        page_title="DB AI Monitor",
        page_icon="🔍",
        layout="wide"
    )
    
    # Check if we have a working connection
    if not st.session_state.get('connection_tested', False):
        init_connection()
    else:
        # Show current connection status and option to reconnect
        st.sidebar.success("✅ Database connected")
        if st.sidebar.button("Change Connection"):
            st.session_state.connection_tested = False
            st.rerun()
        
        # Initialize and render dashboard
        settings.DATABASE_URL = st.session_state.database_url
        dashboard = Dashboard()
        dashboard.render()

if __name__ == "__main__":
    main()