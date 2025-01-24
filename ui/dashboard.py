# ui/dashboard.py
import streamlit as st
import plotly.express as px
from datetime import datetime
from core.db_monitor import DBMonitor
from core.query_analyzer import QueryAnalyzer
from core.ai_service import AIService

class Dashboard:
    def __init__(self):
        self.db_monitor = DBMonitor()
        self.query_analyzer = QueryAnalyzer()
        self.ai_service = AIService()
        self.init_session_state()

    def init_session_state(self):
        """Initialize session state variables"""
        if 'last_refresh' not in st.session_state:
            st.session_state.last_refresh = datetime.now()

    def render_header(self):
        """Render the dashboard header"""
        col1, col2, col3, col4 = st.columns([2,1,1,1])
        with col1:
            st.title("🔍 Database AI Monitor")
        with col2:
            st.write("")
            st.write("Last refresh:", st.session_state.last_refresh.strftime("%H:%M:%S"))
        with col3:
            st.write("")
            if st.button("🔄 Refresh Data"):
                st.session_state.last_refresh = datetime.now()
                st.rerun()
        with col4:
            st.write("")
            if st.button("🗑️ Reset Stats"):
                self.db_monitor.reset_query_stats()
                st.success("Query statistics reset successfully!")
                st.rerun()


    def render_monitoring_status(self):
        """Render monitoring prerequisites status"""
        st.subheader("Monitoring Status")
        status = self.db_monitor.check_monitoring_prerequisites()
        
        if 'error' in status:
            st.error(f"Error checking monitoring status: {status['error']}")
            return
            
        cols = st.columns(3)
        with cols[0]:
            st.metric(
                "pg_stat_statements Extension",
                "Installed ✅" if status['extension_installed'] else "Not Installed ❌"
            )
        with cols[1]:
            st.metric("Track Setting", status['track_setting'])
        with cols[2]:
            st.metric("Tracked Queries", status['tracked_queries'])
            
        # Show configuration status
        st.write("### Configuration Details")
        st.code(f"""
Current Configuration:
- shared_preload_libraries: {status['shared_preload']}
- track_utility: {status['track_utility']}
- Number of tracked queries: {status['tracked_queries']}
        """)
            
        if not status['extension_installed'] or status['tracked_queries'] == 0:
            st.warning("""
                #### Required Configuration Steps:
                
                1. Add to postgresql.conf:
                ```
                shared_preload_libraries = 'pg_stat_statements'
                pg_stat_statements.track = 'all'
                pg_stat_statements.max = 10000
                ```
                
                2. Restart PostgreSQL
                
                3. Run in database:
                ```sql
                CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
                ```
                
                4. Reset statistics:
                ```sql
                SELECT pg_stat_statements_reset();
                ```
            """)

    def render_query_analysis(self):
        """Render the query analysis section"""
        st.header("Query Analysis")
        
        # Add monitoring status at the top
        self.render_monitoring_status()
        
        try:
            df = self.db_monitor.get_heavy_queries()
            
            # Metrics summary
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Queries Analyzed", len(df))
            with col2:
                if not df.empty:
                    st.metric("Slowest Query Time", f"{df['total_exec_time'].max():.2f}ms")
            with col3:
                if not df.empty:
                    st.metric("Avg Query Time", f"{df['avg_exec_time'].mean():.2f}ms")

            # Heavy queries table with AI analysis
            st.subheader("Heavy Queries")
            for idx, row in df.iterrows():
                with st.expander(f"Query {idx + 1}: {row['query_preview']}"):
                    st.code(row['query_preview'], language='sql')
                    
                    # Basic stats and analysis
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"Calls: {row['calls']}")
                        st.write(f"Total Time: {row['total_exec_time']:.2f}ms")
                        st.write(f"Avg Time: {row['avg_exec_time']:.2f}ms")
                    with col2:
                        analysis = self.query_analyzer.analyze_query(row['query_preview'])
                        st.write(f"Complexity: {analysis['complexity']}")
                        st.write("Suggestions:")
                        for suggestion in analysis['suggested_improvements']:
                            st.write(f"- {suggestion}")
                    
                    # Deep analysis button
                    if st.button("🔍 Analisi Approfondita", key=f"deep_analysis_{idx}"):
                        with st.spinner("Analisi approfondita in corso..."):
                            try:
                                ai_analysis = self.ai_service.deep_analyze_query(
                                    row['query_preview'], 
                                    row['total_exec_time']
                                )
                                
                                if 'error' in ai_analysis:
                                    st.error(f"Errore nell'analisi: {ai_analysis['error']}")
                                else:
                                    response = ai_analysis['choices'][0]['message']['content']
                                    st.markdown("### 🔍 Analisi Dettagliata")
                                    st.markdown(response)
                                    
                                    # Add a divider for better readability
                                    st.markdown("---")
                                    
                                    # Add disclaimer about AI-generated content
                                    st.info("Questa analisi è generata da un modello AI e dovrebbe essere " 
                                        "considerata come uno strumento di supporto alla decisione, "
                                        "non come una raccomandazione definitiva.")
                                    
                            except Exception as e:
                                st.error(f"Errore durante l'analisi approfondita: {str(e)}")

            # Query execution time chart
            if not df.empty:
                fig = px.bar(
                    df,
                    x='query_preview',
                    y='total_exec_time',
                    title='Query Execution Times',
                    labels={'query_preview': 'Query', 'total_exec_time': 'Total Execution Time (ms)'}
                )
                st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"Error fetching query data: {str(e)}")

    def render_table_analysis(self):
        """Render the table analysis section"""
        st.header("Table Analysis")
        try:
            df = self.db_monitor.get_table_stats()
            
            # Table statistics
            st.subheader("Table Statistics")
            st.dataframe(
                df,
                column_config={
                    "table_name": "Table Name",
                    "row_count": "Live Rows",
                    "dead_tuples": "Dead Tuples",
                    "total_size": "Total Size",
                    "table_size": "Table Size",
                    "index_size": "Index Size"
                },
                hide_index=True
            )

            # Table size distribution chart
            fig = px.pie(
                df,
                names='table_name',
                values='row_count',
                title='Table Size Distribution (by row count)'
            )
            st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"Error fetching table data: {str(e)}")

    def render_index_analysis(self):
        """Render the index analysis section"""
        st.header("Index Analysis")
        try:
            df = self.db_monitor.get_index_usage()
            
            # Index statistics
            st.subheader("Index Usage Statistics")
            st.dataframe(
                df,
                column_config={
                    "table_name": "Table",
                    "index_name": "Index",
                    "number_of_scans": "Scans",
                    "tuples_read": "Tuples Read",
                    "tuples_fetched": "Tuples Fetched",
                    "index_size": "Size"
                },
                hide_index=True
            )

            # Index usage chart
            fig = px.bar(
                df,
                x='index_name',
                y='number_of_scans',
                color='table_name',
                title='Index Usage Frequency',
                labels={'index_name': 'Index', 'number_of_scans': 'Number of Scans'}
            )
            st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"Error fetching index data: {str(e)}")

    def render(self):
        """Main render method"""
        self.render_header()
        
        # Create tabs
        tab1, tab2, tab3 = st.tabs(["Query Analysis", "Table Analysis", "Index Analysis"])
        
        with tab1:
            self.render_query_analysis()
        
        with tab2:
            self.render_table_analysis()
            
        with tab3:
            self.render_index_analysis()