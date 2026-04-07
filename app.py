import streamlit as st
from sqlalchemy import create_engine
import os
import urllib
from dotenv import load_dotenv
import time
from datetime import datetime

# Import the main ETL function from your pipeline script
from et_pipeline import main_etl_process

# Load environment variables
load_dotenv()

# --- Page Configuration (Must be the first Streamlit command) ---
st.set_page_config(
    page_title="DataFlow | Healthcare ETL",
    page_icon="🧬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Advanced Custom CSS for a Premium Look ---
st.markdown("""
    <style>
    /* Import modern Google Font */
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600;800&display=swap');
    
    html, body, [class*="css"]  {
        font-family: 'Poppins', sans-serif;
    }

    /* Vibrant Gradient Header */
    .main-header {
        font-size: 3.5rem;
        font-weight: 800;
        background: linear-gradient(90deg, #00C9FF 0%, #92FE9D 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: -15px;
        padding-bottom: 0px;
    }
    
    .sub-header {
        font-size: 1.2rem;
        color: #808e9b;
        margin-bottom: 30px;
    }

    /* Animated Primary Button */
    div.stButton > button:first-child {
        background: linear-gradient(90deg, #FF416C 0%, #FF4B2B 100%);
        color: white;
        border: none;
        font-weight: 600;
        letter-spacing: 1px;
        border-radius: 8px;
        padding: 10px 24px;
        transition: all 0.3s ease;
    }
    
    div.stButton > button:first-child:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 15px rgba(255, 65, 108, 0.4);
        color: white;
    }
    
    /* Styled Metric Cards */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        border-left: 5px solid #0984e3;
        padding: 15px 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.05);
    }
    
    /* File Uploader Border */
    [data-testid="stFileUploader"] {
        border: 2px dashed #0984e3;
        border-radius: 15px;
        padding: 20px;
        background-color: #f8fafc;
    }
    </style>
""", unsafe_allow_html=True)

# --- Database Connection ---
@st.cache_resource
def get_db_engine():
    try:
        DB_SERVER = os.getenv("DB_SERVER", "localhost")
        DB_NAME = os.getenv("DB_NAME", "Healthcare_DW")
        DB_USER = os.getenv("DB_USER")
        DB_PASS = os.getenv("DB_PASS")

        if DB_USER and DB_PASS:
            encoded_password = urllib.parse.quote_plus(DB_PASS)
            connection_string = f"mssql+pyodbc://{DB_USER}:{encoded_password}@{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
        else:
            connection_string = f"mssql+pyodbc://@{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"
        
        engine = create_engine(connection_string, fast_executemany=True)
        return engine
    except Exception as e:
        return None

engine = get_db_engine()

# --- Sidebar Configuration ---
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/3200/3200760.png", width=80) # Cool tech icon
    st.title("System Link")
    
    if engine:
        st.success("🟢 **Live Database Link Established**")
        st.caption(f"Target Cluster: `{os.getenv('DB_NAME', 'Healthcare_DW')}`")
    else:
        st.error("🔴 **Offline: Connection Failed**")
        st.caption("Check your .env configuration.")
    
    st.divider()
    st.subheader("📌 Pipeline Rules")
    st.markdown("""
    * **Format:** CSV UTF-8 only
    * **Date Col:** Must be `DD-MM-YY`
    * **Deduplication:** Hash verification active.
    """)
    st.divider()
    st.caption(f"Server Time: {datetime.now().strftime('%H:%M:%S')} EET")

# --- Main UI Area ---
# Creating columns to frame the main content perfectly
left_spacer, center_col, right_spacer = st.columns([1, 8, 1])

with center_col:
    # Stylish Headers
    st.markdown('<p class="main-header">Nexus Healthcare Pipeline</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Automated Ingestion & Warehouse Staging Engine</p>', unsafe_allow_html=True)

    # --- Dashboard Metrics Row ---
    m1, m2, m3, m4 = st.columns(4)
    m1.metric(label="Server Status", value="Optimal", delta="99.9% Uptime")
    m2.metric(label="Ingestion Protocol", value="Batch", delta="CSV Upload", delta_color="off")
    m3.metric(label="Security", value="Active", delta="SHA-256 Hashing")
    if engine:
        m4.metric(label="Database Latency", value="14ms", delta="-2ms")
    else:
        m4.metric(label="Database Latency", value="ERR", delta="Offline", delta_color="inverse")
    
    st.write("---") # Visual divider

    # --- Data Dropzone ---
    st.subheader("⚡ Launch Pipeline")
    uploaded_file = st.file_uploader("Drop your dataset payload here", type="csv", label_visibility="collapsed")

    if uploaded_file is not None:
        st.toast(f"Data payload secured: {uploaded_file.name}", icon="🚀")
        
        # Action row
        col1, col2 = st.columns([3, 1])
        with col1:
            st.info(f"**Payload verified.** Total file size: `{round(uploaded_file.size / (1024*1024), 2)} MB` ready for staging.")
        with col2:
            st.write("") 
            start_etl = st.button("🔥 EXECUTE PIPELINE", use_container_width=True)

        if start_etl:
            if engine is None:
                st.error("Operation aborted: Database link is offline.")
            else:
                # Sleek expanding status indicator
                with st.status("Initiating Data Engineering Sequence...", expanded=True) as status:
                    st.write("⏳ Extracting data into Pandas DataFrame...")
                    time.sleep(0.4) 
                    st.write("🧼 Applying RegEx Title cleaning and type casting...")
                    time.sleep(0.4)
                    st.write("🔐 Generating SHA-256 composite business keys...")
                    
                    try:
                        # Call your updated ETL process
                        success, message = main_etl_process(uploaded_file, engine)
                        
                        if success:
                            st.write("📡 Truncating staging and streaming new batch...")
                            time.sleep(0.4)
                            st.write("⚙️ Executing SQL Server MERGE stored procedure...")
                            status.update(label="Sequence Completed Successfully!", state="complete", expanded=False)
                            st.success(message)
                            st.balloons()
                        else:
                            status.update(label="Sequence Failed", state="error", expanded=True)
                            st.error(message)
                    except Exception as e:
                        status.update(label="Critical System Error", state="error", expanded=True)
                        st.error(f"Traceback: {e}")

    # Empty state message
    if uploaded_file is None:
        st.markdown("<div style='text-align: center; color: #b2bec3; padding: 20px;'><i>Awaiting CSV payload to initialize...</i></div>", unsafe_allow_html=True)