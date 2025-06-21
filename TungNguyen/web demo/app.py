"""
GDELT Hot Topics Forecaster - Fixed Upload Limits
User: strawberrymilktea0604
Current Time: 2025-06-21 08:51:55 UTC
"""

import streamlit as st
import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime

# Current user and time
CURRENT_USER = "strawberrymilktea0604"
CURRENT_TIME = "2025-06-21 08:51:55"

# Page configuration (MUST be first)
st.set_page_config(
    page_title="🔥 GDELT Hot Topics Forecaster - Enhanced Upload",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ❌ REMOVED: st.set_option() calls - these cause the error
# ✅ Use config.toml instead

# Custom CSS for enhanced styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #FF4B4B;
        text-align: center;
        margin-bottom: 2rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
    }
    .user-info {
        background: linear-gradient(90deg, #4CAF50, #45A049);
        color: white;
        padding: 1rem;
        border-radius: 8px;
        text-align: center;
        margin-bottom: 2rem;
    }
    .upload-info {
        background: linear-gradient(135deg, #E3F2FD, #BBDEFB);
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #2196F3;
        margin: 1rem 0;
    }
    .success-box {
        background: linear-gradient(135deg, #E8F5E8, #C8E6C9);
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #4CAF50;
        margin: 1rem 0;
    }
    .warning-box {
        background: linear-gradient(135deg, #FFF3E0, #FFE0B2);
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #FF9800;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

def check_upload_config():
    """Check if upload config is properly set"""
    try:
        # Try to read the config file
        config_path = ".streamlit/config.toml"
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config_content = f.read()
                if 'maxUploadSize' in config_content:
                    return True, "✅ Config file found with upload limits"
                else:
                    return False, "⚠️ Config file exists but missing maxUploadSize"
        else:
            return False, "❌ No config file found"
    except Exception as e:
        return False, f"❌ Error reading config: {e}"

def create_config_instructions():
    """Create step-by-step config instructions"""
    st.markdown("### 🔧 Setup Instructions for Large File Upload")
    
    config_exists, config_status = check_upload_config()
    
    if config_exists:
        st.markdown(f"""
        <div class="success-box">
            <h4>✅ Configuration Status</h4>
            <p>{config_status}</p>
            <p><strong>Upload Limit:</strong> 5GB per file</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="warning-box">
            <h4>⚠️ Configuration Needed</h4>
            <p>{config_status}</p>
            <p>Please follow the setup instructions below:</p>
        </div>
        """, unsafe_allow_html=True)
        
        tab1, tab2, tab3 = st.tabs(["🪟 Windows", "🐧 Linux/Mac", "⚙️ Manual"])
        
        with tab1:
            st.markdown("""
            #### Windows Setup:
            
            **1. Create config directory and file:**
            ```cmd
            mkdir .streamlit
            echo [server] > .streamlit\\config.toml
            echo maxUploadSize = 5000 >> .streamlit\\config.toml
            echo maxMessageSize = 5000 >> .streamlit\\config.toml
            ```
            
            **2. Restart Streamlit:**
            ```cmd
            streamlit run app.py
            ```
            """)
        
        with tab2:
            st.markdown("""
            #### Linux/Mac Setup:
            
            **1. Create config directory and file:**
            ```bash
            mkdir -p .streamlit
            cat > .streamlit/config.toml << EOF
            [server]
            maxUploadSize = 5000
            maxMessageSize = 5000
            EOF
            ```
            
            **2. Restart Streamlit:**
            ```bash
            streamlit run app.py
            ```
            """)
        
        with tab3:
            st.markdown("""
            #### Manual Creation:
            
            **1. Create folder:** `.streamlit` in your project directory
            
            **2. Create file:** `.streamlit/config.toml`
            
            **3. Add content:**
            ```toml
            [server]
            maxUploadSize = 5000
            maxMessageSize = 5000
            enableCORS = false
            
            [theme]
            primaryColor = "#FF4B4B"
            backgroundColor = "#FFFFFF"
            secondaryBackgroundColor = "#F0F2F6"
            textColor = "#262730"
            ```
            
            **4. Restart the app**
            """)
        
        if st.button("🔄 Check Configuration Again"):
            st.rerun()

def enhanced_file_upload():
    """Enhanced file upload interface"""
    st.markdown("### 📁 Enhanced File Upload")
    
    config_exists, _ = check_upload_config()
    
    if config_exists:
        st.markdown("""
        <div class="upload-info">
            <h4>📤 Upload Your GDELT Data</h4>
            <p><strong>✅ Large file support enabled (up to 5GB)</strong></p>
            <p>🗜️ Supported: ZIP files containing CSV data</p>
            <p>📊 Expected: GDELT format with DATE and THEMES columns</p>
        </div>
        """, unsafe_allow_html=True)
        
        uploaded_file = st.file_uploader(
            "Upload GDELT ZIP file (up to 5GB)",
            type=['zip'],
            help="Upload ZIP files containing GDELT CSV data organized by month",
            key="enhanced_upload"
        )
        
        if uploaded_file is not None:
            file_size_mb = len(uploaded_file.getvalue()) / (1024 * 1024)
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("📁 File Name", uploaded_file.name)
            with col2:
                st.metric("📏 File Size", f"{file_size_mb:.1f} MB")
            with col3:
                if file_size_mb < 5000:
                    st.metric("📊 Status", "✅ Ready")
                else:
                    st.metric("📊 Status", "⚠️ Too Large")
            
            if file_size_mb > 1000:  # > 1GB
                st.info(f"💡 Large file detected ({file_size_mb:.1f} MB). Processing may take several minutes.")
            
            if file_size_mb < 5000:
                st.success("✅ File uploaded successfully! Ready for processing.")
                return uploaded_file
            else:
                st.error(f"❌ File too large ({file_size_mb:.1f} MB). Maximum size is 5GB.")
                return None
        
        return None
    
    else:
        st.error("❌ Upload configuration not set. Please follow setup instructions above.")
        return None

def main():
    """Main application"""
    # Header
    st.markdown('<h1 class="main-header">🔥 GDELT Hot Topics Forecaster</h1>', unsafe_allow_html=True)
    
    # User info
    st.markdown(f"""
    <div class="user-info">
        👤 <strong>User:</strong> {CURRENT_USER} | 
        🕐 <strong>Current Time:</strong> {CURRENT_TIME} UTC | 
        📁 <strong>Enhanced Upload:</strong> Up to 5GB per file
    </div>
    """, unsafe_allow_html=True)
    
    # Check and display configuration status
    create_config_instructions()
    
    st.markdown("---")
    
    # Enhanced file upload
    uploaded_file = enhanced_file_upload()
    
    if uploaded_file:
        st.markdown("---")
        st.markdown("### 🚀 Next Steps")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔍 Analyze ZIP Structure", type="primary"):
                st.info("🔄 ZIP analysis feature will be implemented next...")
        
        with col2:
            if st.button("🎭 Use Demo Data Instead", type="secondary"):
                st.info("🔄 Demo data feature will be implemented next...")
    
    # Sidebar status
    with st.sidebar:
        st.markdown("## 📊 System Status")
        
        config_exists, config_status = check_upload_config()
        
        if config_exists:
            st.success("✅ Upload Config OK")
            st.info("📁 Max Size: 5GB")
        else:
            st.error("❌ Config Needed")
            st.warning("⚠️ Limited to 200MB")
        
        st.markdown("---")
        
        st.markdown("### 💡 Quick Tips")
        st.info("🔧 Create .streamlit/config.toml file to enable large uploads")
        st.info("🗜️ Use ZIP files for better compression")
        st.info("📊 Organize data by month for optimal processing")
        
        st.markdown("### 📞 Support")
        st.markdown(f"""
        **User:** {CURRENT_USER}  
        **Time:** {CURRENT_TIME}  
        **Version:** Enhanced Upload v1.0
        """)

if __name__ == "__main__":
    main()