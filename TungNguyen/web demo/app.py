"""
GDELT Hot Topics Forecaster - Streamlit Cloud Deployment
User: tungnguyen
Current Time: 2025-06-21 09:26:56 UTC
"""

import streamlit as st
import sys
import os
from pathlib import Path

# Add subdirectory to path if needed
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

# Import from subdirectory if needed
try:
    # Try to import from TungNguyen/web demo/ if that's where your main code is
    sys.path.append(str(current_dir / "TungNguyen" / "web demo"))
    from complete_gdelt_forecaster_app import main as gdelt_main
    HAS_MAIN_APP = True
except ImportError:
    HAS_MAIN_APP = False

# Page configuration
st.set_page_config(
    page_title="🔥 GDELT Hot Topics Forecaster",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded"
)

def deployment_info():
    """Show deployment information"""
    st.markdown(f"""
    <div style="background: linear-gradient(90deg, #4CAF50, #45A049); color: white; padding: 1rem; border-radius: 8px; text-align: center; margin-bottom: 2rem;">
        🚀 <strong>GDELT Hot Topics Forecaster</strong><br>
        👤 <strong>User:</strong> tungnguyen | 
        🕐 <strong>Deployed:</strong> 2025-06-21 09:26:56 UTC<br>
        ☁️ <strong>Platform:</strong> Streamlit Cloud | 
        📱 <strong>URL:</strong> timeseries123.streamlit.app
    </div>
    """, unsafe_allow_html=True)

def main():
    """Main application entry point"""
    
    # Show deployment info
    deployment_info()
    
    if HAS_MAIN_APP:
        # Run the main GDELT application
        try:
            gdelt_main()
        except Exception as e:
            st.error(f"❌ Error loading main app: {e}")
            show_fallback_app()
    else:
        st.warning("⚠️ Main application not found, showing fallback interface")
        show_fallback_app()

def show_fallback_app():
    """Fallback application if main app fails to load"""
    
    st.title("🔥 GDELT Hot Topics Forecaster")
    
    st.markdown("### 🔧 Deployment Status")
    
    # Check current directory structure
    current_dir = Path(".")
    
    st.markdown("#### 📁 Directory Structure:")
    
    def show_directory_tree(path, prefix="", max_depth=3, current_depth=0):
        if current_depth >= max_depth:
            return
        
        try:
            items = sorted(path.iterdir())
            for i, item in enumerate(items):
                is_last = i == len(items) - 1
                current_prefix = "└── " if is_last else "├── "
                
                if item.is_dir():
                    st.text(f"{prefix}{current_prefix}📁 {item.name}/")
                    if current_depth < max_depth - 1:
                        next_prefix = prefix + ("    " if is_last else "│   ")
                        show_directory_tree(item, next_prefix, max_depth, current_depth + 1)
                else:
                    st.text(f"{prefix}{current_prefix}📄 {item.name}")
        except PermissionError:
            st.text(f"{prefix}❌ Permission denied")
    
    show_directory_tree(current_dir)
    
    st.markdown("### 📦 Dependencies Status")
    
    # Check if required packages are available
    packages_to_check = [
        ("streamlit", "Streamlit framework"),
        ("pandas", "Data manipulation"),
        ("numpy", "Numerical computing"),
        ("plotly", "Interactive visualizations"),
        ("sklearn", "Machine learning"),
        ("xgboost", "Gradient boosting"),
        ("prophet", "Time series forecasting"),
    ]
    
    for package, description in packages_to_check:
        try:
            __import__(package)
            st.success(f"✅ {package}: {description}")
        except ImportError:
            st.error(f"❌ {package}: {description} - Not installed")
    
    st.markdown("### 🚀 Quick Demo")
    
    if st.button("🎭 Run Demo Mode"):
        st.balloons()
        st.success("🎉 Demo mode would run here!")
        
        # Simple demo visualization
        import numpy as np
        import pandas as pd
        
        # Generate sample data
        dates = pd.date_range('2024-04-01', '2024-06-10', freq='D')
        topic_data = {
            'Date': dates,
            'Topic_1_Security': np.random.uniform(0.1, 0.3, len(dates)),
            'Topic_2_Economy': np.random.uniform(0.08, 0.25, len(dates)),
            'Topic_3_Politics': np.random.uniform(0.12, 0.28, len(dates))
        }
        
        df = pd.DataFrame(topic_data)
        
        st.line_chart(df.set_index('Date'))
        st.success("✅ Sample hot topics trend visualization")

if __name__ == "__main__":
    main()