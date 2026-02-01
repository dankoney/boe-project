import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import os

st.set_page_config(
    page_title="Demurrage Analysis Application",
    page_icon="📊",
    layout="wide"  # Use wide layout for enterprise feel
)

# 1. Load Authentication Configuration
# NOTE: In a real enterprise app, replace this YAML with a PostgreSQL query
# to a 'users' table.
# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, '.streamlit', 'config.yaml')

with open(config_path) as file:
    config = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    auto_hash=False,
)

# 2. Render Login Widget
# Call login using named arguments to match the installed `streamlit_authenticator` API.
# For `location='main'` the function does not return the (name, status, username)
# tuple — those values are stored in `st.session_state` instead.
authenticator.login(location='main', key='Login')
name = st.session_state.get('name')
authentication_status = st.session_state.get('authentication_status')
username = st.session_state.get('username')
# 3. Handle Login Status
if authentication_status:
    # SUCCESS: Show the navigation and main app
    st.sidebar.title(f"Welcome, {name}")
    st.sidebar.divider()
    # Use `location='unrendered'` when calling logout from a custom button
    # so the library performs the logout action immediately instead of
    # trying to render its own button inside the method.
    st.sidebar.button("Logout", on_click=lambda: authenticator.logout(location='unrendered'), key='logout_button')
    
    # Add organization logo and centered content
    col1, col2, col3 = st.columns([1.4, 1.2, 1])
    with col2:
        st.image("https://shippers.org.gh/wp-content/uploads/2021/09/gsaLogo400x400.png", 
                 width=150)
    
    st.markdown("<div style='text-align: center;'>", unsafe_allow_html=True)
    st.markdown("<h1 style='text-align: center;'>Demurrage Analytics Dashboard</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; font-style: italic; font-size: 16px;'>Track and analyze container demurrage charges and port operations.</p>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)
    
elif authentication_status is False:
    # FAILURE
    st.error('Username/password is incorrect')

elif authentication_status is None:
    # NO INPUT YET
    st.warning('Please enter your username and password')

# To make this work, create a file named: /frontend/.streamlit/config.yaml
# Use a strong secret key and real passwords/hashed passwords.
# --- config.yaml content ---
# cookie:
#   expiry_days: 30
#   key: "YOUR_VERY_SECRET_KEY"
#   name: "boe_stats_app_cookie"
# credentials:
#   usernames:
#     jdoe:
#       email: jdoe@example.com
#       name: Jane Doe
#       # Password: 'password' (Use a real hash generator like stauth.Hasher)
#       password: "$2b$12$R.Sj3OaH32g2WzGvE9y1O.gXbF.Fq4Zt3yV8t5fQ7rY8zI9qP."