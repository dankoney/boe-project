import streamlit as st
import datetime
import pandas as pd
import requests
from urllib.parse import urlencode
from typing import Dict, Any, List, Optional
import json

try:
    from itables.streamlit import interactive_table
    ITABLES_AVAILABLE = True
except ImportError:
    ITABLES_AVAILABLE = False
    st.warning("itables not installed. Install with: pip install itables")

# --- GLOBAL CSS FOR A CLEANER LOOK, ALIGNMENT FIX & MODERN CARDS ---
st.markdown("""
<style>
/* Streamlit's default padding is often too large, let's reset it for cleaner forms */
div[data-testid="stForm"] > div {
    gap: 0.5rem; /* Reduce vertical gap inside forms/containers */
}

/* 1. Alignment Fix for Search Input and Add Button using Flexbox */
.stCustomAlignedInput > div {
    display: flex;
    align-items: flex-end; /* Align input and button to the bottom line */
    gap: 10px; /* Space between input and button */
}

/* 2. Custom Input Height Adjustment */
.stCustomAlignedInput div[data-testid="stTextInput"] {
    flex-grow: 1; /* Input takes up maximum available width */
    margin-bottom: 0 !important; /* Eliminate bottom margin */
}

/* 3. Style Expander/Container borders for the card aesthetic */
div[data-testid="stVerticalBlock"] > div:has(div[data-testid="stExpander"]) {
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    padding: 10px;
    margin-bottom: 15px;
}

/* 4. Ensure the main search button is prominent */
.stButton button[key="search_btn_bottom"] {
    font-size: 1.1rem;
    height: 3rem;
}

/* --- NEW: MODERN METRIC CARDS --- */
div[data-testid="stMetric"] {
    background-color: #f7f9fb;
    border: 1px solid #e0e0e0;
    padding: 15px;
    border-radius: 10px;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05);
    transition: transform 0.2s;
}

/* Customizing Streamlit's metric labels and values */
div[data-testid="stMetric"] label p {
    font-size: 1.0rem;
    font-weight: 500;
    color: #555;
}

div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
    font-size: 1.8rem;
    font-weight: 700;
    color: #1f77b4; /* Primary color */
}

/* 5. ITables Table Spacing */
div[data-testid="stHtml"] {
    margin-top: 1rem;
    margin-bottom: 1.5rem;
}

/* 6. Container spacing for better layout */
div[data-testid="stContainer"] {
    margin-bottom: 1rem;
}

/* 7. Expander spacing */
div[data-testid="stExpander"] {
    margin-top: 1rem;
    margin-bottom: 1rem;
}

/* 8. ITables DataTable styling */
.dataTables_wrapper {
    margin-top: 1rem;
    margin-bottom: 1.5rem;
}
</style>
""", unsafe_allow_html=True)


# --- Configuration and Constants ---
FASTAPI_ENDPOINT = "http://127.0.0.1:8000/reports/cargo/"
HSCODE_SUGGEST_ENDPOINT = "http://127.0.0.1:8000/hscodes/suggestions"
VESSEL_SUGGEST_ENDPOINT = "http://127.0.0.1:8000/suggestions/vessel"
IMPORTER_SUGGEST_ENDPOINT = "http://127.0.0.1:8000/suggestions/importer"
COUNTRIES = ["CHINA", "USA", "GERMANY", "GHANA", "INDIA", "BRAZIL", "FRANCE", "SINGAPORE"]

# --- Country Code Mapping (Simplified) ---
COUNTRY_CODE_MAP = { 'AF': ('', 'Afghanistan'), 'AL': ('', 'Albania'), 'DZ': ('', 'Algeria'), 'AD': ('', 'Andorra'), 'AO': ('', 'Angola'), 'AR': ('', 'Argentina'), 'AM': ('', 'Armenia'), 'AU': ('', 'Australia'), 'AT': ('', 'Austria'), 'AZ': ('', 'Azerbaijan'), 'BH': ('', 'Bahrain'), 'BD': ('', 'Bangladesh'), 'BB': ('', 'Barbados'), 'BE': ('', 'Belgium'), 'BJ': ('', 'Benin'), 'BT': ('', 'Bhutan'), 'BO': ('', 'Bolivia'), 'BA': ('', 'Bosnia and Herzegovina'), 'BW': ('', 'Botswana'), 'BR': ('', 'Brazil'), 'BG': ('', 'Bulgaria'), 'BF': ('', 'Burkina Faso'), 'BI': ('', 'Burundi'), 'KH': ('', 'Cambodia'), 'CM': ('', 'Cameroon'), 'CA': ('', 'Canada'), 'CV': ('', 'Cape Verde'), 'CF': ('', 'Central African Republic'), 'TD': ('', 'Chad'), 'CL': ('', 'Chile'), 'CN': ('', 'China'), 'CO': ('', 'Colombia'), 'KM': ('', 'Comoros'), 'CD': ('', 'Congo (DRC)'), 'CR': ('', 'Costa Rica'), 'HR': ('', 'Croatia'), 'CU': ('', 'Cuba'), 'CY': ('', 'Cyprus'), 'CZ': ('', 'Czechia'), 'DK': ('', 'Denmark'), 'DJ': ('', 'Djibouti'), 'DO': ('', 'Dominican Republic'), 'EC': ('', 'Ecuador'), 'EG': ('', 'Egypt'), 'SV': ('', 'El Salvador'), 'GQ': ('', 'Equatorial Guinea'), 'ER': ('', 'Eritrea'), 'EE': ('', 'Estonia'), 'ET': ('', 'Ethiopia'), 'FJ': ('', 'Fiji'), 'FI': ('', 'Finland'), 'FR': ('', 'France'), 'GA': ('', 'Gabon'), 'GM': ('', 'Gambia'), 'GE': ('', 'Georgia'), 'DE': ('', 'Germany'), 'GH': ('', 'Ghana'), 'GR': ('', 'Greece'), 'GT': ('', 'Guatemala'), 'GN': ('', 'Guinea'), 'HT': ('', 'Haiti'), 'HN': ('', 'Honduras'), 'HK': ('', 'Hong Kong'), 'HU': ('', 'Hungary'), 'IS': ('', 'Iceland'), 'IN': ('', 'India'), 'ID': ('', 'Indonesia'), 'IR': ('', 'Iran'), 'IQ': ('', 'Iraq'), 'IE': ('', 'Ireland'), 'IL': ('', 'Israel'), 'IT': ('', 'Italy'), 'JM': ('', 'Jamaica'), 'JP': ('', 'Japan'), 'JO': ('', 'Jordan'), 'KZ': ('', 'Kazakhstan'), 'KE': ('', 'Kenya'), 'KP': ('', 'North Korea'), 'KR': ('', 'South Korea'), 'KW': ('', 'Kuwait'), 'KG': ('', 'Kyrgyzstan'), 'LA': ('', 'Laos'), 'LV': ('', 'Latvia'), 'LB': ('', 'Lebanon'), 'LR': ('', 'Liberia'), 'LY': ('', 'Libya'), 'LT': ('', 'Lithuania'), 'LU': ('', 'Luxembourg'), 'MK': ('', 'North Macedonia'), 'MG': ('', 'Madagascar'), 'MW': ('', 'Malawi'), 'MY': ('', 'Malaysia'), 'MV': ('', 'Maldives'), 'ML': ('', 'Mali'), 'MT': ('', 'Malta'), 'MR': ('', 'Mauritania'), 'MX': ('', 'Mexico'), 'MD': ('', 'Moldova'), 'MC': ('', 'Monaco'), 'MN': ('', 'Mongolia'), 'MA': ('', 'Morocco'), 'MZ': ('', 'Mozambique'), 'MM': ('', 'Myanmar'), 'NA': ('', 'Namibia'), 'NP': ('', 'Nepal'), 'NL': ('', 'Netherlands'), 'NZ': ('', 'New Zealand'), 'NI': ('', 'Nicaragua'), 'NE': ('', 'Niger'), 'NG': ('', 'Nigeria'), 'NO': ('', 'Norway'), 'OM': ('', 'Oman'), 'PK': ('', 'Pakistan'), 'PA': ('', 'Panama'), 'PG': ('', 'Papua New Guinea'), 'PY': ('', 'Paraguay'), 'PE': ('', 'Peru'), 'PH': ('', 'Philippines'), 'PL': ('', 'Poland'), 'PT': ('', 'Portugal'), 'QA': ('', 'Qatar'), 'RO': ('', 'Romania'), 'RU': ('', 'Russia'), 'RW': ('', 'Rwanda'), 'SA': ('', 'Saudi Arabia'), 'SN': ('', 'Senegal'), 'RS': ('', 'Serbia'), 'SL': ('', 'Sierra Leone'), 'SG': ('', 'Singapore'), 'SK': ('', 'Slovakia'), 'SI': ('', 'Slovenia'), 'SO': ('', 'Somalia'), 'ZA': ('', 'South Africa'), 'ES': ('', 'Spain'), 'LK': ('', 'Sri Lanka'), 'SD': ('', 'Sudan'), 'SE': ('', 'Sweden'), 'CH': ('', 'Switzerland'), 'SY': ('', 'Syria'), 'TW': ('', 'Taiwan'), 'TJ': ('', 'Tajikistan'), 'TZ': ('', 'Tanzania'), 'TH': ('', 'Thailand'), 'TG': ('', 'Togo'), 'TN': ('', 'Tunisia'), 'TR': ('', 'Turkey'), 'TM': ('', 'Turkmenistan'), 'UG': ('', 'Uganda'), 'UA': ('', 'Ukraine'), 'AE': ('', 'United Arab Emirates'), 'GB': ('', 'United Kingdom'), 'US': ('', 'United States'), 'UY': ('', 'Uruguay'), 'UZ': ('', 'Uzbekistan'), 'VE': ('', 'Venezuela'), 'VN': ('', 'Vietnam'), 'YE': ('', 'Yemen'), 'ZM': ('', 'Zambia'), 'ZW': ('', 'Zimbabwe'), 'N/A': ('', 'Unknown Nationality') }

# Define Initial State values for easy reference and resetting
INITIAL_DATES = {
    'start_date_input': datetime.date(2025, 2, 1),
    'end_date_input': datetime.date(2025, 4, 28)
}

# --- Initialization of Session State ---
if 'report_results' not in st.session_state:
    st.session_state['report_results'] = None
if 'close_filter_expander' not in st.session_state:
    st.session_state['close_filter_expander'] = False

# Search Filter Keys
if 'hscode_search_term' not in st.session_state:
    st.session_state['hscode_search_term'] = ''
if 'hscode_suggestions' not in st.session_state:
    st.session_state['hscode_suggestions'] = []
if 'selected_hscodes' not in st.session_state:
    st.session_state['selected_hscodes'] = []

if 'vessel_search_input' not in st.session_state:
    st.session_state['vessel_search_input'] = ''
if 'vessel_suggestions' not in st.session_state:
    st.session_state['vessel_suggestions'] = []
if 'selected_vessel_names' not in st.session_state:
    st.session_state['selected_vessel_names'] = []

if 'importer_search_input' not in st.session_state:
    st.session_state['importer_search_input'] = ''
if 'importer_suggestions' not in st.session_state:
    st.session_state['importer_suggestions'] = []
if 'selected_importer_names' not in st.session_state:
    st.session_state['selected_importer_names'] = []

# --- FIX: Ensure date keys are initialized in Session State (no widget collision) ---
if 'start_date_input' not in st.session_state:
    st.session_state['start_date_input'] = INITIAL_DATES['start_date_input']
if 'end_date_input' not in st.session_state:
    st.session_state['end_date_input'] = INITIAL_DATES['end_date_input']

# --- Other static inputs initialized to empty strings ---
for key in ['boe_number', 'importer_tin', 'bl_number', 'country_of_origin', 'goods_description_keywords']:
    if key not in st.session_state:
        st.session_state[key] = ''


# --- Filter Key Mapping and Helper Functions ---
API_FILTER_MAP = {
    'country_of_origin': 'country_of_origin',
    'selected_hscodes': 'hscode',
    'boe_number': 'boe_number',
    'importer_tin': 'importer_tin',
    'bl_number': 'bl_number',
    'selected_vessel_names': 'vessel_name',
    'selected_importer_names': 'importer_name_keywords',
    'goods_description_keywords': 'goods_description_keywords',
}

def get_active_filters_display() -> str:
    """Collects and formats all currently active filters for display."""
    filters = []
    
    # 1. Document Numbers & Static Inputs
    for st_key, display_name in [('boe_number', 'BOE No'), ('importer_tin', 'TIN'), ('bl_number', 'BL No'), ('country_of_origin', 'Country of Origin')]:
        value = st.session_state.get(st_key)
        if value and value.strip() != '':
            filters.append(f"**{display_name}:** `{value.strip()}`")

    # 2. Multi-select/List Inputs
    if st.session_state.get('selected_hscodes'):
        filters.append(f"**HS Codes:** `{', '.join(st.session_state['selected_hscodes'])}`")
    if st.session_state.get('selected_vessel_names'):
        filters.append(f"**Vessels:** `{', '.join(st.session_state['selected_vessel_names'])}`")
    if st.session_state.get('selected_importer_names'):
        filters.append(f"**Importers:** `{', '.join(st.session_state['selected_importer_names'])}`")

    # 3. Goods Description Keywords
    keywords = st.session_state.get('goods_description_keywords', '').strip()
    if keywords:
        filters.append(f"**Goods Keywords:** `{keywords}`")
        
    if not filters:
        return "No specific criteria applied (Date Range Only)."
    
    return "Filters Applied: " + " | ".join(filters)

def clear_all_filters():
    """Clears all filter values and resets to initial state."""
    # Clear multi-select lists (safe to set directly)
    st.session_state['selected_hscodes'] = []
    st.session_state['selected_vessel_names'] = []
    st.session_state['selected_importer_names'] = []
    
    # Clear suggestions (safe to set)
    st.session_state['hscode_suggestions'] = []
    st.session_state['vessel_suggestions'] = []
    st.session_state['importer_suggestions'] = []
    
    # Set a flag to clear widget values on next render (including dates)
    st.session_state['clear_widgets_flag'] = True
    st.session_state['reset_dates_flag'] = True
    
    # Reopen expander
    st.session_state['close_filter_expander'] = False


def fetch_hscode_suggestions(search_term: str):
    if len(search_term) >= 4:
        try:
            response = requests.get(f"{HSCODE_SUGGEST_ENDPOINT}?prefix={search_term}")
            response.raise_for_status()
            st.session_state['hscode_suggestions'] = response.json()
        except requests.exceptions.RequestException:
            st.session_state['hscode_suggestions'] = []
    else:
        st.session_state['hscode_suggestions'] = []

def fetch_keyword_suggestions(search_term: str, endpoint: str, state_key: str):
    if len(search_term) >= 3:
        try:
            response = requests.get(f"{endpoint}?keyword={search_term}")
            response.raise_for_status()
            st.session_state[state_key] = response.json()
        except requests.exceptions.RequestException:
            st.session_state[state_key] = []
    else:
        st.session_state[state_key] = []

def _on_keyword_submit(endpoint_url: str, suggestions_state_key: str, input_key: str):
    cur = st.session_state.get(input_key, '').strip()
    if cur:
        fetch_keyword_suggestions(cur, endpoint_url, suggestions_state_key)
    else:
        st.session_state[suggestions_state_key] = []

def collect_filter_params(start_date: datetime.date, end_date: datetime.date) -> Dict[str, Any]:
    filters: Dict[str, Any] = {
        "start_date": str(start_date),
        "end_date": str(end_date),
    }

    for st_key, api_key in API_FILTER_MAP.items():
        value = st.session_state.get(st_key)

        if not value:
            continue

        if api_key == 'country_of_origin' and value is not None and value != '':
            filters[api_key] = str(value).upper()

        elif api_key in ['hscode', 'vessel_name', 'importer_name_keywords'] and isinstance(value, list) and len(value) > 0:
            filters[api_key] = value

        elif api_key == 'goods_description_keywords' and isinstance(value, str) and value.strip() != '':
            keywords = [k.strip() for k in value.split(',') if k.strip()]
            if keywords:
                filters[api_key] = keywords

        elif isinstance(value, str) and value != '':
            filters[api_key] = value

    return filters

def run_search(params: Dict[str, Any]):
    query_string = urlencode(params, doseq=True)
    full_url = f"{FASTAPI_ENDPOINT}?{query_string}"

    st.info(f"API Request URL: {full_url}")

    with st.spinner("🔍 Searching database..."):
        try:
            response = requests.get(full_url)
            response.raise_for_status()
            results = response.json()
            st.session_state['report_results'] = results
            st.session_state['close_filter_expander'] = True
            return True

        except requests.exceptions.ConnectionError:
            st.error("❌ Connection Error: Could not connect to the FastAPI backend. Make sure the API is running.")
        except requests.exceptions.HTTPError as e:
            # Safely extract error detail
            error_detail = response.json().get('detail', 'An unknown error occurred.') if response.content else 'No detail available.'
            st.error(f"❌ API HTTP Error ({response.status_code}): {error_detail}")
        except Exception as e:
            st.error(f"❌ General Error: {e}")

        st.session_state['report_results'] = None
        return False

def _on_hscode_submit():
    cur = st.session_state.get('hscode_search_input', '').strip()
    if len(cur) >= 4:
        fetch_hscode_suggestions(cur)
    else:
        st.session_state['hscode_suggestions'] = []


# -----------------------------------------------------
# --- Utility UI Functions for Suggestions/Selection (Unchanged) ---
# -----------------------------------------------------

def render_suggestion_section(title, input_key, suggestions_state_key, selected_state_key, endpoint_url, min_length):
    """
    Renders the UI for a multi-select field with search, add button, and suggestions.
    """

    clear_flag_key = f'clear_{input_key}'

    if st.session_state.get(clear_flag_key):
        # Clears input box after successful add
        st.session_state[input_key] = ''
        del st.session_state[clear_flag_key]

    st.markdown(f"**Search {title}**", help=f"Type {min_length}+ characters and press Enter")

    # Use a temporary container and the custom class to encapsulate the input/button pair
    st.markdown('<div class="stCustomAlignedInput">', unsafe_allow_html=True)

    col_input, col_add = st.columns([3, 1])

    with col_input:
        st.text_input(
            "__HIDDEN_INPUT__",
            key=input_key,
            label_visibility='collapsed',
            placeholder=f"Enter {title} or search keyword",
            on_change=lambda: _on_keyword_submit(endpoint_url, suggestions_state_key, input_key) if endpoint_url else _on_hscode_submit()
        )

    with col_add:
        current_input = st.session_state.get(input_key, '').strip()

        if st.button(f"+ Add", key=f"add_{input_key}", disabled=(len(current_input) < min_length), width='stretch'):
            if current_input and current_input not in st.session_state[selected_state_key]:
                st.session_state[selected_state_key].append(current_input)
                st.session_state[clear_flag_key] = True
                st.rerun()

    st.markdown('</div>', unsafe_allow_html=True) # Close the custom class wrapper


    # --- Suggestions ---
    suggestions = st.session_state.get(suggestions_state_key, [])
    if suggestions:
        st.markdown("**i\uFE0F Suggestions:**") # Using standard 'i' and U+FE0F for a standard info icon

        with st.container(height=150, border=True):
            col_sugg = st.columns(3)

            for idx, item in enumerate(suggestions):

                main_str = item.get('hscode') or item.get('name')
                display_text = ""
                tooltip_text = ""

                if 'hscode' in item:
                    # HS Code
                    secondary_info = item.get('description', 'No Desc.')
                    display_text = f"**{main_str}** - {secondary_info[:30]}..."
                    tooltip_text = f"HS Code: {main_str} - Description: {secondary_info}"
                elif 'vesselNationality' in item:
                    # Vessel
                    code = item.get('vesselNationality', '').upper()
                    _, full_name = COUNTRY_CODE_MAP.get(code, COUNTRY_CODE_MAP['N/A'])
                    display_text = f"**{main_str}** ({code})"
                    tooltip_text = f"Vessel: {main_str} - Nationality: {full_name} ({code})"
                elif 'importerTin' in item:
                    # Importer
                    tin = item.get('importerTin', 'N/A')
                    display_text = f"**{main_str}** ({tin})"
                    tooltip_text = f"Importer: {main_str} - TIN: {tin}"

                is_selected = main_str in st.session_state[selected_state_key]
                key_name = f"{selected_state_key}_suggest_{main_str}_{idx}"

                with col_sugg[idx % 3]:

                    checked = st.checkbox(
                        display_text,
                        value=is_selected,
                        key=key_name,
                        help=tooltip_text
                    )

                    # --- Selection Logic ---
                    if checked and not is_selected:
                        st.session_state[selected_state_key].append(main_str)
                        st.rerun()
                    if not checked and is_selected:
                        try:
                            st.session_state[selected_state_key].remove(main_str)
                        except ValueError:
                            pass
                        st.rerun()

    # --- Final List Widget ---
    if st.session_state.get(selected_state_key):
        st.markdown("**Selected Terms**")
        final_list = st.multiselect(
            f"Selected {title} Terms",
            options=st.session_state[selected_state_key],
            default=st.session_state[selected_state_key],
            key=f'{selected_state_key}_final_list_widget'
        )
        st.session_state[selected_state_key] = final_list


# --- Main Page Function (Updated) ---
def boe_cargo_report_page():

    # 1. Header (Design Maintained)
    st.markdown(f"""
    <div style='text-align: center; margin-bottom: 0.5rem; padding-bottom: 10px; border-bottom: 2px solid #1f77b4;'>
        <h1 style='color: #1f77b4; font-size: 2.8rem; margin-bottom: 0.2rem;'>Ship Cargo Report Query</h1>
        <p style='color: #666; font-size: 1.1rem;'>Filter Bill of Entry (BOE) data by key shipping and goods criteria.</p>
    </div>
    """, unsafe_allow_html=True)

    # --- FILTER SECTION ---
    
    # Handle clear widgets flag - clear widget values when widgets are rendered
    if st.session_state.get('clear_widgets_flag', False):
        # Clear widget keys when they're about to be rendered
        for key in ['boe_number', 'importer_tin', 'bl_number', 'country_of_origin', 'goods_description_keywords',
                   'hscode_search_input', 'vessel_search_input', 'importer_search_input']:
            if key in st.session_state:
                del st.session_state[key]
        st.session_state['clear_widgets_flag'] = False
    
    # Check if we need to reset dates (before clearing the flag)
    reset_dates = st.session_state.get('reset_dates_flag', False)
    
    # Handle reset dates flag - delete date widget keys so they can be reinitialized
    if reset_dates:
        for key in ['start_date_input', 'end_date_input']:
            if key in st.session_state:
                del st.session_state[key]
        st.session_state['reset_dates_flag'] = False
    
    # Use expander instead of conditional rendering to keep widgets in DOM
    # Expander is open by default, closes after search
    is_expanded = not st.session_state.get('close_filter_expander', False)
    
    with st.expander("🔍 **Define Search Criteria**", expanded=is_expanded):
        # 2. Date Range Card (Required)
        st.markdown("### Date Range (Required)")
        with st.container(border=True):
            col_date_from, col_date_to = st.columns(2)

            with col_date_from:
                # If resetting dates, use initial value; otherwise widget uses session state
                if reset_dates and 'start_date_input' not in st.session_state:
                    start_date = st.date_input("Start Date", key='start_date_input', value=INITIAL_DATES['start_date_input'])
                else:
                    start_date = st.date_input("Start Date", key='start_date_input')
            with col_date_to:
                if reset_dates and 'end_date_input' not in st.session_state:
                    end_date = st.date_input("End Date", key='end_date_input', value=INITIAL_DATES['end_date_input'])
                else:
                    end_date = st.date_input("End Date", key='end_date_input')

        # 3. Advanced Filters - FOUR Focused Tabs (Design Maintained)
        tab_doc_no, tab_item_hscode, tab_vessel_origin, tab_importer = st.tabs(
            ["Document Numbers", "Item & HS Code", "Vessel & Origin", "Importer Details"]
        )

        with tab_doc_no:
            st.markdown("### Document Reference Numbers")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.text_input("BOE Number", placeholder="e.g., BOE-2024-001", key='boe_number')
            with col2:
                st.text_input("BL Number", placeholder="e.g., MAEU123456", key='bl_number')
            with col3:
                st.text_input("Importer TIN", placeholder="e.g., 123456789", key='importer_tin')

        with tab_item_hscode:
            st.markdown("### HS Code Filtering")
            with st.container(border=True):
                render_suggestion_section(
                    title="HS Code",
                    input_key='hscode_search_input',
                    suggestions_state_key='hscode_suggestions',
                    selected_state_key='selected_hscodes',
                    endpoint_url=None,
                    min_length=4
                )

            st.markdown("### Goods Description")
            st.text_area(
                "Goods Description Keywords (comma separated)",
                placeholder="Search keywords (e.g., television, laptop)",
                height=100,
                key='goods_description_keywords'
            )

        with tab_vessel_origin:
            st.markdown("### Vessel Filtering")
            with st.container(border=True):
                render_suggestion_section(
                    title="Vessel Name",
                    input_key='vessel_search_input',
                    suggestions_state_key='vessel_suggestions',
                    selected_state_key='selected_vessel_names',
                    endpoint_url=VESSEL_SUGGEST_ENDPOINT,
                    min_length=3
                )

            st.markdown("### Country of Origin")
            st.selectbox(
                "Country of Origin",
                COUNTRIES,
                key='country_of_origin',
                index=None,
                help="Filter by country of origin"
            )

        with tab_importer:
            st.markdown("### Importer Name Filtering")
            with st.container(border=True):
                render_suggestion_section(
                    title="Importer Name",
                    input_key='importer_search_input',
                    suggestions_state_key='importer_suggestions',
                    selected_state_key='selected_importer_names',
                    endpoint_url=IMPORTER_SUGGEST_ENDPOINT,
                    min_length=3
                )

        st.markdown("---")

        # Retrieve date values after widgets are created for use in collect_filter_params
        start_date = st.session_state.get('start_date_input', INITIAL_DATES['start_date_input'])
        end_date = st.session_state.get('end_date_input', INITIAL_DATES['end_date_input'])

        # 4. Search and Clear Buttons
        col_search_left, col_search_mid, col_search_right, col_clear = st.columns([1, 2, 1, 1])
        with col_search_mid:
            if st.button("**EXECUTE SEARCH**", type="primary", width='stretch', key='search_btn_bottom'):
                params = collect_filter_params(start_date, end_date)
                if run_search(params):
                    st.rerun()
        with col_clear:
            if st.button("**Clear Filters**", type="secondary", width='stretch', key='clear_filters_btn'):
                clear_all_filters()
                st.rerun()


    # --- 5. Report Display (UPDATED) ---
    results = st.session_state['report_results']

    if results is not None:
        st.markdown("## Search Results Overview")
        st.markdown("---")

        if isinstance(results, dict) and 'summary' in results:
            summary = results.get('summary', {})
            records = results.get('records', [])
        else:
            records = results if isinstance(results, list) else []
            summary = {"total_records": len(records), "grand_total_net_weight": 0.0, "grand_total_gross_weight": 0.0}

        total_records = summary.get('total_records', len(records))

        # --- Display Active Filters ---
        st.info(f"""
        **Displaying {total_records:,} records** for the period **{start_date}** to **{end_date}**.
        \n\n{get_active_filters_display()}
        """)

        if records:

            # --- DISPLAY MODERN SUMMARY AGGREGATION (Metric Cards) ---
            col1, col2, col3 = st.columns(3)

            col1.metric("Total Records Found", f"{total_records:,}")
            col2.metric("Total Net Weight", f"{summary.get('grand_total_net_weight', 0.0):,.2f} KG")
            col3.metric("Total Gross Weight", f"{summary.get('grand_total_gross_weight', 0.0):,.2f} KG")

            st.markdown("### Detailed Records Table")

            # --- DISPLAY TABLE WITH POPOVER FOR ITEMS ---
            try:
                # Process records into table format
                processed_data = []
                items_map = {}  # Store items separately by BOE No
                
                for record in records:
                    header = record.get('boeHeader', {})
                    boe_no = record.get('boe_no', 'N/A')
                    
                    row_data = {
                        'BOE Date': record.get('boe_date', 'N/A'),
                        'BOE No': boe_no,
                        'BL Number': record.get('bl_number', 'N/A'),
                        'Importer Name': header.get('importerName', 'N/A'),
                        'Importer TIN': header.get('importerTin', 'N/A'),
                        'Vessel Name': header.get('vesselName', 'N/A'),
                        'Total Items': header.get('calculatedTotalItems', 0),
                        'Net Weight (KG)': header.get('calculatedTotalNetWeight', 0.0),
                        'Gross Weight (KG)': header.get('calculatedTotalGrossWeight', 0.0),
                        # Hidden header fields
                        'ETA': header.get('eta', 'N/A'),
                        'ETD': header.get('etd', 'N/A'),
                        'Submission Date': header.get('submissionDate', 'N/A'),
                        'Declarant Name': header.get('declarantName', 'N/A'),
                        'Declarant TIN': header.get('declarantTin', 'N/A'),
                        'Shipment Type': header.get('shipmentType', 'N/A'),
                        'Vessel Nationality': header.get('vesselNationality', 'N/A'),
                        'Voyage Number': header.get('voyageNumber', 'N/A'),
                        'Rotation Number': header.get('rotationNumber', 'N/A'),
                        'Place of Landing': header.get('placeOfLanding', 'N/A'),
                        'Port of Discharge': header.get('portOfDischarge', 'N/A'),
                        'Release Office Code': header.get('releaseOfficeCode', 'N/A'),
                        'FOB Exchange Rate': header.get('fobExchangeRate', 'N/A'),
                        'CIF Exchange Rate': header.get('cifExchangeRate', 'N/A'),
                        'Examination Office Code': header.get('examinationOfficeCode', 'N/A'),
                        'Customs Office Code': header.get('customsOfficeCode', 'N/A'),
                    }
                    processed_data.append(row_data)
                    # Store items by BOE No for easy lookup
                    items_map[boe_no] = record.get('boeItem', [])
                
                # Create DataFrame with all columns
                df = pd.DataFrame(processed_data)
                
                if ITABLES_AVAILABLE:
                    st.markdown("💡 **Tip:** Click on a row to select it and view BOE items below")
                    st.markdown("")  # Add spacing
                    
                    # Display interactive table using itables
                    with st.container():
                        table_state = interactive_table(
                            df,
                            select="single",
                            classes="display nowrap compact cell-border stripe",
                            buttons=["pageLength", "copyHtml5", "csvHtml5", "excelHtml5", "colvis"],
                            scrollX=True,
                            pageLength=20,
                        )
                    
                    # Get selected rows from table state
                    selected_indices = table_state.get('selected_rows', []) if table_state else []
                    
                    # Add spacing before items section
                    st.markdown("")
                    
                    if selected_indices and len(selected_indices) > 0:
                        # Get the selected row (single selection)
                        selected_idx = selected_indices[0]
                        if selected_idx < len(df):
                            selected_row = df.iloc[selected_idx]
                            boe_no = selected_row['BOE No']
                            items = items_map.get(boe_no, [])
                            
                            # Show items directly without expander
                            st.markdown(f"### 📦 BOE Items: **{boe_no}**")
                            
                            if items:
                                items_data = []
                                for item in items:
                                    items_data.append({
                                        'BL No': item.get('blNo', 'N/A'),
                                        'HS Code': item.get('hsCode', 'N/A'),
                                        'No of Pkg': item.get('noOfPkg', 'N/A'),
                                        'FOB Amount': item.get('fobAmount', 'N/A'),
                                        'Item Number': item.get('itemNumber', 'N/A'),
                                        'Origin Country': item.get('itemOriginCountry', 'N/A'),
                                        'Origin Continent': item.get('itemOriginContinent', 'N/A'),
                                        'Net Weight': item.get('netWeight', 'N/A'),
                                        'Gross Weight': item.get('grossWeight', 'N/A'),
                                        # Hidden item fields
                                        'Crn': item.get('crn', 'N/A'),
                                        'CPC': item.get('cpc', 'N/A'),
                                        'Item Payable': item.get('itemPayable', 'N/A'),
                                        'Item Exempted': item.get('itemExempted', 'N/A'),
                                        'Freight Amount': item.get('freightAmount', 'N/A'),
                                        'Insurance Amount': item.get('insuranceAmount', 'N/A'),
                                        'Package Unit Code': item.get('packageUnitCode', 'N/A'),
                                        'Goods Description': item.get('itemGoodsDescription', 'N/A'),
                                    })
                                
                                items_df = pd.DataFrame(items_data)
                                
                                # Display items in interactive table
                                interactive_table(
                                    items_df,
                                    classes="display nowrap compact cell-border stripe",
                                    buttons=["pageLength", "copyHtml5", "csvHtml5", "excelHtml5", "colvis"],
                                    scrollX=True,
                                    pageLength=10,
                                )
                            else:
                                st.info("No items found for this BOE.")
                    else:
                        st.info("👆 Select a row from the table above to view its BOE items")
                else:
                    # Fallback to standard dataframe if itables is not available
                    st.markdown("💡 **Tip:** Click on a row number to view BOE items")
                    
                    column_config = {
                        'BOE Date': st.column_config.DateColumn(
                            "BOE Date",
                            format="YYYY-MM-DD"
                        ),
                        'Net Weight (KG)': st.column_config.NumberColumn(
                            "Net Weight (KG)",
                            format="%.2f"
                        ),
                        'Gross Weight (KG)': st.column_config.NumberColumn(
                            "Gross Weight (KG)",
                            format="%.2f"
                        ),
                        'Total Items': st.column_config.NumberColumn(
                            "Total Items",
                            format="%d"
                        ),
                    }
                    
                    st.dataframe(
                        df,
                        use_container_width=True,
                        column_config=column_config,
                        hide_index=False,
                        on_select="rerun"
                    )
                    
                    # Handle row selection
                    if st.session_state.get('dataframe_selection', {}).get('selection', {}).get('rows'):
                        selected_indices = st.session_state['dataframe_selection']['selection']['rows']
                        if selected_indices:
                            selected_idx = selected_indices[0]
                            selected_row = df.iloc[selected_idx]
                            boe_no = selected_row['BOE No']
                            items = items_map.get(boe_no, [])
                            
                            with st.popover(f"📦 Items for BOE: {boe_no}"):
                                st.markdown(f"### BOE Items: **{boe_no}**")
                                
                                if items:
                                    items_data = []
                                    for item in items:
                                        items_data.append({
                                            'BL No': item.get('blNo', 'N/A'),
                                            'HS Code': item.get('hsCode', 'N/A'),
                                            'No of Pkg': item.get('noOfPkg', 'N/A'),
                                            'FOB Amount': item.get('fobAmount', 'N/A'),
                                            'Item Number': item.get('itemNumber', 'N/A'),
                                            'Origin Country': item.get('itemOriginCountry', 'N/A'),
                                            'Origin Continent': item.get('itemOriginContinent', 'N/A'),
                                            'Net Weight': item.get('netWeight', 'N/A'),
                                            'Gross Weight': item.get('grossWeight', 'N/A'),
                                            # Hidden item fields
                                            'Crn': item.get('crn', 'N/A'),
                                            'CPC': item.get('cpc', 'N/A'),
                                            'Item Payable': item.get('itemPayable', 'N/A'),
                                            'Item Exempted': item.get('itemExempted', 'N/A'),
                                            'Freight Amount': item.get('freightAmount', 'N/A'),
                                            'Insurance Amount': item.get('insuranceAmount', 'N/A'),
                                            'Package Unit Code': item.get('packageUnitCode', 'N/A'),
                                            'Goods Description': item.get('itemGoodsDescription', 'N/A'),
                                        })
                                    
                                    items_df = pd.DataFrame(items_data)
                                    st.dataframe(items_df, use_container_width=True, hide_index=True)
                                else:
                                    st.info("No items found for this BOE.")
                
            except Exception as e:
                st.error(f"Error processing JSON results for display. Error: {e}")
                import traceback
                st.code(traceback.format_exc())
                st.json(records[0] if records else "No Data")
        else:
            st.warning("No records found matching the specified criteria.")


# --- Authentication Check (Required in every page file) ---
if st.session_state.get('authentication_status'):
    boe_cargo_report_page()
else:
    st.error("Please log in on the Home page to access the reports.")