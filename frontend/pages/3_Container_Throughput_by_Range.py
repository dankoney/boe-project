# frontend/pages/3_Container_Throughput_by_Range.py
import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import date

try:
    from itables.streamlit import interactive_table
    ITABLES_AVAILABLE = True
except ImportError:
    ITABLES_AVAILABLE = False

st.set_page_config(page_title="Container Throughput", page_icon="Container", layout="wide")

def to_float(val, default=0.0):
    try: return float(str(val)) if val is not None and str(val).strip() not in ["", "None"] else default
    except: return default

def to_int(val, default=0):
    try: return int(float(str(val))) if val is not None and str(val).strip() not in ["", "None"] else default
    except: return default

# Elite styling
st.markdown("""
<style>
    .big-metric {font-size: 2.6rem !important; font-weight: 700 !important; color: #1f77b4;}
    .metric-card {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 2rem; border-radius: 16px; border: 1px solid #dee2e6;
        box-shadow: 0 8px 20px rgba(0,0,0,0.1); text-align: center;
    }
</style>
""", unsafe_allow_html=True)

API_URL = "http://127.0.0.1:8000/reports/containers-by-range"

for key in ['range_results', 'start_date', 'end_date']:
    if key not in st.session_state:
        st.session_state[key] = date(2025, 1, 1) if key == 'start_date' else (date.today() if key == 'end_date' else None)

# Hero
st.markdown("""
<div style='text-align:center; padding:3rem; background:linear-gradient(90deg,#1f77b4,#17a2b8); color:white; border-radius:20px; margin-bottom:2rem; box-shadow:0 12px 30px rgba(0,0,0,0.2);'>
    <h1 style='margin:0; font-size:3.5rem;'>Container Throughput by Range</h1>
    <p style='margin:0; font-size:1.5rem; opacity:0.95;'>Tema & Takoradi Ports</p>
</div>
""", unsafe_allow_html=True)

with st.expander("Define Report Period", expanded=not bool(st.session_state.range_results)):
    c1, c2 = st.columns(2)
    with c1: start = st.date_input("Start Date", value=st.session_state.start_date)
    with c2: end = st.date_input("End Date", value=st.session_state.end_date)
    if st.button("Generate Report", type="primary", use_container_width=True):
        with st.spinner("Loading intelligence..."):
            try:
                r = requests.get(API_URL, params={"start_date": str(start), "end_date": str(end)}, timeout=120)
                r.raise_for_status()
                st.session_state.range_results = r.json()
                st.session_state.start_date = start
                st.session_state.end_date = end
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

if st.session_state.range_results:
    data = st.session_state.range_results
    summary_df = pd.DataFrame(data["summary"])
    details_df = pd.DataFrame(data["details"]).copy()

    # Clean summary
    summary_clean = summary_df[~summary_df["Container Type"].astype(str).str.contains("TOTAL", case=False, na=False)]

    total_containers = summary_clean["Number Of Containers"].apply(to_int).sum()
    total_teus = summary_clean["TEUs"].apply(to_float).sum()
    avg_teu = round(total_teus / total_containers, 2) if total_containers > 0 else 0.0

    # Metrics
    c1, c2, c3 = st.columns(3)
    with c1: st.markdown(f"<div class='metric-card'><div class='big-metric'>{total_containers:,}</div><div>Containers</div></div>", True)
    with c2: st.markdown(f"<div class='metric-card'><div class='big-metric'>{total_teus:,.2f}</div><div>TEUs</div></div>", True)
    with c3: st.markdown(f"<div class='metric-card'><div class='big-metric'>{avg_teu:.2f}</div><div>Avg TEU/Container</div></div>", True)

    st.markdown(f"""
    <div style='text-align:center; padding:1rem; background:#f8f9fa; border-radius:12px; margin:2.5rem 0 1rem 0; font-size:1.4rem; color:#2c3e50; font-weight:500; border: 1px solid #e9ecef;'>
        <strong>Period:</strong> {st.session_state.start_date} to {st.session_state.end_date}
    </div>
    """, True)

    cdl1, cdl2 = st.columns(2)
    with cdl1:
        st.download_button("Download Summary CSV", summary_df.to_csv(index=False).encode(),
                         f"throughput_summary_{start}_{end}.csv", "text/csv", use_container_width=True)
    with cdl2:
        st.download_button("Download Details CSV", details_df.to_csv(index=False).encode(),
                         f"throughput_detailed_{start}_{end}.csv", "text/csv", use_container_width=True)

    tab_summary, tab_countries, tab_details = st.tabs(["Range Analysis",  "Top 10 Countries", "Detailed List"])

    # ===================================================================
    # Range Analysis
    # ===================================================================
    with tab_summary:
        st.markdown("## Port-Level Throughput Analysis")
        ports = ["Tema", "Takoradi"]
       # range_priority = ['ASIA', 'EUROPE', 'NORTH AMERICA', 'SOUTH AMERICA', 'AFRICA', 'MIDDLE EAST', 'OCEANIA']
        range_priority = ['AFRICA', 'FAR EAST', 'MED EUROPE', 'NORTH AMERICA', 'NORTH CONTINENT', 'OTHERS', 'UNITED KINGDOM']

        port_stats = {}
        for port in ports:
            dfp = summary_clean[summary_clean["Port of Destination"] == port]
            cont = dfp["Number Of Containers"].apply(to_int).sum()
            teus = dfp["TEUs"].apply(to_float).sum()
            if cont > 0:
                port_stats[port] = {"df": dfp, "cont": cont, "teus": teus}

        tab_labels = [f"{p.upper()} • {port_stats[p]['cont']:,} cont • {port_stats[p]['teus']:,.1f} TEUs"
                     if p in port_stats else f"{p.upper()} • No data" for p in ports]
        port_tabs = st.tabs(tab_labels)

        for idx, port in enumerate(ports):
            with port_tabs[idx]:
                if port not in port_stats:
                    st.info("No containers in this period.")
                    continue

                df_port = port_stats[port]["df"]
                cont = port_stats[port]["cont"]
                teus = port_stats[port]["teus"]
                share = (cont / total_containers) * 100 if total_containers > 0 else 0

                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Containers", f"{cont:,}")
                col2.metric("TEUs", f"{teus:,.2f}")
                col3.metric("Avg TEU", f"{teus/cont:.2f}" if cont else "0")
                col4.metric("National Share", f"{share:.1f}%")

                st.markdown("---")

                range_stats = []
                for rng in range_priority:
                    sub = df_port[df_port["Range"] == rng]
                    if sub.empty: continue
                    c = sub["Number Of Containers"].apply(to_int).sum()
                    t = sub["TEUs"].apply(to_float).sum()
                    pct = (c / cont) * 100
                    range_stats.append({"Range": rng, "Cont": c, "TEUs": t, "Pct": pct})

                if range_stats:
                    rdf = pd.DataFrame(range_stats).sort_values("TEUs", ascending=False)

                    col_chart1, col_chart2 = st.columns(2)
                    with col_chart1:
                        fig_bar = px.bar(rdf, x="TEUs", y="Range", orientation='h',
                                       title="TEUs by Origin Range", color="Range", height=500)
                        fig_bar.update_traces(texttemplate='%{x:,.0f}', textposition='outside')
                        fig_bar.update_layout(showlegend=False, yaxis={'categoryorder': 'total ascending'})
                        st.plotly_chart(fig_bar, use_container_width=True)

                    with col_chart2:
                        fig_pie = px.pie(rdf, values="Cont", names="Range", title="Container Share %", hole=0.4, height=500)
                        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
                        st.plotly_chart(fig_pie, use_container_width=True)

                    for _, row in rdf.iterrows():
                        with st.expander(f"{row['Range']} • {int(row['Cont']):,} cont ({row['Pct']:.1f}%) • {row['TEUs']:,.2f} TEUs"):
                            subset = df_port[df_port["Range"] == row["Range"]]
                            disp = subset.copy()
                            disp["TEUs"] = pd.to_numeric(disp["TEUs"], errors='coerce').fillna(0).round(2)
                            disp["Number Of Containers"] = pd.to_numeric(disp["Number Of Containers"], errors='coerce').fillna(0).astype(int)
                            st.dataframe(disp[["Container Type", "Number Of Containers", "TEUs"]], use_container_width=True, hide_index=True)

    # ===================================================================
    # DETAILED LIST
    # ===================================================================
    with tab_details:
        st.markdown(f"### Detailed Container List — {len(details_df):,} Records")
        details_display = details_df.copy()
        details_display.insert(0, "No.", range(1, len(details_display) + 1))
        if ITABLES_AVAILABLE:
            interactive_table(details_display, classes="display compact stripe hover", buttons=["csvHtml5", "excelHtml5"], pageLength=25)
        else:
            st.dataframe(details_display, use_container_width=True)

    # ===================================================================
    # TOP 10 COUNTRIES 
    # ===================================================================
    with tab_countries:
        st.markdown("## Top 10 Origin Countries by TEU Volume")
      #  st.markdown("### Ghana Ports")

        if "Country" not in details_df.columns or details_df["Country"].isna().all():
            st.warning("No country data available.")
        else:
            df_clean = details_df.copy()
            df_clean["TEUs"] = pd.to_numeric(df_clean["TEUs"], errors='coerce').fillna(0)
            df_clean["Is_40FT"] = df_clean["Container Type"].str.contains("40", na=False)
            df_clean["Is_20FT"] = df_clean["Container Type"].str.contains("20", na=False)

            # Top 10 countries
            country_stats = (
                df_clean[df_clean["Country"].notna() & (df_clean["Country"].str.strip() != "")]
                .groupby("Country", as_index=False)
                .agg({
                    "TEUs": "sum",
                    "Container No": "count",
                    "Is_40FT": "sum",
                    "Is_20FT": "sum"
                })
                .round({"TEUs": 2})
                .rename(columns={"Container No": "Total Containers"})
                .assign(
                    _40FT=lambda x: x["Is_40FT"].astype(int),
                    _20FT=lambda x: x["Is_20FT"].astype(int)
                )
                .drop(columns=["Is_40FT", "Is_20FT"])
                .sort_values("TEUs", ascending=False)
                .head(10)
                .reset_index(drop=True)
            )

            if country_stats.empty:
                st.info("No country data in this period.")
            else:
                country_stats.insert(0, "Rank", range(1, len(country_stats) + 1))

                # === RE-INTRODUCED: TEU Share Pie Chart (Clear & Beautiful) ===
                col1, col2 = st.columns([2.5, 2])
                with col1:
                    fig_bar = px.bar(country_stats, x="TEUs", y="Country", orientation='h',
                                   text="TEUs", color="TEUs", color_continuous_scale="Blues",
                                   title="Top 10 Countries by TEU Volume", height=580)
                    fig_bar.update_traces(texttemplate='%{text:,.1f}', textposition='outside')
                    fig_bar.update_layout(yaxis={'categoryorder': 'total ascending'}, showlegend=False)
                    st.plotly_chart(fig_bar, use_container_width=True)

                with col2:
                    st.markdown("#### TEU Share Distribution")
                    fig_pie = px.pie(country_stats, values="TEUs", names="Country", hole=0.45,
                                   title="Top 10 TEU Share", height=580,
                                   color_discrete_sequence=px.colors.sequential.Blues_r)
                    fig_pie.update_traces(textposition='inside', textinfo='percent+label', insidetextfont_size=14)
                    st.plotly_chart(fig_pie, use_container_width=True)

                # Volume Breakdown Table
                st.markdown("#### Volume Breakdown")
                st.dataframe(
                    country_stats[["Rank", "Country", "TEUs", "Total Containers", "_40FT", "_20FT"]]
                    .style.format({
                        "TEUs": "{:,.2f}",
                        "Total Containers": "{:,}",
                        "_40FT": "{:,}",
                        "_20FT": "{:,}"
                    })
                    .bar(subset=["TEUs"], color="#1f77b4"),
                    use_container_width=True, hide_index=True
                )

                # Country Deep Dive — Now with BL Numbers per Importer
                st.markdown("#### Country Deep Dive")
                for _, row in country_stats.iterrows():
                    with st.expander(f"{row['Rank']}. {row['Country']} • {row['TEUs']:,.1f} TEUs"):
                        subset = df_clean[df_clean["Country"] == row["Country"]]
                        
                        # Importer-level aggregation with actual BL numbers
                        importer_bl = (
                            subset.groupby("Importer")["BL Number"]
                            .apply(lambda x: ", ".join(sorted(x.dropna().astype(str).unique())[:8]) + 
                                  ("..." if len(x.dropna().unique()) > 8 else ""))
                            .reset_index()
                        )
                        importer_teus = subset.groupby("Importer", as_index=False)["TEUs"].sum().round(2)
                        importer_cont = subset.groupby("Importer", as_index=False)["Container No"].count()

                        importer_summary = importer_teus.merge(importer_cont, on="Importer").merge(importer_bl, on="Importer")
                        importer_summary = importer_summary.sort_values("TEUs", ascending=False).head(10)
                        importer_summary["Importer"] = importer_summary["Importer"].str.split().str.join(" ").str.upper()

                        st.dataframe(
                            importer_summary.rename(columns={"BL Number": "BL Numbers"})[["Importer", "TEUs", "Container No", "BL Numbers"]],
                            use_container_width=True,
                            hide_index=True
                        )

else:
    st.info("Select a date range and click **Generate Report** to begin.")