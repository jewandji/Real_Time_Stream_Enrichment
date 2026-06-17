import streamlit as st
import pandas as pd
import psycopg2
import time
import pydeck as pdk
import plotly.express as px

st.set_page_config(page_title="FlightTracker", page_icon="✈️", layout="wide")

@st.cache_resource
def init_connection():
    return psycopg2.connect(
        host="localhost", database="aviation_weather", user="admin", password="password", port="5432"
    )

conn = init_connection()

def get_data():
    query = """
    SELECT icao24, callsign, longitude, latitude, altitude, velocity, true_track,
           weather_temp, weather_wind_speed, weather_wind_dir, last_updated 
    FROM enriched_flights WHERE longitude IS NOT NULL AND latitude IS NOT NULL;
    """
    return pd.read_sql(query, conn)

# --- BARRE LATÉRALE (FILTRES & NAVIGATION) ---
st.sidebar.title("✈️ Menu Principal")

# La navigation avec mémorisation d'état !
menu = st.sidebar.radio(
    "📍 Choisissez une vue :",
    ["Carte en Direct", "Analyse Météo", "Données Brutes"]
)

st.sidebar.markdown("---")
st.sidebar.header("🎛️ Filtres de données")
auto_refresh = st.sidebar.checkbox("Rafraîchissement auto (5s)", value=True)
alt_min, alt_max = st.sidebar.slider("Altitude (mètres)", 0, 15000, (0, 15000), step=500)
wind_min = st.sidebar.slider("Vent minimum (km/h)", 0.0, 150.0, 0.0, step=5.0)

st.title("🌍 Real-Time Stream Enrichment")

placeholder = st.empty()

with placeholder.container():
    df_raw = get_data()
    
    if df_raw.empty:
        st.warning("En attente des données Kafka...")
    else:
        df = df_raw[(df_raw['altitude'] >= alt_min) & (df_raw['altitude'] <= alt_max) & (df_raw['weather_wind_speed'] >= wind_min)]

        # KPIs
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("✈️ Vols traqués", len(df))
        col2.metric("⛰️ Altitude moyenne", f"{round(df['altitude'].mean(), 0)} m" if not df.empty else "0 m")
        col3.metric("💨 Vent moyen", f"{round(df['weather_wind_speed'].mean(), 1)} km/h" if not df.empty else "0 km/h")
        col4.metric("🌡️ Température moy.", f"{round(df['weather_temp'].mean(), 1)} °C" if not df.empty else "0 °C")

        st.markdown("---")

        # --- ROUTAGE DES VUES ---
        if menu == "Carte en Direct":
            if not df.empty:
                # Utilisation de ScatterplotLayer pour être sûr à 100% que les points s'affichent
                layer = pdk.Layer(
                    "ScatterplotLayer",
                    data=df,
                    get_position=["longitude", "latitude"],
                    get_radius=15000, # Rayon de 15km pour bien les voir
                    get_fill_color="[weather_wind_speed > 30 ? 255 : 0, 100, weather_wind_speed > 30 ? 0 : 255, 200]",
                    pickable=True
                )
                view_state = pdk.ViewState(latitude=46.603354, longitude=1.888334, zoom=4.5, pitch=0) # Centré sur la France
                
                st.pydeck_chart(pdk.Deck(
                    map_style=None,
                    initial_view_state=view_state,
                    layers=[layer],
                    tooltip={"text": "Vol: {callsign}\nVent: {weather_wind_speed} km/h\nAlt: {altitude} m"}
                ))
            else:
                st.info("Aucun vol ne correspond à ces filtres.")

        elif menu == "Analyse Météo":
            if not df.empty:
                # Première ligne de graphiques (ceux que tu as déjà)
                c1, c2 = st.columns(2)
                with c1:
                    st.subheader("💨 Impact du vent sur la vitesse")
                    fig_scatter = px.scatter(
                        df, x="weather_wind_speed", y="velocity", color="altitude", hover_name="callsign",
                        color_continuous_scale="Viridis"
                    )
                    st.plotly_chart(fig_scatter, use_container_width=True)
                with c2:
                    st.subheader("🧭 Distribution des Caps")
                    fig_polar = px.scatter_polar(
                        df, r="velocity", theta="true_track", hover_name="callsign", start_angle=90, direction="clockwise"
                    )
                    st.plotly_chart(fig_polar, use_container_width=True)

                st.markdown("---")

                # Deuxième ligne de graphiques (Les nouveautés !)
                c3, c4 = st.columns(2)
                with c3:
                    st.subheader("📊 Distribution des Altitudes")
                    # Un bel histogramme pour voir les paliers de vol
                    fig_hist = px.histogram(
                        df, x="altitude", nbins=20,
                        labels={"altitude": "Altitude (m)"},
                        color_discrete_sequence=['#00C4B4'] # Couleur stylée
                    )
                    fig_hist.update_layout(yaxis_title="Nombre d'avions")
                    st.plotly_chart(fig_hist, use_container_width=True)
                    
                with c4:
                    st.subheader("⚠️ Top 5 : Pires conditions de vent")
                    # On isole les 5 avions affrontant le plus de vent
                    top_wind = df.nlargest(5, 'weather_wind_speed')[['callsign', 'altitude', 'weather_wind_speed', 'weather_temp']]
                    top_wind.columns = ["Vol", "Altitude (m)", "Vent (km/h)", "Température (°C)"]
                    # On affiche un tableau propre sans l'index
                    st.dataframe(top_wind, use_container_width=True, hide_index=True)

        elif menu == "Données Brutes":
            st.dataframe(df.head(50), use_container_width=True)

if auto_refresh:
    time.sleep(5)
    st.rerun()