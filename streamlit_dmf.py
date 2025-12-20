import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

# 1. Inicjalizacja sesji
session = get_active_session()

st.set_page_config(layout="wide")
st.title("📊 Monitor Jakości Danych (DMF)")
st.write("Wyniki jakości danych dla tabeli **SGH.PUBLIC.BAZA_KLIENTOW_WWW**.")

# Parametry Twojego zapytania
REF_NAME = 'SGH.PUBLIC.BAZA_KLIENTOW_WWW'
REF_DOMAIN = 'table'

def get_dmf_results():
    sql = f"""
    SELECT 
        MEASUREMENT_TIME::timestamp as CZAS,
        METRIC_NAME,
        VALUE::float as WARTOSC
    FROM TABLE(SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS(
        REF_ENTITY_NAME => '{REF_NAME}',
        REF_ENTITY_DOMAIN => '{REF_DOMAIN}'
    ))
    ORDER BY CZAS ASC
    """
    return session.sql(sql).to_pandas()

# Główna sekcja aplikacji
try:
    df = get_dmf_results()
    # st.dataframe(df)

    if not df.empty:
        # Wybór metryki
        metrics = df['METRIC_NAME'].unique()
        selected_metric = st.selectbox("Wybierz metrykę:", metrics)

        # Filtrowanie
        chart_data = df[df['METRIC_NAME'] == selected_metric]

        # Wskaźniki i wykres
        col1, col2 = st.columns([1, 3])
        
        with col1:
            last_val = chart_data['WARTOSC'].iloc[-1]
            st.metric("Ostatni wynik", f"{last_val:.2f}")
            st.dataframe(chart_data.sort_values('CZAS', ascending=False), use_container_width=True)

        with col2:
            st.subheader(f"Trend: {selected_metric}")
            st.line_chart(chart_data, x='CZAS', y='WARTOSC')
            
    else:
        st.warning("Brak danych historycznych. Uruchom DMF w Worksheet.")

except Exception as e:
    # Sekcja 'except' musi być na tym samym poziomie co 'try'
    st.error(f"Wystąpił błąd aplikacji: {e}")
