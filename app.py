# app.py
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import os

# === 🎨 Configuración de la página ===
st.set_page_config(
    page_title="📊 Indicadores Técnicos - Laboratorio",
    page_icon="🏥",
    layout="wide"
)

# === 🔌 Conexión segura a la base de datos ===
@st.cache_resource
def init_connection():
    # En Streamlit Cloud: usa secrets
    # En local: usa .env (si existe)
    try:
        # Intentar cargar desde Streamlit Secrets
        db_password = st.secrets["db"]["password"]
    except KeyError:
        # Si no está, intentar desde variables de entorno (desarrollo local)
        from dotenv import load_dotenv
        load_dotenv()
        db_password = os.getenv("DB_PASSWORD")
        if not db_password:
            st.error("❌ No se encontró la contraseña de la base de datos. Configura 'DB_PASSWORD' en secrets o .env")
            st.stop()

    engine = create_engine(
        f"postgresql://avnadmin:{db_password}@"
        "repuestos-lfdomc-bc58.i.aivencloud.com:27168/defaultdb"
    )
    return engine

engine = init_connection()

# === 📥 Cargar y procesar datos ===
@st.cache_data(ttl=300)  # Actualiza cada 5 minutos
def cargar_y_procesar_datos():
    df_vista = pd.read_sql("SELECT * FROM vista_indicadores_equipos", engine)
    df_equipos = pd.read_sql("SELECT id_equipo, fecha_instalacion, fecha_ultima_falla FROM equipos_instalados", engine)
    df = df_vista.merge(df_equipos, on='id_equipo', how='left')
    
    # Calcular alertas
    hoy = pd.Timestamp.today()
    proximas_fallas = []
    alertas = []
    
    for _, row in df.iterrows():
        mtbf = row['mtbf_dias']
        fecha_ultima = row['fecha_ultima_falla']
        fecha_inst = row['fecha_instalacion']
        
        proxima_fecha = None
        if pd.notna(mtbf) and mtbf > 0:
            if pd.notna(fecha_ultima):
                proxima_fecha = pd.Timestamp(fecha_ultima) + pd.Timedelta(days=mtbf)
            elif pd.notna(fecha_inst):
                proxima_fecha = pd.Timestamp(fecha_inst) + pd.Timedelta(days=mtbf)
        
        if pd.notna(proxima_fecha):
            dias_hasta = (proxima_fecha - hoy).days
            fecha_str = proxima_fecha.strftime('%Y-%m-%d')
            if dias_hasta < 0:
                texto = f"{fecha_str} (¡ya pasó!)"
                alerta = "⚠️ CRÍTICO"
            elif dias_hasta <= 90:
                texto = f"{fecha_str} (~{dias_hasta:,} días)"
                alerta = "🔴 ALTA PRIORIDAD"
            else:
                texto = f"{fecha_str} (~{dias_hasta:,} días)"
                alerta = "🟡 Normal"
        else:
            texto = "No estimable"
            alerta = "⚪ Sin datos"
        
        proximas_fallas.append(texto)
        alertas.append(alerta)
    
    df['Próxima falla estimada'] = proximas_fallas
    df['Prioridad'] = alertas
    
    # Formato legible para días
    def dias_a_texto(dias):
        if pd.isna(dias) or dias <= 0:
            return "N/A"
        anos = dias / 365.25
        return f"{int(dias):,} días ({anos:.1f} años)"
    
    for col in ['dias_operativos', 'mtbf_dias', 'dias_desde_ultima_falla']:
        if col in df.columns:
            df[f"{col}_texto"] = df[col].apply(dias_a_texto)
    
    return df

df = cargar_y_procesar_datos()

# === 🖥️ Interfaz de usuario ===
st.title("🏥 Dashboard de Gestión Técnica")
st.markdown("### Equipos médicos - Indicadores de confiabilidad y alertas")

# Filtros
col1, col2 = st.columns(2)
with col1:
    prioridad_filtro = st.multiselect(
        "Filtrar por prioridad",
        options=df['Prioridad'].unique(),
        default=df['Prioridad'].unique()
    )
with col2:
    modelo_filtro = st.multiselect(
        "Filtrar por modelo",
        options=df['nombre_modelo'].unique(),
        default=df['nombre_modelo'].unique()
    )

df_filtrado = df[
    (df['Prioridad'].isin(prioridad_filtro)) &
    (df['nombre_modelo'].isin(modelo_filtro))
]

# Métricas
criticos = len(df_filtrado[df_filtrado['Prioridad'] == '⚠️ CRÍTICO'])
altas = len(df_filtrado[df_filtrado['Prioridad'] == '🔴 ALTA PRIORIDAD'])

col1, col2, col3 = st.columns(3)
col1.metric("Equipos críticos", criticos)
col2.metric("Alta prioridad", altas)
col3.metric("Total de equipos", len(df_filtrado))

# Tabla
st.subheader("📋 Detalle de equipos")
columnas_mostrar = [
    'id_equipo', 'nombre_modelo', 'marca', 'codigo_referencia',
    'cantidad_fallas', 'dias_operativos_texto', 'mtbf_dias_texto',
    'confiabilidad_6m', 'dias_desde_ultima_falla_texto',
    'Próxima falla estimada', 'Prioridad'
]
df_display = df_filtrado[columnas_mostrar].rename(columns={
    'dias_operativos_texto': 'Días operativos',
    'mtbf_dias_texto': 'MTBF',
    'dias_desde_ultima_falla_texto': 'Días desde última falla'
})

# Estilo condicional
def resaltar_prioridad(val):
    color = 'lightcoral' if 'CRÍTICO' in val or 'ALTA' in val else 'white'
    return f'background-color: {color}'

st.dataframe(df_display.style.applymap(resaltar_prioridad, subset=['Prioridad']))

# Descarga
st.download_button(
    label="📥 Descargar datos (CSV)",
    data=df_display.to_csv(index=False).encode('utf-8'),
    file_name="indicadores_tecnicos.csv",
    mime="text/csv"
)

st.markdown("---")
st.caption("Actualizado automáticamente cada 5 minutos • Datos desde PostgreSQL en Aiven")