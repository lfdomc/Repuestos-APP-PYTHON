# app.py
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import os
import matplotlib.pyplot as plt

# === 🎨 Configuración de la página ===
st.set_page_config(
    page_title="📊 Indicadores Técnicos - Laboratorio",
    page_icon="🏥",
    layout="wide"
)

# === 🔌 Conexión segura a la base de datos ===
@st.cache_resource
def init_connection():
    try:
        db_password = st.secrets["db"]["password"]
    except KeyError:
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

# === 📥 Cargar y procesar datos de equipos ===
@st.cache_data(ttl=300)
def cargar_y_procesar_datos():
    df_vista = pd.read_sql("SELECT * FROM vista_indicadores_equipos", engine)
    df_equipos = pd.read_sql("SELECT id_equipo, fecha_instalacion, fecha_ultima_falla FROM equipos_instalados", engine)
    df = df_vista.merge(df_equipos, on='id_equipo', how='left')
    
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
    
    def dias_a_texto(dias):
        if pd.isna(dias) or dias <= 0:
            return "N/A"
        anos = dias / 365.25
        return f"{int(dias):,} días ({anos:.1f} años)"
    
    for col in ['dias_operativos', 'mtbf_dias', 'dias_desde_ultima_falla']:
        if col in df.columns:
            df[f"{col}_texto"] = df[col].apply(dias_a_texto)
    
    return df

# === 📦 Cargar y procesar análisis de repuestos ===
@st.cache_data(ttl=300)
def cargar_analisis_repuestos():
    df_rep = pd.read_sql("SELECT * FROM catalogo_repuestos", engine)
    df_inv = pd.read_sql("SELECT * FROM inventario_logistico", engine)
    df_pol = pd.read_sql("SELECT * FROM politica_stock_repuestos", engine)
    df_eq = pd.read_sql("SELECT * FROM equipos_instalados", engine)
    df_compat = pd.read_sql("SELECT * FROM compatibilidad", engine)
    df_mod = pd.read_sql("SELECT * FROM modelos", engine)
    
    equipos_por_modelo = df_eq.groupby('id_modelo').size().reset_index(name='total_equipos')
    compat_instalada = df_compat.merge(equipos_por_modelo, on='id_modelo', how='inner')
    
    politica_aplicada = []
    for _, row in compat_instalada.iterrows():
        repuesto_id = row['id_repuesto']
        modelo_id = row['id_modelo']
        total_equipos = row['total_equipos']
        
        pol_filtro = df_pol[df_pol['id_repuesto'] == repuesto_id].copy()
        pol_aplicable = pol_filtro[
            (pol_filtro['equipos_min'] <= total_equipos) & 
            (pol_filtro['equipos_max'].isna() | (pol_filtro['equipos_max'] >= total_equipos))
        ]
        if not pol_aplicable.empty:
            stock_req = pol_aplicable.iloc[0]['stock_minimo']
            politica_aplicada.append({
                'id_repuesto': repuesto_id,
                'id_modelo': modelo_id,
                'stock_minimo': stock_req,
                'total_equipos': total_equipos
            })
    
    if not politica_aplicada:
        return pd.DataFrame()
    
    df_pol_aplicada = pd.DataFrame(politica_aplicada)
    stock_min_total = df_pol_aplicada.groupby('id_repuesto')['stock_minimo'].sum().reset_index()
    stock_min_total.columns = ['id_repuesto', 'stock_minimo_total']
    
    if df_inv.empty:
        df_inv_for_merge = pd.DataFrame(columns=['id_repuesto', 'stock_actual'])
    else:
        df_inv_for_merge = df_inv[['id_repuesto', 'stock_actual']]
    
    df_stock = stock_min_total.merge(df_inv_for_merge, on='id_repuesto', how='left')
    df_stock['stock_actual'] = pd.to_numeric(df_stock['stock_actual'], errors='coerce').fillna(0)
    df_stock = df_stock.merge(df_rep[['id_repuesto', 'descripcion', 'tipo_repuesto', 'criticidad']], on='id_repuesto', how='left')
    
    df_pol_aplicada = df_pol_aplicada.merge(df_mod[['id_modelo', 'nombre_modelo']], on='id_modelo', how='left')
    modelos_por_repuesto = df_pol_aplicada.groupby('id_repuesto')['nombre_modelo'].apply(
        lambda x: ', '.join(sorted(x.unique()))
    ).reset_index()
    modelos_por_repuesto.columns = ['id_repuesto', 'modelos_asociados']
    
    df_stock = df_stock.merge(modelos_por_repuesto, on='id_repuesto', how='left')
    df_stock['deficit'] = (df_stock['stock_minimo_total'] - df_stock['stock_actual']).clip(lower=0)
    
    return df_stock[df_stock['deficit'] > 0].sort_values('deficit', ascending=False)

# === 🖥️ Interfaz principal ===
st.title("🏥 Dashboard de Gestión Técnica")

vista = st.sidebar.radio("Seleccionar vista", ["📊 Indicadores de Equipos", "📦 Análisis de Repuestos"])

if vista == "📊 Indicadores de Equipos":
    st.markdown("### Equipos médicos - Indicadores de confiabilidad y alertas")
    
    df = cargar_y_procesar_datos()
    
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

    criticos = len(df_filtrado[df_filtrado['Prioridad'] == '⚠️ CRÍTICO'])
    altas = len(df_filtrado[df_filtrado['Prioridad'] == '🔴 ALTA PRIORIDAD'])

    col1, col2, col3 = st.columns(3)
    col1.metric("Equipos críticos", criticos)
    col2.metric("Alta prioridad", altas)
    col3.metric("Total de equipos", len(df_filtrado))

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

    def resaltar_prioridad(val):
        color = 'lightcoral' if 'CRÍTICO' in val or 'ALTA' in val else 'white'
        return f'background-color: {color}'

    st.dataframe(df_display.style.applymap(resaltar_prioridad, subset=['Prioridad']))

    st.download_button(
        label="📥 Descargar datos (CSV)",
        data=df_display.to_csv(index=False).encode('utf-8'),
        file_name="indicadores_tecnicos.csv",
        mime="text/csv"
    )

else:  # Vista de repuestos
    st.markdown("### 📦 Análisis de Repuestos Críticos")
    st.caption("Basado en política de stock, compatibilidad y equipos instalados")
    
    df_repuestos = cargar_analisis_repuestos()
    
    if df_repuestos.empty:
        st.success("✅ Todos los repuestos cumplen con la política de stock.")
    else:
        # Filtros
        col1, col2 = st.columns(2)
        with col1:
            tipos = st.multiselect(
                "Filtrar por tipo de repuesto",
                options=df_repuestos['tipo_repuesto'].dropna().unique(),
                default=df_repuestos['tipo_repuesto'].dropna().unique()
            )
        with col2:
            criticidades = st.multiselect(
                "Filtrar por criticidad",
                options=sorted(df_repuestos['criticidad'].dropna().unique()),
                default=sorted(df_repuestos['criticidad'].dropna().unique())
            )
        
        df_filtrado = df_repuestos[
            (df_repuestos['tipo_repuesto'].isin(tipos)) &
            (df_repuestos['criticidad'].isin(criticidades))
        ]
        
        st.metric("Repuestos críticos", len(df_filtrado))
        
        # Gráfico: Top 10 por déficit
        top10 = df_filtrado.nlargest(10, 'deficit')
        if not top10.empty:
            st.subheader("🔝 Top 10 Repuestos por Déficit")
            fig, ax = plt.subplots(figsize=(10, 6))
            bars = ax.barh(top10['descripcion'], top10['deficit'], color='steelblue')
            ax.set_xlabel('Déficit (unidades)')
            ax.set_title('Repuestos que requieren reposición urgente')
            for bar in bars:
                width = bar.get_width()
                ax.text(width + 0.1, bar.get_y() + bar.get_height()/2, 
                        f'{int(width)}', va='center', ha='left')
            plt.tight_layout()
            st.pyplot(fig)
        
        # Tabla detallada
        columnas_mostrar = [
            'id_repuesto', 'descripcion', 'tipo_repuesto', 'criticidad',
            'stock_actual', 'stock_minimo_total', 'deficit', 'modelos_asociados'
        ]
        df_display = df_filtrado[columnas_mostrar].rename(columns={
            'descripcion': 'Descripción',
            'tipo_repuesto': 'Tipo',
            'criticidad': 'Criticidad',
            'stock_actual': 'Stock actual',
            'stock_minimo_total': 'Stock mínimo requerido',
            'deficit': 'Déficit',
            'modelos_asociados': 'Modelos compatibles'
        })
        
        st.dataframe(df_display)
        
        st.download_button(
            label="📥 Descargar lista de compra (CSV)",
            data=df_display.to_csv(index=False).encode('utf-8'),
            file_name="lista_compra_repuestos.csv",
            mime="text/csv"
        )

st.markdown("---")
st.caption("Actualizado automáticamente cada 5 minutos • Datos desde PostgreSQL en Aiven")