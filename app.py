# app.py
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
import os
import matplotlib.pyplot as plt
from io import BytesIO
from datetime import datetime, date

# === 🖼️ Metaetiquetas para vista previa en redes sociales (WhatsApp, LinkedIn, etc.) ===
preview_image_url = "https://raw.githubusercontent.com/lfdomc/Repuestos-APP-PYTHON/main/preview.png"
app_url = "https://repuestos-app-python-p96bvhkf58pm9yuujw5rha.streamlit.app/"

st.markdown(f"""
    <meta property="og:title" content="🏥 Dashboard de Gestión Técnica - Laboratorio">
    <meta property="og:description" content="Indicadores de confiabilidad de equipos médicos y análisis inteligente de repuestos críticos.">
    <meta property="og:image" content="{preview_image_url}">
    <meta property="og:url" content="{app_url}">
    <meta property="og:type" content="website">
    <meta name="twitter:card" content="summary_large_image">
""", unsafe_allow_html=True)

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

# === 📥 Función para crear archivo Excel en memoria ===
def to_excel(df_dict):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in df_dict.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    return output.getvalue()

# === 📊 Funciones de carga de datos ===
@st.cache_data(ttl=300)
def cargar_datos_maestros():
    """Carga datos maestros para los formularios."""
    tecnicos = pd.read_sql("SELECT id_tecnico, nombre FROM tecnicos WHERE activo = TRUE", engine)
    equipos = pd.read_sql("""
        SELECT e.id_equipo, e.id_cliente, m.nombre_modelo, c.nombre_cliente 
        FROM equipos_instalados e 
        JOIN modelos m ON e.id_modelo = m.id_modelo
        JOIN clientes c ON e.id_cliente = c.id_cliente
        WHERE e.estado = 'Activo'
    """, engine)
    repuestos = pd.read_sql("SELECT id_repuesto, descripcion FROM catalogo_repuestos", engine)
    contratos = pd.read_sql("SELECT id_contrato, id_cliente FROM contratos WHERE activo = TRUE", engine)
    
    return tecnicos, equipos, repuestos, contratos

@st.cache_data(ttl=300)
def cargar_analisis_stock():
    """Carga análisis de stock actual vs requerido."""
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
    
    return df_stock.sort_values('deficit', ascending=False)

@st.cache_data(ttl=300)
def cargar_costos_operativos():
    """Carga análisis de costos operativos por técnico y contrato."""
    df_costos = pd.read_sql("""
        SELECT 
            s.id_servicio,
            s.fecha,
            t.nombre AS tecnico,
            e.id_equipo,
            cli.nombre_cliente,
            s.duracion_horas,
            s.km_recorridos,
            COALESCE(SUM(cr.cantidad * cat.precio_unitario), 0) AS costo_repuestos,
            (s.duracion_horas * ((tec.salario_bruto * 1.35) / 160)) AS costo_tecnico,
            (s.km_recorridos * (750.00 / tec.vehiculo_km_l)) AS costo_combustible
        FROM servicios_tecnicos s
        LEFT JOIN tecnicos tec ON s.id_tecnico = tec.id_tecnico
        LEFT JOIN consumo_repuestos cr ON s.id_servicio = cr.id_servicio
        LEFT JOIN catalogo_repuestos cat ON cr.id_repuesto = cat.id_repuesto
        LEFT JOIN equipos_instalados e ON s.id_equipo = e.id_equipo
        LEFT JOIN clientes cli ON e.id_cliente = cli.id_cliente
        LEFT JOIN tecnicos t ON s.id_tecnico = t.id_tecnico
        GROUP BY s.id_servicio, s.fecha, t.nombre, e.id_equipo, cli.nombre_cliente, 
                 s.duracion_horas, s.km_recorridos, tec.salario_bruto, tec.vehiculo_km_l
        ORDER BY s.fecha DESC
    """, engine)
    
    return df_costos

@st.cache_data(ttl=300)
def cargar_indicadores_equipos():
    """Carga los indicadores de equipos como en el script original."""
    # Obtener datos básicos de equipos
    df_equipos = pd.read_sql("""
        SELECT 
            e.id_equipo,
            e.id_cliente,
            e.id_modelo,
            e.ano_fabricacion,
            e.fecha_instalacion,
            e.zona,
            e.tiempo_viaje,
            e.estado,
            e.tipo_contrato,
            e.tipo_cliente,
            e.observaciones,
            e.fecha_ultima_falla,
            e.cantidad_fallas,
            e.dias_operativos,
            m.nombre_modelo,
            m.marca,
            c.nombre_cliente,
            c.codigo_referencia
        FROM equipos_instalados e
        JOIN modelos m ON e.id_modelo = m.id_modelo
        JOIN clientes c ON e.id_cliente = c.id_cliente
    """, engine)
    
    # Calcular MTBF y confiabilidad
    df_equipos['mtbf_dias'] = np.where(
        df_equipos['cantidad_fallas'] > 0,
        df_equipos['dias_operativos'] / df_equipos['cantidad_fallas'],
        np.nan
    )
    
    # Calcular días desde última falla
    hoy = pd.Timestamp.today()
    df_equipos['fecha_ultima_falla_dt'] = pd.to_datetime(df_equipos['fecha_ultima_falla'])
    df_equipos['dias_desde_ultima_falla'] = np.where(
        pd.notna(df_equipos['fecha_ultima_falla_dt']),
        (hoy - df_equipos['fecha_ultima_falla_dt']).dt.days,
        np.nan
    )
    
    # Calcular confiabilidad a 6 meses (180 días)
    df_equipos['confiabilidad_6m'] = np.where(
        pd.notna(df_equipos['mtbf_dias']) & (df_equipos['mtbf_dias'] > 0),
        np.exp(-180 / df_equipos['mtbf_dias']),
        np.nan
    )
    
    # Calcular próximas fallas estimadas
    proximas_fallas = []
    alertas = []
    
    for _, row in df_equipos.iterrows():
        mtbf = row['mtbf_dias']
        fecha_ultima = row['fecha_ultima_falla_dt']
        fecha_inst = pd.to_datetime(row['fecha_instalacion'])
        
        proxima_fecha = None
        if pd.notna(mtbf) and mtbf > 0:
            if pd.notna(fecha_ultima):
                proxima_fecha = fecha_ultima + pd.Timedelta(days=mtbf)
            elif pd.notna(fecha_inst):
                proxima_fecha = fecha_inst + pd.Timedelta(days=mtbf)
        
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
    
    df_equipos['Próxima falla estimada'] = proximas_fallas
    df_equipos['Prioridad'] = alertas
    
    def dias_a_texto(dias):
        if pd.isna(dias) or dias <= 0:
            return "N/A"
        anos = dias / 365.25
        return f"{int(dias):,} días ({anos:.1f} años)"
    
    for col in ['dias_operativos', 'mtbf_dias', 'dias_desde_ultima_falla']:
        if col in df_equipos.columns:
            df_equipos[f"{col}_texto"] = df_equipos[col].apply(dias_a_texto)
    
    return df_equipos

# === 🖥️ Interfaz principal con menú en el header ===
st.title("🏥 Dashboard de Gestión Técnica")

# Menú de navegación
tab_registro, tab_equipos, tab_stock, tab_costos = st.tabs(["📝 Registro", "📊 Equipos", "📦 Stock", "💰 Costos"])

with tab_registro:
    st.header("📝 Registro de Nuevos Servicios Técnicos")
    
    # Cargar datos maestros
    tecnicos, equipos, repuestos, contratos = cargar_datos_maestros()
    
    # Formulario de registro
    col1, col2 = st.columns(2)
    
    with col1:
        # ¡IMPORTANTE! Campo para id_servicio definido por el usuario
        id_servicio = st.number_input("ID del servicio", min_value=1, step=1, 
                                    help="Número único que identifica este servicio")
        fecha_servicio = st.date_input("Fecha del servicio", value=date.today())
        tecnico_seleccionado = st.selectbox("Técnico", options=tecnicos['id_tecnico'], 
                                          format_func=lambda x: tecnicos[tecnicos['id_tecnico']==x]['nombre'].iloc[0])
        equipo_seleccionado = st.selectbox("Equipo", options=equipos['id_equipo'],
                                         format_func=lambda x: f"{x} - {equipos[equipos['id_equipo']==x]['nombre_modelo'].iloc[0]} ({equipos[equipos['id_equipo']==x]['nombre_cliente'].iloc[0]})")
        tipo_mant = st.selectbox("Tipo de mantenimiento", ["Preventivo", "Correctivo"])
        duracion_horas = st.number_input("Duración (horas)", min_value=0.0, step=0.5)
        km_recorridos = st.number_input("Kilómetros recorridos", min_value=0.0, step=1.0)
    
    with col2:
        # Buscar contrato asociado al equipo
        cliente_equipo = equipos[equipos['id_equipo']==equipo_seleccionado]['id_cliente'].iloc[0]
        contratos_equipo = contratos[contratos['id_cliente']==cliente_equipo]
        if not contratos_equipo.empty:
            contrato_seleccionado = st.selectbox("Contrato", options=contratos_equipo['id_contrato'])
        else:
            contrato_seleccionado = None
            st.warning("No hay contratos asociados a este cliente")
        
        observaciones = st.text_area("Observaciones")
    
    # Repuestos usados
    st.subheader("🔧 Repuestos utilizados")
    num_repuestos = st.number_input("Número de repuestos diferentes", min_value=0, max_value=10, value=0)
    
    repuestos_usados = []
    for i in range(num_repuestos):
        col_r1, col_r2 = st.columns(2)
        with col_r1:
            repuesto_id = st.selectbox(f"Repuesto {i+1}", options=repuestos['id_repuesto'],
                                     format_func=lambda x: repuestos[repuestos['id_repuesto']==x]['descripcion'].iloc[0],
                                     key=f"repuesto_{i}")
        with col_r2:
            cantidad = st.number_input(f"Cantidad {i+1}", min_value=1, value=1, key=f"cantidad_{i}")
        repuestos_usados.append({'id_repuesto': repuesto_id, 'cantidad': cantidad})
    
    # Botón de registro
    if st.button("💾 Registrar Servicio"):
        try:
            # Validar que el id_servicio no exista
            with engine.connect() as conn:
                existe = pd.read_sql(
                    "SELECT 1 FROM servicios_tecnicos WHERE id_servicio = %s", 
                    conn, params=[id_servicio]
                )
            
            if not existe.empty:
                st.error(f"❌ El ID de servicio {id_servicio} ya existe. Usa un ID diferente.")
            else:
                # Insertar servicio técnico
                with engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO servicios_tecnicos (id_servicio, fecha, id_tecnico, id_equipo, id_contrato, tipo_mant, duracion_horas, km_recorridos, observaciones)
                        VALUES (:id_servicio, :fecha, :id_tecnico, :id_equipo, :id_contrato, :tipo_mant, :duracion_horas, :km_recorridos, :observaciones);
                    """), {
                        'id_servicio': int(id_servicio),
                        'fecha': fecha_servicio,
                        'id_tecnico': tecnico_seleccionado,
                        'id_equipo': equipo_seleccionado,
                        'id_contrato': contrato_seleccionado,
                        'tipo_mant': tipo_mant,
                        'duracion_horas': duracion_horas,
                        'km_recorridos': km_recorridos,
                        'observaciones': observaciones
                    })
                    
                    # Insertar repuestos usados
                    for repuesto in repuestos_usados:
                        conn.execute(text("""
                            INSERT INTO consumo_repuestos (id_servicio, id_repuesto, cantidad)
                            VALUES (:id_servicio, :id_repuesto, :cantidad)
                            ON CONFLICT (id_servicio, id_repuesto) DO NOTHING;
                        """), {
                            'id_servicio': int(id_servicio),
                            'id_repuesto': repuesto['id_repuesto'],
                            'cantidad': repuesto['cantidad']
                        })
                
                st.success(f"✅ Servicio registrado exitosamente con ID: {id_servicio}")
                # Limpiar el formulario (opcional)
                st.experimental_rerun()
            
        except Exception as e:
            st.error(f"❌ Error al registrar el servicio: {str(e)}")
    
    # Registro de gastos diarios
    st.header("🍽️ Registro de Gastos Diarios")
    
    col_g1, col_g2 = st.columns(2)
    with col_g1:
        fecha_gastos = st.date_input("Fecha de gastos", value=date.today(), key="fecha_gastos")
        tecnico_gastos = st.selectbox("Técnico", options=tecnicos['id_tecnico'], 
                                    format_func=lambda x: tecnicos[tecnicos['id_tecnico']==x]['nombre'].iloc[0],
                                    key="tecnico_gastos")
    
    with col_g2:
        desayuno = st.number_input("Desayuno (CRC)", min_value=0.0, step=100.0, key="desayuno")
        almuerzo = st.number_input("Almuerzo (CRC)", min_value=0.0, step=100.0, key="almuerzo")
        cena = st.number_input("Cena (CRC)", min_value=0.0, step=100.0, key="cena")
        hospedaje = st.number_input("Hospedaje (CRC)", min_value=0.0, step=1000.0, key="hospedaje")
        parqueo = st.number_input("Parqueo (CRC)", min_value=0.0, step=100.0, key="parqueo")
        otros_gastos = st.number_input("Otros gastos (CRC)", min_value=0.0, step=100.0, key="otros_gastos")
    
    if st.button("💾 Registrar Gastos Diarios"):
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO dias_tecnicos (fecha, id_tecnico, viaticos_desayuno, viaticos_almuerzo, viaticos_cena, hospedaje, parqueo, otros_gastos)
                    VALUES (:fecha, :id_tecnico, :desayuno, :almuerzo, :cena, :hospedaje, :parqueo, :otros_gastos)
                    ON CONFLICT (fecha, id_tecnico) DO UPDATE SET
                        viaticos_desayuno = EXCLUDED.viaticos_desayuno,
                        viaticos_almuerzo = EXCLUDED.viaticos_almuerzo,
                        viaticos_cena = EXCLUDED.viaticos_cena,
                        hospedaje = EXCLUDED.hospedaje,
                        parqueo = EXCLUDED.parqueo,
                        otros_gastos = EXCLUDED.otros_gastos;
                """), {
                    'fecha': fecha_gastos,
                    'id_tecnico': tecnico_gastos,
                    'desayuno': desayuno,
                    'almuerzo': almuerzo,
                    'cena': cena,
                    'hospedaje': hospedaje,
                    'parqueo': parqueo,
                    'otros_gastos': otros_gastos
                })
            
            st.success("✅ Gastos diarios registrados exitosamente")
            
        except Exception as e:
            st.error(f"❌ Error al registrar los gastos: {str(e)}")

with tab_equipos:
    st.header("📊 Indicadores de Equipos Médicos")
    st.caption("Confiabilidad, MTBF y alertas predictivas")
    
    df_equipos = cargar_indicadores_equipos()
    
    # Filtros
    col1, col2 = st.columns(2)
    with col1:
        prioridad_filtro = st.multiselect(
            "Filtrar por prioridad",
            options=df_equipos['Prioridad'].unique(),
            default=df_equipos['Prioridad'].unique()
        )
    with col2:
        modelo_filtro = st.multiselect(
            "Filtrar por modelo",
            options=df_equipos['nombre_modelo'].unique(),
            default=df_equipos['nombre_modelo'].unique()
        )

    df_filtrado = df_equipos[
        (df_equipos['Prioridad'].isin(prioridad_filtro)) &
        (df_equipos['nombre_modelo'].isin(modelo_filtro))
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

    # Descarga en Excel
    excel_data = to_excel({"Indicadores_Equipos": df_display})
    st.download_button(
        label="📥 Descargar datos (Excel)",
        data=excel_data,
        file_name="indicadores_tecnicos.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

with tab_stock:
    st.header("📦 Análisis de Stock de Repuestos")
    st.caption("Basado en política de stock, compatibilidad y equipos instalados")
    
    df_stock = cargar_analisis_stock()
    
    if df_stock.empty:
        st.success("✅ Todos los repuestos cumplen con la política de stock.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            tipos = st.multiselect(
                "Filtrar por tipo de repuesto",
                options=df_stock['tipo_repuesto'].dropna().unique(),
                default=df_stock['tipo_repuesto'].dropna().unique()
            )
        with col2:
            criticidades = st.multiselect(
                "Filtrar por criticidad",
                options=sorted(df_stock['criticidad'].dropna().unique()),
                default=sorted(df_stock['criticidad'].dropna().unique())
            )
        
        df_filtrado = df_stock[
            (df_stock['tipo_repuesto'].isin(tipos)) &
            (df_stock['criticidad'].isin(criticidades))
        ]
        
        st.metric("Repuestos críticos", len(df_filtrado))
        
        # Mostrar top 10
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
        
        # Descarga en Excel
        excel_data = to_excel({"Lista_Compra_Repuestos": df_display})
        st.download_button(
            label="📥 Descargar lista de compra (Excel)",
            data=excel_data,
            file_name="lista_compra_repuestos.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

with tab_costos:
    st.header("💰 Análisis de Costos Operativos")
    st.caption("Costos reales por servicio técnico")
    
    df_costos = cargar_costos_operativos()
    
    if df_costos.empty:
        st.info("No hay datos de costos operativos disponibles.")
    else:
        # Filtros
        col1, col2 = st.columns(2)
        with col1:
            tecnico_filtro = st.multiselect("Filtrar por técnico", options=df_costos['tecnico'].unique())
        with col2:
            fecha_inicio = st.date_input("Fecha inicio", value=df_costos['fecha'].min())
            fecha_fin = st.date_input("Fecha fin", value=df_costos['fecha'].max())
        
        # Aplicar filtros
        df_filtrado = df_costos.copy()
        if tecnico_filtro:
            df_filtrado = df_filtrado[df_filtrado['tecnico'].isin(tecnico_filtro)]
        df_filtrado = df_filtrado[(df_filtrado['fecha'] >= fecha_inicio) & (df_filtrado['fecha'] <= fecha_fin)]
        
        # Métricas
        col1, col2, col3 = st.columns(3)
        col1.metric("Total servicios", len(df_filtrado))
        col2.metric("Costo técnico total", f"₡{df_filtrado['costo_tecnico'].sum():,.0f}")
        col3.metric("Costo repuestos total", f"₡{df_filtrado['costo_repuestos'].sum():,.0f}")
        
        # Tabla detallada
        df_display = df_filtrado.rename(columns={
            'id_servicio': 'ID Servicio',
            'fecha': 'Fecha',
            'tecnico': 'Técnico',
            'id_equipo': 'Equipo',
            'nombre_cliente': 'Cliente',
            'duracion_horas': 'Horas',
            'km_recorridos': 'Km',
            'costo_tecnico': 'Costo Técnico',
            'costo_combustible': 'Costo Combustible',
            'costo_repuestos': 'Costo Repuestos'
        })
        
        df_display['Costo Total'] = df_display['Costo Técnico'] + df_display['Costo Combustible'] + df_display['Costo Repuestos']
        
        st.dataframe(df_display)
        
        # Gráfico de costos
        if not df_display.empty:
            st.subheader("📊 Distribución de costos")
            costo_total = df_display['Costo Total'].sum()
            costo_tecnico = df_display['Costo Técnico'].sum()
            costo_combustible = df_display['Costo Combustible'].sum()
            costo_repuestos = df_display['Costo Repuestos'].sum()
            
            fig, ax = plt.subplots()
            labels = ['Técnico', 'Combustible', 'Repuestos']
            sizes = [costo_tecnico, costo_combustible, costo_repuestos]
            colors = ['lightblue', 'lightgreen', 'lightcoral']
            ax.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
            ax.axis('equal')
            st.pyplot(fig)

st.markdown("---")
st.caption("Actualizado automáticamente cada 5 minutos • Datos desde PostgreSQL en Aiven")