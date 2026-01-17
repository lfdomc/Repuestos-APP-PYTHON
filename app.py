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