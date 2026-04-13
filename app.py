from io import BytesIO
import pandas as pd
import streamlit as st
from openpyxl.utils import get_column_letter

from database import crear_tablas
from google_sheets import (
    actualizar_campos_por_clave,
    buscar_fila_por_clave,
    construir_tabla_resumen_pacientes,
    construir_tabla_tomas_pendientes,
    leer_sheet_como_dataframe,
    obtener_columnas_por_toma,
    obtener_fila_como_diccionario,
    preparar_datos_hospitalarios,
    preparar_resumen_influenza_observaciones,
)
from logic import (
    aprobar_usuario,
    actualizar_estado_accion_calidad,
    actualizar_estado_no_conformidad,
    autenticar_usuario,
    buscar_voluntario_por_id,
    contar_acciones_abiertas,
    contar_acciones_vencidas,
    contar_no_conformidades_abiertas,
    contar_racks_activos,
    contar_visitas_pendientes,
    contar_voluntarios,
    exportar_a_excel,
    guardar_evidencia_calidad,
    listar_auditorias_calidad,
    listar_acciones_calidad,
    listar_evidencias_calidad,
    listar_hallazgos_auditoria,
    listar_no_conformidades,
    obtener_ocupacion_racks,
    obtener_usuarios_pendientes,
    registrar_auditoria_calidad,
    registrar_accion_calidad,
    registrar_hallazgo_auditoria,
    registrar_no_conformidad,
    registrar_usuario,
    registrar_voluntario,
    ver_alicuotas_pbmc_voluntario,
    ver_alicuotas_suero_voluntario,
    ver_rack_pbmc,
    ver_rack_suero,
    ver_racks,
    ver_visitas,
)
from logic import crear_admin_inicial

st.set_page_config(page_title="Sistema INER - Voluntarios", layout="wide")
crear_tablas()

crear_admin_inicial(
    st.secrets["admin_email"],
    st.secrets["admin_password"]
)


def aplicar_estilos():
    st.markdown(
        """
        <style>
        :root {
            --iner-ink: #16324f;
            --iner-accent: #1f7a8c;
            --iner-soft: #e7f4f4;
            --iner-warm: #f3efe7;
            --iner-border: rgba(22, 50, 79, 0.12);
        }

        .block-container {
            padding-top: 2rem;
        }

        .iner-hero {
            background: linear-gradient(135deg, rgba(31,122,140,0.16), rgba(243,239,231,0.9));
            border: 1px solid var(--iner-border);
            border-radius: 22px;
            padding: 1.3rem 1.4rem;
            margin-bottom: 1rem;
        }

        .iner-kicker {
            color: var(--iner-accent);
            font-size: 0.86rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            margin-bottom: 0.35rem;
        }

        .iner-title {
            color: var(--iner-ink);
            font-size: 2.1rem;
            line-height: 1.1;
            font-weight: 800;
            margin: 0;
        }

        .iner-copy {
            color: rgba(22, 50, 79, 0.82);
            margin-top: 0.55rem;
            margin-bottom: 0;
        }

        .iner-section {
            background: rgba(255, 255, 255, 0.78);
            border: 1px solid var(--iner-border);
            border-radius: 18px;
            padding: 0.9rem 1rem 0.4rem 1rem;
            margin: 0.7rem 0 1rem 0;
        }

        .iner-chip {
            display: inline-block;
            background: var(--iner-soft);
            color: var(--iner-ink);
            border-radius: 999px;
            padding: 0.25rem 0.75rem;
            font-size: 0.85rem;
            font-weight: 600;
            margin-bottom: 0.45rem;
        }

        div[data-testid="stMetric"] {
            background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(231,244,244,0.92));
            border: 1px solid var(--iner-border);
            border-radius: 18px;
            padding: 0.8rem;
        }

        div[data-testid="stDataFrame"] {
            border-radius: 16px;
            overflow: hidden;
        }

        div[data-testid="stDownloadButton"] button {
            background: linear-gradient(135deg, #1f7a8c, #16324f);
            color: white;
            border: none;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def tarjeta_seccion(etiqueta, titulo, descripcion=None):
    descripcion_html = f'<p class="iner-copy">{descripcion}</p>' if descripcion else ""
    st.markdown(
        f"""
        <div class="iner-section">
            <div class="iner-chip">{etiqueta}</div>
            <h3 style="margin:0;color:#16324f;">{titulo}</h3>
            {descripcion_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


aplicar_estilos()

def pantalla_acceso():
    st.title("Acceso al sistema")
    pestana_login, pestana_registro = st.tabs(["Iniciar sesión", "Crear cuenta"])

    with pestana_login:
        email_login = st.text_input("Correo", key="login_email")
        password_login = st.text_input("Contraseña", type="password", key="login_password")

        if st.button("Ingresar"):
            try:
                resultado = autenticar_usuario(email_login, password_login)

                if resultado["ok"]:
                    st.session_state["autenticado"] = True
                    st.session_state["usuario_email"] = resultado["email"]
                    st.session_state["es_admin"] = resultado["es_admin"]
                    st.rerun()
                else:
                    st.error(resultado["mensaje"])
            except Exception as e:
                st.error(f"Error al iniciar sesión: {e}")

    with pestana_registro:
        email_registro = st.text_input("Correo institucional o personal", key="registro_email")
        password_registro = st.text_input("Contraseña", type="password", key="registro_password")
        password_registro_2 = st.text_input("Confirmar contraseña", type="password", key="registro_password_2")

        if st.button("Crear cuenta"):
            try:
                if not email_registro or not password_registro:
                    st.warning("Completa correo y contraseña.")
                elif password_registro != password_registro_2:
                    st.warning("Las contraseñas no coinciden.")
                else:
                    registrar_usuario(email_registro, password_registro)
                    st.success("Cuenta creada. Queda pendiente de aprobación.")
            except Exception as e:
                st.error(f"No se pudo crear la cuenta: {e}")


if "autenticado" not in st.session_state:
    st.session_state["autenticado"] = False

if "es_admin" not in st.session_state:
    st.session_state["es_admin"] = False

if not st.session_state["autenticado"]:
    pantalla_acceso()
    st.stop()


def mostrar_biobanco():
    st.title("Sistema de Seguimiento de Voluntarios y Biobanco")

    st.subheader("Dashboard resumen")

    col1, col2, col3 = st.columns(3)

    with col1:
        total_voluntarios = contar_voluntarios()
        st.metric("Voluntarios registrados", total_voluntarios)

    with col2:
        total_pendientes = contar_visitas_pendientes()
        st.metric("Visitas pendientes", total_pendientes)

    with col3:
        total_racks = contar_racks_activos()
        st.metric("Racks activos", total_racks)

    try:
        ocupacion = obtener_ocupacion_racks()

        if ocupacion:
            st.subheader("Ocupación de racks")
            df_ocupacion = pd.DataFrame(
                ocupacion,
                columns=["ID Rack", "Tipo", "Capacidad", "Ocupadas"]
            )
            df_ocupacion["% Ocupación"] = (
                df_ocupacion["Ocupadas"] / df_ocupacion["Capacidad"] * 100
            ).round(2)

            st.dataframe(df_ocupacion, use_container_width=True)

    except Exception as e:
        st.warning(f"No se pudo mostrar el dashboard: {e}")

    st.subheader("Registro de voluntarios")

    with st.form("form_voluntario"):
        col1, col2 = st.columns(2)

        with col1:
            id_voluntario = st.text_input("ID del voluntario")
            expediente = st.text_input("Expediente")
            fecha_toma1 = st.date_input("Fecha Toma 1")
            apellido_paterno = st.text_input("Apellido paterno")
            apellido_materno = st.text_input("Apellido materno")
            nombre = st.text_input("Nombre")
            genero = st.selectbox("Género", ["M", "F"])
            residencia = st.text_input("Residencia")
            fecha_nacimiento = st.date_input("Fecha de nacimiento")

        with col2:
            edad = st.number_input("Edad", min_value=0, max_value=120, step=1)
            peso = st.number_input("Peso", min_value=0.0, step=0.1)
            estatura = st.number_input("Estatura", min_value=0.0, step=0.01)
            tubos_amarillos = st.number_input("Tubos amarillos", min_value=0, step=1)
            tubos_verdes = st.number_input("Tubos verdes", min_value=0, step=1)
            cantidad_pbmc = st.number_input("Número de alícuotas PBMC", min_value=0, step=1)
            conteo_celular = st.text_input("Conteo celular PBMC")
            correo = st.text_input("Correo")
            telefono = st.text_input("Teléfono")
            patologias = st.text_area("Patologías")
            observaciones = st.text_area("Observaciones")

        enviado = st.form_submit_button("Registrar voluntario")

    if enviado:
        datos = {
            "id_voluntario": id_voluntario,
            "expediente": expediente,
            "fecha_toma1": str(fecha_toma1),
            "apellido_paterno": apellido_paterno,
            "apellido_materno": apellido_materno,
            "nombre": nombre,
            "genero": genero,
            "residencia": residencia,
            "fecha_nacimiento": str(fecha_nacimiento),
            "edad": edad,
            "peso": peso,
            "estatura": estatura,
            "tubos_amarillos": tubos_amarillos,
            "tubos_verdes": tubos_verdes,
            "cantidad_pbmc": cantidad_pbmc,
            "conteo_celular": conteo_celular,
            "correo": correo,
            "telefono": telefono,
            "patologias": patologias,
            "observaciones": observaciones,
        }

        try:
            registrar_voluntario(datos)
            st.success(f"Voluntario {id_voluntario} registrado correctamente.")

            visitas = ver_visitas(id_voluntario)
            st.subheader("Visitas generadas")
            st.table(visitas)

        except Exception as e:
            st.error(f"Error al registrar: {e}")

    st.subheader("Racks registrados")

    try:
        racks = ver_racks()
        if racks:
            df_racks = pd.DataFrame(
                racks,
                columns=["ID Rack", "Tipo", "Capacidad", "Ocupadas"]
            )
            st.dataframe(df_racks, use_container_width=True)
        else:
            st.info("No hay racks registrados todavía.")
    except Exception as e:
        st.warning(f"No se pudieron mostrar los racks: {e}")

    st.subheader("Visualización de racks 9x9")
    col1, col2 = st.columns(2)

    with col1:
        st.write("### Suero")
        try:
            rack_suero = ver_rack_suero("SUERO_1")
            st.table(rack_suero)
        except Exception:
            st.info("No hay datos de suero todavía")

    with col2:
        st.write("### PBMC")
        try:
            rack_pbmc = ver_rack_pbmc("PBMC_1")
            st.table(rack_pbmc)
        except Exception:
            st.info("No hay datos de PBMC todavía")

    st.subheader("Exportar información")

    if st.button("Generar archivo Excel"):
        try:
            archivo = exportar_a_excel()

            with open(archivo, "rb") as f:
                st.download_button(
                    label="Descargar reporte Excel",
                    data=f,
                    file_name=archivo,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
        except Exception as e:
            st.error(f"Error al exportar: {e}")

    st.subheader("Consulta por ID de voluntario")

    id_busqueda = st.text_input("Ingresa el ID del voluntario a consultar")

    if st.button("Buscar voluntario"):
        try:
            voluntario = buscar_voluntario_por_id(id_busqueda)

            if voluntario:
                columnas_voluntario = [
                    "id_voluntario", "expediente", "fecha_toma1",
                    "apellido_paterno", "apellido_materno", "nombre",
                    "genero", "residencia", "fecha_nacimiento", "edad",
                    "peso", "estatura", "tubos_amarillos", "tubos_verdes",
                    "correo", "telefono", "patologias", "observaciones",
                ]

                df_voluntario = pd.DataFrame([voluntario], columns=columnas_voluntario)
                st.write("### Datos del voluntario")
                st.dataframe(df_voluntario, use_container_width=True)

                visitas = ver_visitas(id_busqueda)
                if visitas:
                    df_visitas = pd.DataFrame(
                        visitas,
                        columns=["Tipo toma", "Fecha programada", "Fecha real", "Estado"],
                    )
                    st.write("### Visitas")
                    st.dataframe(df_visitas, use_container_width=True)

                suero = ver_alicuotas_suero_voluntario(id_busqueda)
                if suero:
                    df_suero = pd.DataFrame(
                        suero,
                        columns=["Núm alícuota", "Fecha ingreso", "Rack", "Fila", "Columna", "Tipo toma"],
                    )
                    st.write("### Alícuotas de suero")
                    st.dataframe(df_suero, use_container_width=True)

                pbmc = ver_alicuotas_pbmc_voluntario(id_busqueda)
                if pbmc:
                    df_pbmc = pd.DataFrame(
                        pbmc,
                        columns=["Núm alícuota", "Conteo celular", "Fecha ingreso", "Rack", "Fila", "Columna", "Tipo toma"],
                    )
                    st.write("### Alícuotas PBMC")
                    st.dataframe(df_pbmc, use_container_width=True)

            else:
                st.warning("No se encontró un voluntario con ese ID.")

        except Exception as e:
            st.error(f"Error en la búsqueda: {e}")


def mostrar_proyecto_hospitalario():
    st.markdown(
        """
        <div class="iner-hero">
            <div class="iner-kicker">Proyecto Hospitalario</div>
            <h1 class="iner-title">Seguimiento hospitalario y tomas pendientes</h1>
            <p class="iner-copy">Vista operativa del Google Sheet con búsqueda clínica, edición por toma y resúmenes listos para revisión e impresión.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    try:
        df_sheet = leer_sheet_como_dataframe()
        df_sheet = preparar_datos_hospitalarios(df_sheet)
        df_sheet = preparar_resumen_influenza_observaciones(df_sheet)

        if "CLAVE DE LABORATORIO" not in df_sheet.columns:
            st.error("La hoja no contiene la columna 'CLAVE DE LABORATORIO'.")
            return

        tarjeta_seccion(
            "Consulta",
            "Búsqueda por clave de laboratorio",
            "Encuentra rápidamente registros clínicos por clave para inspección o seguimiento.",
        )
        clave_busqueda = st.text_input("Ingresa la clave de laboratorio", placeholder="Ej. VSR_H_001")

        if clave_busqueda:
            df_filtrado_clave = df_sheet[
                df_sheet["CLAVE DE LABORATORIO"].astype(str).str.contains(clave_busqueda, case=False, na=False)
            ]

            if not df_filtrado_clave.empty:
                st.write("### Resultado de búsqueda")
                st.dataframe(df_filtrado_clave, use_container_width=True)
            else:
                st.warning("No se encontró ningún paciente con esa clave de laboratorio.")

        tarjeta_seccion(
            "Edición",
            "Edición dinámica de tomas",
            "Carga un paciente por clave y modifica solo las columnas asociadas a la toma seleccionada.",
        )

        clave_edicion = st.text_input(
            "Clave de laboratorio para editar",
            placeholder="Ej. VSR_H_001",
            key="clave_edicion",
        )

        toma_seleccionada = st.selectbox(
            "Selecciona la toma",
            [f"T{i}" for i in range(1, 15)],
        )

        if st.button("Cargar paciente"):
            try:
                numero_fila = buscar_fila_por_clave(clave_edicion)

                if numero_fila:
                    fila_paciente = obtener_fila_como_diccionario(numero_fila)

                    st.session_state["fila_paciente"] = fila_paciente
                    st.session_state["clave_edicion_actual"] = clave_edicion

                    st.success(f"Paciente encontrado en fila {numero_fila}")
                else:
                    st.warning("No se encontró la clave")

            except Exception as e:
                st.error(f"Error al cargar paciente: {e}")

        if "fila_paciente" in st.session_state:
            fila_paciente = st.session_state["fila_paciente"]
            clave_actual = st.session_state["clave_edicion_actual"]

            numero_toma = int(toma_seleccionada.replace("T", ""))
            columnas_toma = obtener_columnas_por_toma(numero_toma)

            st.write(f"### Editando {toma_seleccionada}")

            if columnas_toma:
                cambios = {}

                col1, col2 = st.columns(2)

                for i, col in enumerate(columnas_toma):
                    valor_actual = fila_paciente.get(col, "")

                    with col1 if i % 2 == 0 else col2:
                        nuevo_valor = st.text_input(
                            col,
                            value=valor_actual,
                            key=f"{col}_{numero_toma}",
                        )

                    cambios[col] = nuevo_valor

                if st.button("Guardar cambios"):
                    try:
                        actualizar_campos_por_clave(clave_actual, cambios)
                        st.success("Cambios guardados correctamente en Google Sheets")

                        numero_fila = buscar_fila_por_clave(clave_actual)
                        fila_actualizada = obtener_fila_como_diccionario(numero_fila)
                        st.session_state["fila_paciente"] = fila_actualizada

                    except Exception as e:
                        st.error(f"Error al guardar: {e}")
            else:
                st.warning(f"No se encontraron columnas para {toma_seleccionada}")

        tarjeta_seccion("Resumen", "Distribución por diagnóstico")

        if "DIAGNOSTICO_GRUPO" in df_sheet.columns:
            conteo_diag = df_sheet["DIAGNOSTICO_GRUPO"].value_counts().reset_index()
            conteo_diag.columns = ["Diagnóstico agrupado", "Número de pacientes"]
            st.dataframe(conteo_diag, use_container_width=True)

        tarjeta_seccion("Resumen", "Distribución por grupo de edad")

        if "GRUPO_EDAD" in df_sheet.columns:
            conteo_edad = df_sheet["GRUPO_EDAD"].value_counts().reset_index()
            conteo_edad.columns = ["Grupo de edad", "Número de pacientes"]
            st.dataframe(conteo_edad, use_container_width=True)

        tarjeta_seccion("Cruce", "Subclasificación de edad y diagnóstico")

        if "GRUPO_EDAD" in df_sheet.columns and "DIAGNOSTICO_GRUPO" in df_sheet.columns:
            tabla_cruzada = pd.crosstab(
                df_sheet["GRUPO_EDAD"],
                df_sheet["DIAGNOSTICO_GRUPO"],
            )

            orden_edades = ["Bebé", "Niño", "Adolescente", "Adulto"]
            tabla_cruzada = tabla_cruzada.reindex(
                [edad for edad in orden_edades if edad in tabla_cruzada.index]
            )

            columnas_preferidas = [
                "COVID",
                "Coinfección COVID",
                "INFLUENZA",
                "Coinfección INFLUENZA",
                "VSR",
                "Coinfección VSR",
            ]

            columnas_existentes = list(tabla_cruzada.columns)
            columnas_extra = [col for col in columnas_existentes if col not in columnas_preferidas]
            columnas_finales = [col for col in columnas_preferidas if col in columnas_existentes] + columnas_extra

            tabla_cruzada = tabla_cruzada[columnas_finales]

            st.dataframe(tabla_cruzada, use_container_width=True)

        tarjeta_seccion(
            "Vigilancia",
            "Resumen de influenza y coinfecciones en observaciones",
            "Detección automática basada en el contenido de observaciones del Google Sheet.",
        )

        if "INFLUENZA_OBS_GRUPO" in df_sheet.columns:
            df_influenza = df_sheet[df_sheet["INFLUENZA_OBS_GRUPO"].notna()]

            if not df_influenza.empty:
                conteo_influenza = df_influenza["INFLUENZA_OBS_GRUPO"].value_counts().reset_index()
                conteo_influenza.columns = ["Influenza / coinfección detectada", "Número de pacientes"]
                st.dataframe(conteo_influenza, use_container_width=True)
            else:
                st.info("No se encontraron registros de influenza en observaciones.")

        df_tomas_pendientes = construir_tabla_tomas_pendientes(df_sheet)
        tarjeta_seccion(
            "Operación",
            "Tomas pendientes del día",
            "Lista operativa con folio, nombre, observaciones, toma pendiente y clave de laboratorio, lista para exportación.",
        )

        if not df_tomas_pendientes.empty:
            st.dataframe(df_tomas_pendientes, use_container_width=True)

            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df_tomas_pendientes.to_excel(writer, sheet_name="Tomas pendientes", index=False)
                worksheet = writer.sheets["Tomas pendientes"]

                for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row, min_col=1, max_col=1):
                    for cell in row:
                        cell.number_format = "@"
                        cell.value = str(cell.value) if cell.value is not None else ""

                for col_idx, column_cells in enumerate(worksheet.columns, start=1):
                    max_length = 0

                    for cell in column_cells:
                        cell_value = "" if cell.value is None else str(cell.value)
                        max_length = max(max_length, len(cell_value))

                    adjusted_width = min(max(max_length + 2, 14), 40)
                    worksheet.column_dimensions[get_column_letter(col_idx)].width = adjusted_width

            st.download_button(
                label="Descargar tomas pendientes en Excel",
                data=buffer.getvalue(),
                file_name="tomas_pendientes_lit.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            st.info("No se encontraron tomas pendientes en columnas de ingreso a LIT.")

        tarjeta_seccion("Base", "Datos completos del Google Sheet")
        st.dataframe(df_sheet, use_container_width=True)

        tarjeta_seccion("Base", "Resumen general de pacientes")
        df_resumen_pacientes = construir_tabla_resumen_pacientes(df_sheet)
        st.dataframe(df_resumen_pacientes, use_container_width=True)

    except Exception as e:
        st.error(f"Error al leer Google Sheets: {e}")


def mostrar_calidad():
    st.markdown(
        """
        <div class="iner-hero">
            <div class="iner-kicker">Sistema de calidad</div>
            <h1 class="iner-title">Seguimiento de no conformidades y acciones</h1>
            <p class="iner-copy">Espacio operativo para apoyar la trazabilidad requerida en validación ISO 9001 dentro de la misma plataforma.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("No conformidades abiertas", contar_no_conformidades_abiertas())
    with col2:
        st.metric("Acciones abiertas", contar_acciones_abiertas())
    with col3:
        st.metric("Acciones vencidas", contar_acciones_vencidas())

    tab1, tab2, tab3, tab4 = st.tabs(["No conformidades", "Acciones", "Auditorías", "Seguimiento"])

    with tab1:
        tarjeta_seccion(
            "Registro",
            "Nueva no conformidad",
            "Documenta hallazgos, desviaciones, incidentes o incumplimientos con responsable y fecha compromiso.",
        )

        with st.form("form_no_conformidad", clear_on_submit=True):
            col1, col2 = st.columns(2)

            with col1:
                codigo = st.text_input("Código", placeholder="NC-2026-001")
                titulo = st.text_input("Título")
                origen = st.selectbox("Origen", ["Auditoría", "Proceso", "Cliente", "Proveedor", "Interno"])
                area = st.text_input("Área o proceso")
                severidad = st.selectbox("Severidad", ["Baja", "Media", "Alta", "Crítica"])
                fecha_deteccion = st.date_input("Fecha de detección")

            with col2:
                detectado_por = st.text_input("Detectado por")
                responsable = st.text_input("Responsable")
                fecha_compromiso = st.date_input("Fecha compromiso")
                descripcion = st.text_area("Descripción")
                causa_raiz = st.text_area("Causa raíz")

            enviado_nc = st.form_submit_button("Guardar no conformidad")

        if enviado_nc:
            datos_nc = {
                "codigo": codigo.strip(),
                "titulo": titulo.strip(),
                "descripcion": descripcion.strip(),
                "origen": origen,
                "area": area.strip(),
                "severidad": severidad,
                "detectado_por": detectado_por.strip(),
                "responsable": responsable.strip(),
                "fecha_deteccion": str(fecha_deteccion),
                "fecha_compromiso": str(fecha_compromiso),
                "causa_raiz": causa_raiz.strip(),
            }

            campos_requeridos = [
                "codigo",
                "titulo",
                "descripcion",
                "area",
                "detectado_por",
                "responsable",
            ]

            if any(not datos_nc[campo] for campo in campos_requeridos):
                st.warning("Completa todos los campos obligatorios de la no conformidad.")
            else:
                try:
                    registrar_no_conformidad(datos_nc)
                    st.success("No conformidad registrada correctamente.")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo registrar la no conformidad: {e}")

        no_conformidades = listar_no_conformidades()
        st.subheader("No conformidades registradas")

        if no_conformidades:
            df_nc = pd.DataFrame(
                no_conformidades,
                columns=[
                    "ID", "Código", "Título", "Origen", "Área", "Severidad",
                    "Estado", "Detectado por", "Responsable", "Fecha detección",
                    "Fecha compromiso", "Causa raíz", "Fecha cierre",
                ],
            )
            st.dataframe(df_nc, use_container_width=True, hide_index=True)
        else:
            st.info("Todavía no hay no conformidades registradas.")

        tarjeta_seccion(
            "Estado",
            "Actualizar no conformidad",
            "Puedes mover hallazgos a En proceso y solo administradores pueden cerrarlos formalmente.",
        )

        if no_conformidades:
            opciones_estado_nc = {
                f"{codigo} | {titulo} | {estado}": id_nc
                for id_nc, codigo, titulo, _, _, _, estado, *_ in no_conformidades
            }

            with st.form("form_estado_nc"):
                seleccion_nc = st.selectbox("Selecciona la no conformidad", list(opciones_estado_nc.keys()))
                nuevo_estado_nc = st.selectbox("Nuevo estado", ["Abierta", "En proceso", "Cerrada"])
                causa_raiz_update = st.text_area("Actualizar causa raíz", key="causa_raiz_update")
                verificacion_cierre = st.text_area("Verificación de cierre", key="verificacion_cierre_nc")
                guardar_estado_nc = st.form_submit_button("Actualizar estado")

            if guardar_estado_nc:
                try:
                    actualizar_estado_no_conformidad(
                        id_no_conformidad=opciones_estado_nc[seleccion_nc],
                        nuevo_estado=nuevo_estado_nc,
                        causa_raiz=causa_raiz_update.strip() or None,
                        verificacion_cierre=verificacion_cierre.strip() or None,
                        es_admin=st.session_state.get("es_admin", False),
                    )
                    st.success("Estado de no conformidad actualizado.")
                    st.rerun()
                except PermissionError as e:
                    st.warning(str(e))
                except Exception as e:
                    st.error(f"No se pudo actualizar el estado: {e}")

            tarjeta_seccion(
                "Evidencia",
                "Adjuntar archivo a no conformidad",
                "Permite conservar soportes del hallazgo o del cierre en la misma plataforma.",
            )

            with st.form("form_evidencia_nc"):
                evidencia_nc = st.selectbox("No conformidad para evidencia", list(opciones_estado_nc.keys()), key="evidencia_nc")
                descripcion_evidencia_nc = st.text_input("Descripción de la evidencia", key="descripcion_evidencia_nc")
                archivo_nc = st.file_uploader(
                    "Archivo de evidencia",
                    key="archivo_evidencia_nc",
                    type=None,
                )
                guardar_evidencia_nc = st.form_submit_button("Guardar evidencia")

            if guardar_evidencia_nc:
                if archivo_nc is None:
                    st.warning("Selecciona un archivo para guardar la evidencia.")
                else:
                    try:
                        guardar_evidencia_calidad(
                            tipo_entidad="no_conformidad",
                            id_entidad=opciones_estado_nc[evidencia_nc],
                            nombre_archivo_original=archivo_nc.name,
                            contenido_archivo=archivo_nc.getvalue(),
                            descripcion=descripcion_evidencia_nc.strip(),
                            subido_por=st.session_state.get("usuario_email", ""),
                        )
                        st.success("Evidencia guardada correctamente.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"No se pudo guardar la evidencia: {e}")

            evidencia_nc_tabla = []
            for etiqueta, id_nc in opciones_estado_nc.items():
                for id_evidencia, nombre_archivo, descripcion, subido_por, ruta_archivo, created_at in listar_evidencias_calidad("no_conformidad", id_nc):
                    evidencia_nc_tabla.append(
                        {
                            "No conformidad": etiqueta,
                            "Archivo": nombre_archivo,
                            "Descripción": descripcion,
                            "Subido por": subido_por,
                            "Fecha": created_at,
                            "Ruta": ruta_archivo,
                        }
                    )

            if evidencia_nc_tabla:
                st.write("### Evidencias de no conformidades")
                st.dataframe(pd.DataFrame(evidencia_nc_tabla), use_container_width=True, hide_index=True)

    with tab2:
        tarjeta_seccion(
            "Acción",
            "Nueva acción correctiva o preventiva",
            "Cada acción queda ligada a una no conformidad para mantener trazabilidad y seguimiento.",
        )

        no_conformidades = listar_no_conformidades()
        opciones_nc = {
            f"{codigo} | {titulo}": id_nc
            for id_nc, codigo, titulo, *_ in no_conformidades
        }

        if not opciones_nc:
            st.info("Primero registra una no conformidad para poder asociar acciones.")
        else:
            with st.form("form_accion_calidad", clear_on_submit=True):
                referencia_nc = st.selectbox("No conformidad asociada", list(opciones_nc.keys()))
                col1, col2 = st.columns(2)

                with col1:
                    titulo_accion = st.text_input("Título de la acción")
                    tipo_accion = st.selectbox("Tipo de acción", ["Correctiva", "Preventiva", "Contención"])
                    responsable_accion = st.text_input("Responsable")

                with col2:
                    fecha_inicio = st.date_input("Fecha de inicio")
                    fecha_compromiso_accion = st.date_input("Fecha compromiso", key="fecha_compromiso_accion")
                    descripcion_accion = st.text_area("Descripción de la acción")

                enviada_accion = st.form_submit_button("Guardar acción")

            if enviada_accion:
                datos_accion = {
                    "id_no_conformidad": opciones_nc[referencia_nc],
                    "titulo": titulo_accion.strip(),
                    "descripcion": descripcion_accion.strip(),
                    "tipo_accion": tipo_accion,
                    "responsable": responsable_accion.strip(),
                    "fecha_inicio": str(fecha_inicio),
                    "fecha_compromiso": str(fecha_compromiso_accion),
                }

                campos_requeridos = ["titulo", "descripcion", "responsable"]
                if any(not datos_accion[campo] for campo in campos_requeridos):
                    st.warning("Completa todos los campos obligatorios de la acción.")
                else:
                    try:
                        registrar_accion_calidad(datos_accion)
                        st.success("Acción registrada correctamente.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"No se pudo registrar la acción: {e}")

        acciones = listar_acciones_calidad()
        st.subheader("Acciones registradas")

        if acciones:
            df_acciones = pd.DataFrame(
                acciones,
                columns=[
                    "ID acción", "ID NC", "Código NC", "Título", "Tipo",
                    "Responsable", "Estado", "Fecha inicio", "Fecha compromiso", "Fecha cierre",
                ],
            )
            st.dataframe(df_acciones, use_container_width=True, hide_index=True)
        else:
            st.info("Todavía no hay acciones registradas.")

        if acciones:
            tarjeta_seccion(
                "Estado",
                "Actualizar acción",
                "Las acciones pueden pasar a En proceso o Cerrada y registrar verificación de eficacia.",
            )

            opciones_accion = {
                f"{codigo_nc} | {titulo_accion} | {estado}": id_accion
                for id_accion, _, codigo_nc, titulo_accion, _, _, estado, *_ in acciones
            }

            with st.form("form_estado_accion"):
                seleccion_accion = st.selectbox("Selecciona la acción", list(opciones_accion.keys()))
                nuevo_estado_accion = st.selectbox("Nuevo estado de la acción", ["Abierta", "En proceso", "Cerrada"])
                verificacion_eficacia = st.text_area("Verificación de eficacia", key="verificacion_eficacia")
                guardar_estado_accion = st.form_submit_button("Actualizar acción")

            if guardar_estado_accion:
                try:
                    actualizar_estado_accion_calidad(
                        id_accion=opciones_accion[seleccion_accion],
                        nuevo_estado=nuevo_estado_accion,
                        verificacion_eficacia=verificacion_eficacia.strip() or None,
                    )
                    st.success("Acción actualizada correctamente.")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo actualizar la acción: {e}")

            tarjeta_seccion(
                "Evidencia",
                "Adjuntar archivo a acción",
                "Útil para guardar planes, formatos, fotos o pruebas de implementación.",
            )

            with st.form("form_evidencia_accion"):
                evidencia_accion = st.selectbox("Acción para evidencia", list(opciones_accion.keys()), key="evidencia_accion")
                descripcion_evidencia_accion = st.text_input("Descripción de la evidencia", key="descripcion_evidencia_accion")
                archivo_accion = st.file_uploader(
                    "Archivo de evidencia de acción",
                    key="archivo_evidencia_accion",
                    type=None,
                )
                guardar_evidencia_accion = st.form_submit_button("Guardar evidencia de acción")

            if guardar_evidencia_accion:
                if archivo_accion is None:
                    st.warning("Selecciona un archivo para guardar la evidencia.")
                else:
                    try:
                        guardar_evidencia_calidad(
                            tipo_entidad="accion",
                            id_entidad=opciones_accion[evidencia_accion],
                            nombre_archivo_original=archivo_accion.name,
                            contenido_archivo=archivo_accion.getvalue(),
                            descripcion=descripcion_evidencia_accion.strip(),
                            subido_por=st.session_state.get("usuario_email", ""),
                        )
                        st.success("Evidencia de acción guardada correctamente.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"No se pudo guardar la evidencia: {e}")

    with tab3:
        tarjeta_seccion(
            "Auditoría",
            "Programación de auditorías internas",
            "Deja programadas auditorías, criterios y responsable para fortalecer la preparación de ISO 9001.",
        )

        with st.form("form_auditoria", clear_on_submit=True):
            col1, col2 = st.columns(2)

            with col1:
                codigo_auditoria = st.text_input("Código de auditoría", placeholder="AUD-2026-001")
                titulo_auditoria = st.text_input("Título")
                area_auditoria = st.text_input("Área auditada")
                auditor_lider = st.text_input("Auditor líder")

            with col2:
                fecha_programada = st.date_input("Fecha programada", key="fecha_programada_auditoria")
                estado_auditoria = st.selectbox("Estado", ["Programada", "En ejecución", "Cerrada"])
                alcance_auditoria = st.text_area("Alcance")
                criterios_auditoria = st.text_area("Criterios")

            guardar_auditoria = st.form_submit_button("Guardar auditoría")

        if guardar_auditoria:
            datos_auditoria = {
                "codigo": codigo_auditoria.strip(),
                "titulo": titulo_auditoria.strip(),
                "area": area_auditoria.strip(),
                "auditor_lider": auditor_lider.strip(),
                "fecha_programada": str(fecha_programada),
                "alcance": alcance_auditoria.strip(),
                "criterios": criterios_auditoria.strip(),
                "estado": estado_auditoria,
            }

            campos_requeridos = ["codigo", "titulo", "area", "auditor_lider"]
            if any(not datos_auditoria[campo] for campo in campos_requeridos):
                st.warning("Completa los campos obligatorios de la auditoría.")
            else:
                try:
                    registrar_auditoria_calidad(datos_auditoria)
                    st.success("Auditoría registrada correctamente.")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo registrar la auditoría: {e}")

        auditorias = listar_auditorias_calidad()
        st.subheader("Auditorías internas")

        if auditorias:
            df_auditorias = pd.DataFrame(
                auditorias,
                columns=["ID", "Código", "Título", "Área", "Auditor líder", "Fecha programada", "Estado", "Resultado"],
            )
            st.dataframe(df_auditorias, use_container_width=True, hide_index=True)
        else:
            st.info("Todavía no hay auditorías registradas.")

        tarjeta_seccion(
            "Hallazgo",
            "Registrar hallazgo de auditoría",
            "Cada auditoría puede generar hallazgos con severidad, responsable y fecha compromiso.",
        )

        if auditorias:
            opciones_auditoria = {
                f"{codigo} | {titulo}": id_auditoria
                for id_auditoria, codigo, titulo, *_ in auditorias
            }

            with st.form("form_hallazgo_auditoria", clear_on_submit=True):
                auditoria_hallazgo = st.selectbox("Auditoría asociada", list(opciones_auditoria.keys()))
                referencia_hallazgo = st.text_input("Referencia", placeholder="ISO 9001 - 8.7")
                descripcion_hallazgo = st.text_area("Descripción del hallazgo")
                col1, col2 = st.columns(2)
                with col1:
                    severidad_hallazgo = st.selectbox("Severidad del hallazgo", ["Menor", "Mayor", "Crítica"])
                    responsable_hallazgo = st.text_input("Responsable")
                with col2:
                    estado_hallazgo = st.selectbox("Estado del hallazgo", ["Abierto", "En proceso", "Cerrado"])
                    fecha_compromiso_hallazgo = st.date_input("Fecha compromiso", key="fecha_compromiso_hallazgo")
                guardar_hallazgo = st.form_submit_button("Guardar hallazgo")

            if guardar_hallazgo:
                datos_hallazgo = {
                    "id_auditoria": opciones_auditoria[auditoria_hallazgo],
                    "referencia": referencia_hallazgo.strip(),
                    "descripcion": descripcion_hallazgo.strip(),
                    "severidad": severidad_hallazgo,
                    "estado": estado_hallazgo,
                    "responsable": responsable_hallazgo.strip(),
                    "fecha_compromiso": str(fecha_compromiso_hallazgo),
                }

                if not datos_hallazgo["referencia"] or not datos_hallazgo["descripcion"]:
                    st.warning("Completa la referencia y la descripción del hallazgo.")
                else:
                    try:
                        registrar_hallazgo_auditoria(datos_hallazgo)
                        st.success("Hallazgo registrado correctamente.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"No se pudo registrar el hallazgo: {e}")

        hallazgos = listar_hallazgos_auditoria()
        if hallazgos:
            st.write("### Hallazgos de auditoría")
            df_hallazgos = pd.DataFrame(
                hallazgos,
                columns=["ID hallazgo", "ID auditoría", "Código auditoría", "Referencia", "Descripción", "Severidad", "Estado", "Responsable", "Fecha compromiso"],
            )
            st.dataframe(df_hallazgos, use_container_width=True, hide_index=True)

    with tab4:
        tarjeta_seccion(
            "Control",
            "Vista de seguimiento",
            "Resumen rápido para revisar carga operativa, prioridades y trazabilidad del sistema de calidad.",
        )

        no_conformidades = listar_no_conformidades()
        acciones = listar_acciones_calidad()

        col1, col2 = st.columns(2)

        with col1:
            if no_conformidades:
                df_nc = pd.DataFrame(
                    no_conformidades,
                    columns=[
                        "ID", "Código", "Título", "Origen", "Área", "Severidad",
                        "Estado", "Detectado por", "Responsable", "Fecha detección",
                        "Fecha compromiso", "Causa raíz", "Fecha cierre",
                    ],
                )
                st.write("### Distribución por severidad")
                st.bar_chart(df_nc["Severidad"].value_counts())
            else:
                st.info("Sin datos de no conformidades para graficar.")

        with col2:
            if acciones:
                df_acciones = pd.DataFrame(
                    acciones,
                    columns=[
                        "ID acción", "ID NC", "Código NC", "Título", "Tipo",
                        "Responsable", "Estado", "Fecha inicio", "Fecha compromiso", "Fecha cierre",
                    ],
                )
                st.write("### Distribución por estado de acciones")
                st.bar_chart(df_acciones["Estado"].value_counts())
            else:
                st.info("Sin datos de acciones para graficar.")

        evidencia_total = []
        for id_nc, *_ in no_conformidades:
            evidencia_total.extend(listar_evidencias_calidad("no_conformidad", id_nc))
        for id_accion, *_ in acciones:
            evidencia_total.extend(listar_evidencias_calidad("accion", id_accion))

        if evidencia_total:
            st.write("### Descarga de evidencias")
            for id_evidencia, nombre_archivo, descripcion, subido_por, ruta_archivo, created_at in evidencia_total:
                try:
                    with open(ruta_archivo, "rb") as archivo:
                        st.download_button(
                            label=f"Descargar {nombre_archivo}",
                            data=archivo.read(),
                            file_name=nombre_archivo,
                            key=f"descarga_evidencia_{id_evidencia}",
                        )
                    if descripcion:
                        st.caption(f"{descripcion} | {subido_por} | {created_at}")
                except FileNotFoundError:
                    st.warning(f"No se encontró el archivo {nombre_archivo} en la ruta almacenada.")


st.sidebar.title("Navegación")
seccion = st.sidebar.radio(
    "Selecciona un módulo",
    ["C23-25", "B37-25", "Calidad"],
)

if seccion == "C23-25":
    mostrar_biobanco()
elif seccion == "B37-25":
    mostrar_proyecto_hospitalario()
elif seccion == "Calidad":
    mostrar_calidad()

st.sidebar.write(f"Sesión: {st.session_state.get('usuario_email', '')}")
if st.session_state.get("es_admin", False):
    with st.sidebar:
        st.subheader("Aprobación de usuarios")

        pendientes = obtener_usuarios_pendientes()

        if pendientes:
            for id_usuario, email, fecha_registro in pendientes:
                st.write(f"{email} - registrado el {fecha_registro}")
                if st.button("Aprobar", key=f"aprobar_{id_usuario}", use_container_width=True):
                    aprobar_usuario(id_usuario)
                    st.success(f"Usuario {email} aprobado.")
                    st.rerun()
        else:
            st.info("No hay usuarios pendientes.")

if st.sidebar.button("Cerrar sesión"):
    st.session_state["autenticado"] = False
    st.session_state["usuario_email"] = ""
    st.session_state["es_admin"] = False
    st.rerun()

