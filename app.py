import streamlit as st
import pandas as pd

from google_sheets import (
    leer_sheet_como_dataframe,
    preparar_datos_hospitalarios,
    buscar_fila_por_clave,
    obtener_fila_como_diccionario,
    actualizar_campos_por_clave,
    normalizar_diagnostico,
    clasificar_edad,
    preparar_datos_hospitalarios,
    obtener_columnas_por_toma,
    construir_tabla_resumen_pacientes,
    preparar_resumen_influenza_observaciones
)
from database import crear_tablas
from logic import (
    registrar_voluntario,
    ver_racks,
    ver_visitas,
    ver_rack_suero,
    ver_rack_pbmc,
    exportar_a_excel,
    buscar_voluntario_por_id,
    ver_alicuotas_suero_voluntario,
    ver_alicuotas_pbmc_voluntario,
    contar_voluntarios,
    contar_visitas_pendientes,
    contar_racks_activos,
    obtener_ocupacion_racks
)


st.set_page_config(page_title="Sistema INER - Voluntarios", layout="wide")
crear_tablas()


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
            "observaciones": observaciones
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
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
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
                    "correo", "telefono", "patologias", "observaciones"
                ]

                df_voluntario = pd.DataFrame([voluntario], columns=columnas_voluntario)
                st.write("### Datos del voluntario")
                st.dataframe(df_voluntario, use_container_width=True)

                visitas = ver_visitas(id_busqueda)
                if visitas:
                    df_visitas = pd.DataFrame(
                        visitas,
                        columns=["Tipo toma", "Fecha programada", "Fecha real", "Estado"]
                    )
                    st.write("### Visitas")
                    st.dataframe(df_visitas, use_container_width=True)

                suero = ver_alicuotas_suero_voluntario(id_busqueda)
                if suero:
                    df_suero = pd.DataFrame(
                        suero,
                        columns=["Núm alícuota", "Fecha ingreso", "Rack", "Fila", "Columna", "Tipo toma"]
                    )
                    st.write("### Alícuotas de suero")
                    st.dataframe(df_suero, use_container_width=True)

                pbmc = ver_alicuotas_pbmc_voluntario(id_busqueda)
                if pbmc:
                    df_pbmc = pd.DataFrame(
                        pbmc,
                        columns=["Núm alícuota", "Conteo celular", "Fecha ingreso", "Rack", "Fila", "Columna", "Tipo toma"]
                    )
                    st.write("### Alícuotas PBMC")
                    st.dataframe(df_pbmc, use_container_width=True)

            else:
                st.warning("No se encontró un voluntario con ese ID.")

        except Exception as e:
            st.error(f"Error en la búsqueda: {e}")


def mostrar_proyecto_hospitalario():
    st.title("Proyecto hospitalario")
    st.info("Módulo independiente del biobanco. Aquí se muestra información leída desde Google Sheets.")

    try:
        df_sheet = leer_sheet_como_dataframe()
        df_sheet = preparar_datos_hospitalarios(df_sheet)
        df_sheet = preparar_resumen_influenza_observaciones(df_sheet)

        st.subheader("Búsqueda por clave de laboratorio")
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
        
                st.subheader("Edición dinámica de tomas")

        clave_edicion = st.text_input(
            "Clave de laboratorio para editar",
            placeholder="Ej. VSR_H_001",
            key="clave_edicion"
        )

        toma_seleccionada = st.selectbox(
            "Selecciona la toma",
            [f"T{i}" for i in range(1, 15)]
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
                            key=f"{col}_{numero_toma}"
                        )

                    cambios[col] = nuevo_valor

                if st.button("Guardar cambios"):
                    try:
                        actualizar_campos_por_clave(clave_actual, cambios)
                        st.success("Cambios guardados correctamente en Google Sheets")

                        # Recargar fila actualizada
                        numero_fila = buscar_fila_por_clave(clave_actual)
                        fila_actualizada = obtener_fila_como_diccionario(numero_fila)
                        st.session_state["fila_paciente"] = fila_actualizada

                    except Exception as e:
                        st.error(f"Error al guardar: {e}")
            else:
                st.warning(f"No se encontraron columnas para {toma_seleccionada}")
                
        st.subheader("Resumen por diagnóstico")

        if "DIAGNOSTICO_GRUPO" in df_sheet.columns:
            conteo_diag = df_sheet["DIAGNOSTICO_GRUPO"].value_counts().reset_index()
            conteo_diag.columns = ["Diagnóstico agrupado", "Número de pacientes"]
            st.dataframe(conteo_diag, use_container_width=True)

        st.subheader("Resumen por grupo de edad")

        if "GRUPO_EDAD" in df_sheet.columns:
            conteo_edad = df_sheet["GRUPO_EDAD"].value_counts().reset_index()
            conteo_edad.columns = ["Grupo de edad", "Número de pacientes"]
            st.dataframe(conteo_edad, use_container_width=True)

        st.subheader("Subclasificación edad y diagnóstico")

        if "GRUPO_EDAD" in df_sheet.columns and "DIAGNOSTICO_GRUPO" in df_sheet.columns:
            tabla_cruzada = pd.crosstab(
                df_sheet["GRUPO_EDAD"],
                df_sheet["DIAGNOSTICO_GRUPO"]
            )

            # Orden fijo de edades
            orden_edades = ["Bebé", "Niño", "Adolescente", "Adulto"]
            tabla_cruzada = tabla_cruzada.reindex(
                [edad for edad in orden_edades if edad in tabla_cruzada.index]
            )

            # Orden sugerido de diagnósticos
            columnas_preferidas = [
                "COVID",
                "Coinfección COVID",
                "INFLUENZA",
                "Coinfección INFLUENZA",
                "VSR",
                "Coinfección VSR"
            ]

            # Agregar también cualquier diagnóstico nuevo que exista
            columnas_existentes = list(tabla_cruzada.columns)
            columnas_extra = [col for col in columnas_existentes if col not in columnas_preferidas]
            columnas_finales = [col for col in columnas_preferidas if col in columnas_existentes] + columnas_extra

            tabla_cruzada = tabla_cruzada[columnas_finales]

            st.dataframe(tabla_cruzada, use_container_width=True)

        st.subheader("Resumen de influenza y coinfecciones en observaciones")

        if "INFLUENZA_OBS_GRUPO" in df_sheet.columns:
            df_influenza = df_sheet[df_sheet["INFLUENZA_OBS_GRUPO"].notna()]

            if not df_influenza.empty:
                conteo_influenza = df_influenza["INFLUENZA_OBS_GRUPO"].value_counts().reset_index()
                conteo_influenza.columns = ["Influenza / coinfección detectada", "Número de pacientes"]
                st.dataframe(conteo_influenza, use_container_width=True)
            else:
                st.info("No se encontraron registros de influenza en observaciones.")
        st.subheader("Datos completos del Google Sheet")
        st.dataframe(df_sheet, use_container_width=True)

        st.subheader("Resumen general de pacientes")
        df_resumen_pacientes = construir_tabla_resumen_pacientes(df_sheet)
        st.dataframe(df_resumen_pacientes, use_container_width=True)

        

    except Exception as e:
        st.error(f"Error al leer Google Sheets: {e}")
        
st.sidebar.title("Navegación")
seccion = st.sidebar.radio(
    "Selecciona un módulo",
    ["C23-25", "B37-25"]
)

if seccion == "C23-25":
    mostrar_biobanco()
elif seccion == "B37-25":
    mostrar_proyecto_hospitalario()