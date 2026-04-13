import re
import unicodedata

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials


SPREADSHEET_ID = "15ph6nHs8SgxqUX3aKiRKjR-0BsG2Z-8mg6qy_NSueBg"


def obtener_spreadsheet():
    creds_dict = st.secrets["gcp_service_account"]

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)

    return gc.open_by_key(SPREADSHEET_ID)


def conectar_sheet(nombre_hoja=None):
    spreadsheet = obtener_spreadsheet()

    if nombre_hoja:
        return spreadsheet.worksheet(nombre_hoja)

    return spreadsheet.get_worksheet(0)


def leer_sheet_como_dataframe():
    ws = conectar_sheet()
    datos = ws.get_all_records()
    return pd.DataFrame(datos)


def obtener_encabezados():
    ws = conectar_sheet()
    encabezados = ws.row_values(1)
    return encabezados


def buscar_fila_por_clave(clave_laboratorio, nombre_columna="CLAVE DE LABORATORIO"):
    ws = conectar_sheet()
    datos = ws.get_all_values()

    if not datos:
        return None

    encabezados = datos[0]

    if nombre_columna not in encabezados:
        raise ValueError(f"No se encontró la columna '{nombre_columna}' en la hoja.")

    idx_col = encabezados.index(nombre_columna)

    for numero_fila, fila in enumerate(datos[1:], start=2):
        valor = fila[idx_col] if idx_col < len(fila) else ""
        if str(valor).strip().upper() == str(clave_laboratorio).strip().upper():
            return numero_fila

    return None


def obtener_fila_como_diccionario(numero_fila):
    ws = conectar_sheet()
    encabezados = ws.row_values(1)
    valores = ws.row_values(numero_fila)

    if len(valores) < len(encabezados):
        valores.extend([""] * (len(encabezados) - len(valores)))

    return dict(zip(encabezados, valores))


def actualizar_campos_por_clave(clave_laboratorio, cambios, nombre_columna="CLAVE DE LABORATORIO"):
    ws = conectar_sheet()
    encabezados = ws.row_values(1)

    numero_fila = buscar_fila_por_clave(clave_laboratorio, nombre_columna=nombre_columna)

    if numero_fila is None:
        raise ValueError("No se encontró la clave de laboratorio.")

    for columna, valor_nuevo in cambios.items():
        if columna not in encabezados:
            continue

        numero_columna = encabezados.index(columna) + 1
        ws.update_cell(numero_fila, numero_columna, valor_nuevo)

    return True


def normalizar_diagnostico(valor):
    if pd.isna(valor):
        return "Sin dato"

    texto = str(valor).strip().upper()

    tiene_vsr = "VSR" in texto
    tiene_covid = "COVID" in texto
    tiene_influenza = "INFLUENZA" in texto
    es_coinfeccion = "COI" in texto or "COINFE" in texto

    diagnosticos_detectados = []

    if tiene_covid:
        diagnosticos_detectados.append("COVID")
    if tiene_influenza:
        diagnosticos_detectados.append("INFLUENZA")
    if tiene_vsr:
        diagnosticos_detectados.append("VSR")

    if es_coinfeccion and len(diagnosticos_detectados) == 1:
        return "Coinfección " + diagnosticos_detectados[0]

    if len(diagnosticos_detectados) > 1:
        return "Coinfección " + "/".join(diagnosticos_detectados)
    if len(diagnosticos_detectados) == 1:
        return diagnosticos_detectados[0]
    return texto


def clasificar_edad(valor):
    if valor is None:
        return "Sin dato"

    texto = str(valor).strip().lower()

    if any(c.isalpha() for c in texto):
        return "Bebé"

    try:
        edad = float(texto)

        if edad < 12:
            return "Niño"
        if edad < 18:
            return "Adolescente"
        return "Adulto"

    except Exception:
        return "Sin clasificar"


def preparar_datos_hospitalarios(df):
    df = df.copy()

    if "DIAGNÓSTICO" in df.columns:
        df["DIAGNOSTICO_GRUPO"] = df["DIAGNÓSTICO"].apply(normalizar_diagnostico)

    if "EDAD" in df.columns:
        df["GRUPO_EDAD"] = df["EDAD"].apply(clasificar_edad)

    return df


def obtener_columnas_por_toma(toma):
    ws = conectar_sheet()
    encabezados = ws.row_values(1)

    prefijo = f"T{toma}"
    columnas_toma = [col for col in encabezados if col.startswith(prefijo)]

    return columnas_toma


def es_fecha_numerica_valida(valor):
    if pd.isna(valor):
        return False

    texto = str(valor).strip()

    if texto == "":
        return False

    texto_mayus = texto.upper()

    if texto_mayus in ["NA", "N/A", "SIN MUESTRA", "NO"]:
        return False

    if any(c.isalpha() for c in texto):
        return False

    if any(c.isdigit() for c in texto):
        return True

    return False


def obtener_tomas_disponibles(df):
    columnas = df.columns.tolist()
    tomas = set()

    for col in columnas:
        match = re.match(r"^T(\d+)", str(col).strip(), re.IGNORECASE)
        if match:
            tomas.add(int(match.group(1)))

    return sorted(tomas)


def _normalizar_nombre_columna(nombre):
    texto = unicodedata.normalize("NFKD", str(nombre))
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = re.sub(r"\s+", " ", texto.strip().upper())
    return texto


def _buscar_columna(df, nombre_objetivo):
    objetivo = _normalizar_nombre_columna(nombre_objetivo)

    for columna in df.columns:
        if _normalizar_nombre_columna(columna) == objetivo:
            return columna

    return None


def construir_tabla_resumen_pacientes(df):
    df = df.copy()
    tomas_disponibles = obtener_tomas_disponibles(df)
    columna_observaciones = _buscar_columna(df, "OBSERVACIONES")
    resumen = []

    for _, fila in df.iterrows():
        total_tomas = 0

        for t in tomas_disponibles:
            columnas_ingreso = [
                col for col in df.columns
                if str(col).strip().upper().startswith(f"T{t}")
                and "INGRESO" in str(col).strip().upper()
            ]

            tiene_toma = any(es_fecha_numerica_valida(fila[col]) for col in columnas_ingreso)

            if tiene_toma:
                total_tomas += 1

        resumen.append({
            "NOMBRE COMPLETO": fila.get("NOMBRE COMPLETO", ""),
            "DIAGNÓSTICO": fila.get("DIAGNÓSTICO", ""),
            "CLAVE DE LABORATORIO": fila.get("CLAVE DE LABORATORIO", ""),
            "OBSERVACIONES": fila.get(columna_observaciones, "") if columna_observaciones else "",
            "TOTAL TOMAS": total_tomas,
        })

    return pd.DataFrame(resumen)


def clasificar_influenza_observaciones(texto):
    if pd.isna(texto):
        return None

    texto = str(texto).strip().upper()

    if texto == "":
        return None

    tiene_ah1n1 = "AH1N1" in texto
    tiene_ah3n2 = "AH3N2" in texto
    tiene_influenza_generica = "INFLUENZA" in texto and not (tiene_ah1n1 or tiene_ah3n2)

    agentes = []

    if tiene_ah1n1:
        agentes.append("AH1N1")
    if tiene_ah3n2:
        agentes.append("AH3N2")
    if tiene_influenza_generica:
        agentes.append("INFLUENZA")

    otros_patrones = {
        "M PNEUMONIAE": "M PNEUMONIAE",
        "MYCOPLASMA": "M PNEUMONIAE",
        "RINOVIRUS/ENTEROVIRUS": "RINOVIRUS/ENTEROVIRUS",
        "RINOVIRUS": "RINOVIRUS/ENTEROVIRUS",
        "ENTEROVIRUS": "RINOVIRUS/ENTEROVIRUS",
        "VSR": "VSR",
        "COVID": "COVID",
        "SARS-COV-2": "COVID",
        "ADENOVIRUS": "ADENOVIRUS",
        "PARAINFLUENZA": "PARAINFLUENZA",
        "METAPNEUMOVIRUS": "METAPNEUMOVIRUS",
    }

    for patron, nombre in otros_patrones.items():
        if patron in texto and nombre not in agentes:
            agentes.append(nombre)

    if not any(x in agentes for x in ["AH1N1", "AH3N2", "INFLUENZA"]):
        return None

    if len(agentes) == 1:
        return agentes[0]

    return "Coinfección " + " + ".join(agentes)


def _obtener_columna_observaciones(df):
    return _buscar_columna(df, "OBSERVACIONES")


def preparar_resumen_influenza_observaciones(df):
    df = df.copy()
    columna_observaciones = _obtener_columna_observaciones(df)

    if columna_observaciones:
        df["INFLUENZA_OBS_GRUPO"] = df[columna_observaciones].apply(clasificar_influenza_observaciones)

    return df
