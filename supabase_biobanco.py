from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import calendar
from pathlib import Path

import pandas as pd

try:
    from supabase import Client, create_client
    SUPABASE_SDK_AVAILABLE = True
except ImportError:  # pragma: no cover
    Client = object  # type: ignore[assignment]
    create_client = None
    SUPABASE_SDK_AVAILABLE = False


@dataclass
class SupabaseBiobancoConfig:
    url: str
    key: str
    enabled: bool = False


_CONFIG = SupabaseBiobancoConfig(url="", key="", enabled=False)
_CLIENT: Client | None = None


def configure_supabase_biobanco(*, url: str | None, key: str | None, enabled: bool = False):
    global _CONFIG, _CLIENT
    _CONFIG = SupabaseBiobancoConfig(
        url=(url or "").strip(),
        key=(key or "").strip(),
        enabled=bool(enabled and url and key),
    )
    _CLIENT = None


def supabase_biobanco_enabled() -> bool:
    return bool(SUPABASE_SDK_AVAILABLE and _CONFIG.enabled and _CONFIG.url and _CONFIG.key)


def get_biobanco_backend_label() -> str:
    return "Supabase" if supabase_biobanco_enabled() else "SQLite local"


def _client() -> Client:
    global _CLIENT
    if not supabase_biobanco_enabled():
        raise RuntimeError("Supabase de biobanco no está configurado.")
    if _CLIENT is None:
        _CLIENT = create_client(_CONFIG.url, _CONFIG.key)
    return _CLIENT


def _safe(data):
    return data if data is not None else []


def _insert(table: str, payload: dict | list[dict]) -> list[dict]:
    response = _client().table(table).insert(payload).execute()
    return _safe(response.data)


def _select(table: str, columns: str = "*", order_by: str | None = None, desc: bool = False, limit: int | None = None, **filters) -> list[dict]:
    query = _client().table(table).select(columns)
    for key, value in filters.items():
        query = query.eq(key, value)
    if order_by:
        query = query.order(order_by, desc=desc)
    if limit is not None:
        query = query.limit(limit)
    response = query.execute()
    return _safe(response.data)


def _update(table: str, payload: dict, **filters) -> list[dict]:
    query = _client().table(table).update(payload)
    for key, value in filters.items():
        query = query.eq(key, value)
    response = query.execute()
    return _safe(response.data)


def sumar_meses(fecha_str: str, meses: int) -> str:
    fecha = datetime.strptime(fecha_str, "%Y-%m-%d")
    anio = fecha.year + (fecha.month - 1 + meses) // 12
    mes = (fecha.month - 1 + meses) % 12 + 1
    dia = min(fecha.day, calendar.monthrange(anio, mes)[1])
    return datetime(anio, mes, dia).strftime("%Y-%m-%d")


def inicializar_rack_suero():
    existente = _select("racks", id_rack="SUERO_1")
    if not existente:
        _insert(
            "racks",
            {"id_rack": "SUERO_1", "tipo_banco": "suero", "capacidad": 81, "ocupadas": 0},
        )


def _obtener_o_crear_rack_disponible(tipo_banco: str = "suero") -> str:
    racks = _select("racks", order_by="id_rack", tipo_banco=tipo_banco)
    for rack in racks:
        if int(rack["ocupadas"]) < int(rack["capacidad"]):
            return rack["id_rack"]

    numero_nuevo = len(racks) + 1
    id_rack_nuevo = f"{tipo_banco.upper()}_{numero_nuevo}"
    _insert(
        "racks",
        {"id_rack": id_rack_nuevo, "tipo_banco": tipo_banco, "capacidad": 81, "ocupadas": 0},
    )
    return id_rack_nuevo


def _obtener_siguiente_posicion_rack(id_rack: str) -> tuple[int, int]:
    rows = _select("racks", id_rack=id_rack)
    if not rows:
        raise ValueError("El rack no existe.")
    rack = rows[0]
    ocupadas = int(rack["ocupadas"])
    capacidad = int(rack["capacidad"])
    if ocupadas >= capacidad:
        raise ValueError("El rack está lleno.")
    posicion = ocupadas
    return posicion // 9 + 1, posicion % 9 + 1


def _aumentar_ocupacion_rack(id_rack: str, cantidad: int = 1):
    rows = _select("racks", id_rack=id_rack)
    if not rows:
        raise ValueError("El rack no existe.")
    ocupadas = int(rows[0]["ocupadas"])
    _update("racks", {"ocupadas": ocupadas + cantidad}, id_rack=id_rack)


def _asignar_alicuotas_suero(id_voluntario: str, tipo_toma: str, fecha_ingreso: str, cantidad: int = 6):
    inserts = []
    racks_actualizar: list[str] = []
    for numero_alicuota in range(1, cantidad + 1):
        id_rack = _obtener_o_crear_rack_disponible("suero")
        fila, columna = _obtener_siguiente_posicion_rack(id_rack)
        inserts.append(
            {
                "id_voluntario": id_voluntario,
                "tipo_toma": tipo_toma,
                "numero_alicuota": numero_alicuota,
                "fecha_ingreso": fecha_ingreso,
                "id_rack": id_rack,
                "fila": fila,
                "columna": columna,
            }
        )
        racks_actualizar.append(id_rack)
        _aumentar_ocupacion_rack(id_rack, 1)
    _insert("alicuotas_suero", inserts)


def _asignar_alicuotas_pbmc(id_voluntario: str, tipo_toma: str, fecha_ingreso: str, cantidad: int, conteo_celular: str):
    inserts = []
    for numero_alicuota in range(1, cantidad + 1):
        id_rack = _obtener_o_crear_rack_disponible("pbmc")
        fila, columna = _obtener_siguiente_posicion_rack(id_rack)
        inserts.append(
            {
                "id_voluntario": id_voluntario,
                "tipo_toma": tipo_toma,
                "numero_alicuota": numero_alicuota,
                "conteo_celular": conteo_celular,
                "fecha_ingreso": fecha_ingreso,
                "id_rack": id_rack,
                "fila": fila,
                "columna": columna,
            }
        )
        _aumentar_ocupacion_rack(id_rack, 1)
    _insert("alicuotas_pbmc", inserts)


def registrar_voluntario(datos: dict):
    _insert(
        "voluntarios",
        {
            "id_voluntario": datos["id_voluntario"],
            "expediente": datos["expediente"],
            "fecha_toma1": datos["fecha_toma1"],
            "apellido_paterno": datos["apellido_paterno"],
            "apellido_materno": datos["apellido_materno"],
            "nombre": datos["nombre"],
            "genero": datos["genero"],
            "residencia": datos["residencia"],
            "fecha_nacimiento": datos["fecha_nacimiento"],
            "edad": int(datos["edad"]),
            "peso": float(datos["peso"]),
            "estatura": float(datos["estatura"]),
            "tubos_amarillos": int(datos["tubos_amarillos"]),
            "tubos_verdes": int(datos["tubos_verdes"]),
            "correo": datos["correo"],
            "telefono": datos["telefono"],
            "patologias": datos["patologias"],
            "observaciones": datos["observaciones"],
        },
    )

    fechas = {
        "T1": datos["fecha_toma1"],
        "T2": sumar_meses(datos["fecha_toma1"], 2),
        "T3": sumar_meses(datos["fecha_toma1"], 4),
        "T4": sumar_meses(datos["fecha_toma1"], 6),
    }
    visitas = []
    for tipo_toma, fecha in fechas.items():
        visitas.append(
            {
                "id_voluntario": datos["id_voluntario"],
                "tipo_toma": tipo_toma,
                "fecha_programada": fecha,
                "fecha_real": fecha if tipo_toma == "T1" else None,
                "estado": "Realizada" if tipo_toma == "T1" else "Pendiente",
            }
        )
    _insert("visitas", visitas)

    _asignar_alicuotas_suero(datos["id_voluntario"], "T1", datos["fecha_toma1"], 6)

    cantidad_pbmc = int(datos.get("cantidad_pbmc", 0) or 0)
    conteo_celular = datos.get("conteo_celular", "")
    if cantidad_pbmc > 0:
        _asignar_alicuotas_pbmc(datos["id_voluntario"], "T1", datos["fecha_toma1"], cantidad_pbmc, conteo_celular)


def ver_visitas(id_voluntario: str) -> list[tuple]:
    rows = _select("visitas", order_by="tipo_toma", id_voluntario=id_voluntario)
    return [(row["tipo_toma"], row["fecha_programada"], row.get("fecha_real"), row["estado"]) for row in rows]


def ver_alicuotas_suero(id_voluntario: str) -> list[tuple]:
    rows = _select("alicuotas_suero", order_by="numero_alicuota", id_voluntario=id_voluntario)
    return [(row["numero_alicuota"], row["fecha_ingreso"], row["id_rack"], row["fila"], row["columna"]) for row in rows]


def ver_rack_suero(id_rack: str = "SUERO_1") -> list[list[str]]:
    registros = _select("alicuotas_suero", id_rack=id_rack)
    rack = [["LIBRE" for _ in range(9)] for _ in range(9)]
    for row in registros:
        rack[int(row["fila"]) - 1][int(row["columna"]) - 1] = f"{row['id_voluntario']}-A{row['numero_alicuota']}"
    return rack


def ver_racks() -> list[tuple]:
    rows = _select("racks", order_by="id_rack")
    return [(row["id_rack"], row["tipo_banco"], row["capacidad"], row["ocupadas"]) for row in rows]


def ver_rack_pbmc(id_rack: str) -> list[list[str]]:
    registros = _select("alicuotas_pbmc", id_rack=id_rack)
    rack = [["LIBRE" for _ in range(9)] for _ in range(9)]
    for row in registros:
        rack[int(row["fila"]) - 1][int(row["columna"]) - 1] = f"{row['id_voluntario']}-P{row['numero_alicuota']}"
    return rack


def _tabla_df(table: str) -> pd.DataFrame:
    return pd.DataFrame(_select(table, order_by="id_voluntario" if table == "voluntarios" else None))


def exportar_a_excel(nombre_archivo: str = "reporte_biobanco.xlsx") -> str:
    ruta = Path(nombre_archivo)
    with pd.ExcelWriter(ruta, engine="openpyxl") as writer:
        _tabla_df("voluntarios").to_excel(writer, sheet_name="Voluntarios", index=False)
        _tabla_df("visitas").to_excel(writer, sheet_name="Visitas", index=False)
        _tabla_df("alicuotas_suero").to_excel(writer, sheet_name="Suero", index=False)
        _tabla_df("alicuotas_pbmc").to_excel(writer, sheet_name="PBMC", index=False)
        _tabla_df("racks").to_excel(writer, sheet_name="Racks", index=False)
    return str(ruta)


def buscar_voluntario_por_id(id_voluntario: str):
    rows = _select("voluntarios", id_voluntario=id_voluntario)
    if not rows:
        return None
    row = rows[0]
    return (
        row["id_voluntario"],
        row.get("expediente"),
        row.get("fecha_toma1"),
        row.get("apellido_paterno"),
        row.get("apellido_materno"),
        row.get("nombre"),
        row.get("genero"),
        row.get("residencia"),
        row.get("fecha_nacimiento"),
        row.get("edad"),
        row.get("peso"),
        row.get("estatura"),
        row.get("tubos_amarillos"),
        row.get("tubos_verdes"),
        row.get("correo"),
        row.get("telefono"),
        row.get("patologias"),
        row.get("observaciones"),
    )


def ver_alicuotas_suero_voluntario(id_voluntario: str) -> list[tuple]:
    rows = _select("alicuotas_suero", order_by="numero_alicuota", id_voluntario=id_voluntario)
    return [
        (row["numero_alicuota"], row["fecha_ingreso"], row["id_rack"], row["fila"], row["columna"], row["tipo_toma"])
        for row in rows
    ]


def ver_alicuotas_pbmc_voluntario(id_voluntario: str) -> list[tuple]:
    rows = _select("alicuotas_pbmc", order_by="numero_alicuota", id_voluntario=id_voluntario)
    return [
        (
            row["numero_alicuota"],
            row.get("conteo_celular"),
            row["fecha_ingreso"],
            row["id_rack"],
            row["fila"],
            row["columna"],
            row["tipo_toma"],
        )
        for row in rows
    ]


def contar_voluntarios() -> int:
    return len(_select("voluntarios", "id_voluntario"))


def contar_visitas_pendientes() -> int:
    return len(_select("visitas", "id_visita", estado="Pendiente"))


def contar_racks_activos() -> int:
    return len(_select("racks", "id_rack"))


def obtener_ocupacion_racks() -> list[tuple]:
    return ver_racks()
