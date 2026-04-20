from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import mimetypes
import uuid

try:
    from supabase import Client, create_client
    SUPABASE_SDK_AVAILABLE = True
except ImportError:  # pragma: no cover
    Client = object  # type: ignore[assignment]
    create_client = None
    SUPABASE_SDK_AVAILABLE = False


@dataclass
class SupabaseQualityConfig:
    url: str
    key: str
    enabled: bool = False
    evidencias_bucket: str = "calidad-evidencias"
    documentos_bucket: str = "calidad-documentos"


_CONFIG = SupabaseQualityConfig(url="", key="", enabled=False)
_CLIENT: Client | None = None


def configure_supabase_quality(
    *,
    url: str | None,
    key: str | None,
    enabled: bool = False,
    evidencias_bucket: str = "calidad-evidencias",
    documentos_bucket: str = "calidad-documentos",
):
    global _CONFIG, _CLIENT
    _CONFIG = SupabaseQualityConfig(
        url=(url or "").strip(),
        key=(key or "").strip(),
        enabled=bool(enabled and url and key),
        evidencias_bucket=evidencias_bucket,
        documentos_bucket=documentos_bucket,
    )
    _CLIENT = None


def supabase_quality_enabled() -> bool:
    return bool(SUPABASE_SDK_AVAILABLE and _CONFIG.enabled and _CONFIG.url and _CONFIG.key)


def get_quality_backend_label() -> str:
    return "Supabase" if supabase_quality_enabled() else "SQLite local"


def _client() -> Client:
    global _CLIENT
    if not supabase_quality_enabled():
        raise RuntimeError("Supabase de calidad no está configurado.")
    if _CLIENT is None:
        _CLIENT = create_client(_CONFIG.url, _CONFIG.key)
    return _CLIENT


def _now_date() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _safe(data):
    return data if data is not None else []


def _insert(table: str, payload: dict) -> list[dict]:
    response = _client().table(table).insert(payload).execute()
    return _safe(response.data)


def _update(table: str, payload: dict, **filters) -> list[dict]:
    query = _client().table(table).update(payload)
    for key, value in filters.items():
        query = query.eq(key, value)
    response = query.execute()
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


def _log_event(entidad_tipo: str, entidad_id: int | None, accion: str, detalle: str, usuario_email: str):
    _insert(
        "calidad_bitacora",
        {
            "entidad_tipo": entidad_tipo,
            "entidad_id": entidad_id,
            "accion": accion,
            "detalle": detalle,
            "usuario_email": usuario_email,
        },
    )


def registrar_no_conformidad(datos: dict):
    inserted = _insert(
        "calidad_no_conformidades",
        {
            "codigo": datos["codigo"],
            "titulo": datos["titulo"],
            "descripcion": datos["descripcion"],
            "origen": datos["origen"],
            "area": datos["area"],
            "severidad": datos["severidad"],
            "estado": datos.get("estado", "Abierta"),
            "detectado_por": datos["detectado_por"],
            "responsable": datos["responsable"],
            "fecha_deteccion": datos["fecha_deteccion"],
            "fecha_compromiso": datos.get("fecha_compromiso"),
            "causa_raiz": datos.get("causa_raiz"),
        },
    )
    if inserted and datos.get("usuario_email"):
        _log_event(
            "no_conformidad",
            inserted[0]["id_no_conformidad"],
            "Creación",
            f"Se registró la no conformidad {datos['codigo']} con estado inicial {datos.get('estado', 'Abierta')}.",
            datos["usuario_email"],
        )


def listar_no_conformidades() -> list[tuple]:
    rows = _select("calidad_no_conformidades", order_by="created_at", desc=True)
    return [
        (
            row["id_no_conformidad"],
            row["codigo"],
            row["titulo"],
            row["descripcion"],
            row["origen"],
            row["area"],
            row["severidad"],
            row["estado"],
            row["detectado_por"],
            row["responsable"],
            row["fecha_deteccion"],
            row.get("fecha_compromiso"),
            row.get("causa_raiz"),
            row.get("fecha_cierre"),
            row.get("aprobado_por"),
            row.get("fecha_aprobacion"),
            row.get("comentario_final"),
        )
        for row in rows
    ]


def actualizar_no_conformidad(datos: dict):
    _update(
        "calidad_no_conformidades",
        {
            "titulo": datos["titulo"],
            "descripcion": datos["descripcion"],
            "origen": datos["origen"],
            "area": datos["area"],
            "severidad": datos["severidad"],
            "detectado_por": datos["detectado_por"],
            "responsable": datos["responsable"],
            "fecha_deteccion": datos["fecha_deteccion"],
            "fecha_compromiso": datos["fecha_compromiso"],
            "causa_raiz": datos.get("causa_raiz"),
        },
        id_no_conformidad=datos["id_no_conformidad"],
    )
    if datos.get("usuario_email"):
        _log_event(
            "no_conformidad",
            datos["id_no_conformidad"],
            "Edición",
            f"Se actualizaron los datos base de la no conformidad {datos['codigo']}.",
            datos["usuario_email"],
        )


def actualizar_estado_no_conformidad(id_no_conformidad: int, nuevo_estado: str, causa_raiz: str | None = None, verificacion_cierre: str | None = None, usuario_email: str | None = None):
    payload = {"estado": nuevo_estado}
    if causa_raiz:
        payload["causa_raiz"] = causa_raiz
    if verificacion_cierre:
        payload["verificacion_cierre"] = verificacion_cierre
    if nuevo_estado == "Cerrada":
        payload["fecha_cierre"] = _now_date()
    _update("calidad_no_conformidades", payload, id_no_conformidad=id_no_conformidad)
    if usuario_email:
        detalle = f"Se actualizó el estado de la no conformidad a {nuevo_estado}."
        if verificacion_cierre and nuevo_estado == "Cerrada":
            detalle += " Se registró verificación de cierre."
        _log_event("no_conformidad", id_no_conformidad, "Cambio de estado", detalle, usuario_email)


def aprobar_cierre_no_conformidad(id_no_conformidad: int, aprobado_por: str, comentario_final: str, verificacion_cierre: str | None = None):
    payload = {
        "estado": "Cerrada",
        "fecha_cierre": _now_date(),
        "aprobado_por": aprobado_por,
        "fecha_aprobacion": _now_date(),
        "comentario_final": comentario_final,
    }
    if verificacion_cierre:
        payload["verificacion_cierre"] = verificacion_cierre
    _update("calidad_no_conformidades", payload, id_no_conformidad=id_no_conformidad)
    _log_event("no_conformidad", id_no_conformidad, "Cierre formal", f"Cierre aprobado por {aprobado_por}. Comentario final registrado.", aprobado_por)


def registrar_accion_calidad(datos: dict):
    inserted = _insert(
        "calidad_acciones",
        {
            "id_no_conformidad": datos["id_no_conformidad"],
            "titulo": datos["titulo"],
            "descripcion": datos["descripcion"],
            "tipo_accion": datos["tipo_accion"],
            "responsable": datos["responsable"],
            "estado": datos.get("estado", "Abierta"),
            "fecha_inicio": datos["fecha_inicio"],
            "fecha_compromiso": datos.get("fecha_compromiso"),
        },
    )
    if inserted and datos.get("usuario_email"):
        _log_event("accion", inserted[0]["id_accion"], "Creación", f"Se registró la acción '{datos['titulo']}' con estado inicial {datos.get('estado', 'Abierta')}.", datos["usuario_email"])


def listar_acciones_calidad() -> list[tuple]:
    acciones = _select("calidad_acciones", order_by="created_at", desc=True)
    mapa_nc = {row["id_no_conformidad"]: row["codigo"] for row in _select("calidad_no_conformidades", "id_no_conformidad,codigo")}
    return [
        (
            row["id_accion"],
            row["id_no_conformidad"],
            mapa_nc.get(row["id_no_conformidad"], ""),
            row["titulo"],
            row["descripcion"],
            row["tipo_accion"],
            row["responsable"],
            row["estado"],
            row["fecha_inicio"],
            row.get("fecha_compromiso"),
            row.get("fecha_cierre"),
            row.get("aprobado_por"),
            row.get("fecha_aprobacion"),
            row.get("comentario_final"),
        )
        for row in acciones
    ]


def actualizar_accion_calidad(datos: dict):
    _update(
        "calidad_acciones",
        {
            "titulo": datos["titulo"],
            "descripcion": datos["descripcion"],
            "tipo_accion": datos["tipo_accion"],
            "responsable": datos["responsable"],
            "fecha_inicio": datos["fecha_inicio"],
            "fecha_compromiso": datos["fecha_compromiso"],
        },
        id_accion=datos["id_accion"],
    )
    if datos.get("usuario_email"):
        _log_event("accion", datos["id_accion"], "Edición", f"Se actualizaron los datos base de la acción '{datos['titulo']}'.", datos["usuario_email"])


def actualizar_estado_accion_calidad(id_accion: int, nuevo_estado: str, verificacion_eficacia: str | None = None, usuario_email: str | None = None):
    payload = {"estado": nuevo_estado}
    if verificacion_eficacia:
        payload["verificacion_eficacia"] = verificacion_eficacia
    if nuevo_estado == "Cerrada":
        payload["fecha_cierre"] = _now_date()
    _update("calidad_acciones", payload, id_accion=id_accion)
    if usuario_email:
        detalle = f"Se actualizó el estado de la acción a {nuevo_estado}."
        if verificacion_eficacia and nuevo_estado == "Cerrada":
            detalle += " Se registró verificación de eficacia."
        _log_event("accion", id_accion, "Cambio de estado", detalle, usuario_email)


def aprobar_cierre_accion(id_accion: int, aprobado_por: str, comentario_final: str, verificacion_eficacia: str | None = None):
    payload = {
        "estado": "Cerrada",
        "fecha_cierre": _now_date(),
        "aprobado_por": aprobado_por,
        "fecha_aprobacion": _now_date(),
        "comentario_final": comentario_final,
    }
    if verificacion_eficacia:
        payload["verificacion_eficacia"] = verificacion_eficacia
    _update("calidad_acciones", payload, id_accion=id_accion)
    _log_event("accion", id_accion, "Cierre formal", f"Cierre aprobado por {aprobado_por}. Comentario final registrado.", aprobado_por)


def contar_no_conformidades_abiertas() -> int:
    return len([row for row in _select("calidad_no_conformidades", "estado") if row["estado"] != "Cerrada"])


def contar_acciones_abiertas() -> int:
    return len([row for row in _select("calidad_acciones", "estado") if row["estado"] != "Cerrada"])


def contar_acciones_vencidas() -> int:
    hoy = datetime.now().date()
    total = 0
    for row in _select("calidad_acciones", "estado,fecha_compromiso"):
        fecha = row.get("fecha_compromiso")
        if row["estado"] != "Cerrada" and fecha:
            try:
                if datetime.strptime(fecha, "%Y-%m-%d").date() < hoy:
                    total += 1
            except ValueError:
                pass
    return total


def listar_bitacora_calidad(limit: int = 200) -> list[tuple]:
    rows = _select("calidad_bitacora", order_by="created_at", desc=True, limit=limit)
    return [
        (
            row["id_evento"],
            row["entidad_tipo"],
            row.get("entidad_id"),
            row["accion"],
            row["detalle"],
            row["usuario_email"],
            row["created_at"],
        )
        for row in rows
    ]


def _upload_bytes(bucket: str, folder: str, nombre_original: str, contenido: bytes) -> str:
    extension = Path(nombre_original).suffix
    storage_path = f"{folder}/{uuid.uuid4().hex}{extension}"
    content_type = mimetypes.guess_type(nombre_original)[0] or "application/octet-stream"
    _client().storage.from_(bucket).upload(storage_path, contenido, {"content-type": content_type, "upsert": "false"})
    return storage_path


def guardar_evidencia_calidad(tipo_entidad: str, id_entidad: int, nombre_archivo_original: str, contenido_archivo: bytes, descripcion: str, subido_por: str):
    storage_path = _upload_bytes(_CONFIG.evidencias_bucket, f"{tipo_entidad}/{id_entidad}", nombre_archivo_original, contenido_archivo)
    inserted = _insert(
        "calidad_evidencias",
        {
            "tipo_entidad": tipo_entidad,
            "id_entidad": id_entidad,
            "nombre_archivo": nombre_archivo_original,
            "ruta_archivo": storage_path,
            "descripcion": descripcion,
            "subido_por": subido_por,
        },
    )
    _log_event(tipo_entidad, id_entidad, "Carga de evidencia", f"Se adjuntó el archivo '{nombre_archivo_original}'.", subido_por)
    return inserted


def listar_evidencias_calidad(tipo_entidad: str, id_entidad: int) -> list[tuple]:
    rows = _select("calidad_evidencias", order_by="created_at", desc=True, tipo_entidad=tipo_entidad, id_entidad=id_entidad)
    return [
        (
            row["id_evidencia"],
            row["nombre_archivo"],
            row.get("descripcion"),
            row["subido_por"],
            row["ruta_archivo"],
            row["created_at"],
        )
        for row in rows
    ]


def descargar_archivo_storage(bucket: str, ruta_archivo: str) -> bytes:
    return _client().storage.from_(bucket).download(ruta_archivo)


def registrar_documento_calidad(datos: dict):
    inserted = _insert(
        "calidad_documentos",
        {
            "codigo": datos["codigo"],
            "nombre": datos["nombre"],
            "proceso_area": datos["proceso_area"],
            "tipo_documento": datos["tipo_documento"],
            "estado": datos.get("estado", "Borrador"),
            "version_actual": datos.get("version_actual"),
            "vigente_desde": datos.get("vigente_desde"),
            "vigente_hasta": datos.get("vigente_hasta"),
            "observaciones": datos.get("observaciones"),
        },
    )
    if inserted and datos.get("usuario_email"):
        _log_event("documento", inserted[0]["id_documento"], "Creación", f"Se registró el documento {datos['codigo']} en estado {datos.get('estado', 'Borrador')}.", datos["usuario_email"])


def listar_documentos_calidad() -> list[tuple]:
    rows = _select("calidad_documentos", order_by="created_at", desc=True)
    return [
        (
            row["id_documento"],
            row["codigo"],
            row["nombre"],
            row["proceso_area"],
            row["tipo_documento"],
            row["estado"],
            row.get("version_actual"),
            row.get("vigente_desde"),
            row.get("vigente_hasta"),
            row.get("aprobado_por"),
            row.get("fecha_aprobacion"),
            row.get("observaciones"),
        )
        for row in rows
    ]


def registrar_version_documento(id_documento: int, version: str, cambios_resumen: str, elaborado_por: str, nombre_archivo_original: str | None = None, contenido_archivo: bytes | None = None):
    storage_path = None
    if nombre_archivo_original and contenido_archivo is not None:
        storage_path = _upload_bytes(_CONFIG.documentos_bucket, f"documentos/{id_documento}/{version}", nombre_archivo_original, contenido_archivo)
    _insert(
        "calidad_documento_versiones",
        {
            "id_documento": id_documento,
            "version": version,
            "nombre_archivo": nombre_archivo_original,
            "ruta_archivo": storage_path,
            "cambios_resumen": cambios_resumen,
            "elaborado_por": elaborado_por,
            "es_vigente": False,
        },
    )
    _update("calidad_documentos", {"version_actual": version}, id_documento=id_documento)
    _log_event("documento", id_documento, "Nueva versión", f"Se registró la versión {version} del documento.", elaborado_por)


def listar_versiones_documento(id_documento: int) -> list[tuple]:
    rows = _select("calidad_documento_versiones", order_by="created_at", desc=True, id_documento=id_documento)
    return [
        (
            row["id_version"],
            row["version"],
            row.get("nombre_archivo"),
            row.get("ruta_archivo"),
            row.get("cambios_resumen"),
            row["elaborado_por"],
            row.get("aprobado_por"),
            row.get("fecha_aprobacion"),
            row.get("es_vigente"),
            row["created_at"],
        )
        for row in rows
    ]


def aprobar_version_documento(id_documento: int, id_version: int, aprobado_por: str, vigente_desde: str | None = None, vigente_hasta: str | None = None, estado_documento: str = "Vigente"):
    _client().table("calidad_documento_versiones").update({"es_vigente": False}).eq("id_documento", id_documento).execute()
    _update(
        "calidad_documento_versiones",
        {"aprobado_por": aprobado_por, "fecha_aprobacion": _now_date(), "es_vigente": True},
        id_version=id_version,
    )
    version = _select("calidad_documento_versiones", "version", id_version=id_version)[0]["version"]
    _update(
        "calidad_documentos",
        {
            "estado": estado_documento,
            "vigente_desde": vigente_desde,
            "vigente_hasta": vigente_hasta,
            "aprobado_por": aprobado_por,
            "fecha_aprobacion": _now_date(),
            "version_actual": version,
        },
        id_documento=id_documento,
    )
    _log_event("documento", id_documento, "Aprobación documental", f"Se aprobó formalmente una versión documental y se marcó como {estado_documento}.", aprobado_por)


def registrar_auditoria_calidad(datos: dict):
    inserted = _insert(
        "calidad_auditorias",
        {
            "codigo": datos["codigo"],
            "titulo": datos["titulo"],
            "area": datos["area"],
            "auditor_lider": datos["auditor_lider"],
            "fecha_programada": datos["fecha_programada"],
            "alcance": datos.get("alcance"),
            "criterios": datos.get("criterios"),
            "estado": datos.get("estado", "Programada"),
            "resultado": datos.get("resultado"),
        },
    )
    if inserted and datos.get("usuario_email"):
        _log_event("auditoria", inserted[0]["id_auditoria"], "Creación", f"Se programó la auditoría {datos['codigo']} con estado {datos.get('estado', 'Programada')}.", datos["usuario_email"])


def listar_auditorias_calidad() -> list[tuple]:
    rows = _select("calidad_auditorias", order_by="fecha_programada", desc=True)
    return [
        (
            row["id_auditoria"],
            row["codigo"],
            row["titulo"],
            row["area"],
            row["auditor_lider"],
            row["fecha_programada"],
            row["estado"],
            row.get("resultado"),
        )
        for row in rows
    ]


def registrar_hallazgo_auditoria(datos: dict):
    inserted = _insert(
        "calidad_auditoria_hallazgos",
        {
            "id_auditoria": datos["id_auditoria"],
            "referencia": datos["referencia"],
            "descripcion": datos["descripcion"],
            "severidad": datos["severidad"],
            "estado": datos.get("estado", "Abierto"),
            "responsable": datos.get("responsable"),
            "fecha_compromiso": datos.get("fecha_compromiso"),
        },
    )
    if inserted and datos.get("usuario_email"):
        _log_event("hallazgo_auditoria", inserted[0]["id_hallazgo"], "Creación", f"Se registró el hallazgo '{datos['referencia']}' con estado {datos.get('estado', 'Abierto')}.", datos["usuario_email"])


def listar_hallazgos_auditoria() -> list[tuple]:
    rows = _select("calidad_auditoria_hallazgos", order_by="created_at", desc=True)
    mapa_auditorias = {row["id_auditoria"]: row["codigo"] for row in _select("calidad_auditorias", "id_auditoria,codigo")}
    return [
        (
            row["id_hallazgo"],
            row["id_auditoria"],
            mapa_auditorias.get(row["id_auditoria"], ""),
            row["referencia"],
            row["descripcion"],
            row["severidad"],
            row["estado"],
            row.get("responsable"),
            row.get("fecha_compromiso"),
        )
        for row in rows
    ]
