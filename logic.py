from datetime import datetime
import calendar
from pathlib import Path
import uuid

import bcrypt
import pandas as pd

from database import DOCUMENTOS_DIR, EVIDENCIAS_DIR, conectar_db
from supabase_quality import (
    aprobar_cierre_accion as sb_aprobar_cierre_accion,
    aprobar_cierre_no_conformidad as sb_aprobar_cierre_no_conformidad,
    aprobar_version_documento as sb_aprobar_version_documento,
    configure_supabase_quality,
    contar_acciones_abiertas as sb_contar_acciones_abiertas,
    contar_acciones_vencidas as sb_contar_acciones_vencidas,
    contar_no_conformidades_abiertas as sb_contar_no_conformidades_abiertas,
    descargar_archivo_storage,
    get_quality_backend_label,
    guardar_evidencia_calidad as sb_guardar_evidencia_calidad,
    listar_acciones_calidad as sb_listar_acciones_calidad,
    listar_auditorias_calidad as sb_listar_auditorias_calidad,
    listar_bitacora_calidad as sb_listar_bitacora_calidad,
    listar_documentos_calidad as sb_listar_documentos_calidad,
    listar_evidencias_calidad as sb_listar_evidencias_calidad,
    listar_hallazgos_auditoria as sb_listar_hallazgos_auditoria,
    listar_no_conformidades as sb_listar_no_conformidades,
    listar_versiones_documento as sb_listar_versiones_documento,
    registrar_accion_calidad as sb_registrar_accion_calidad,
    registrar_auditoria_calidad as sb_registrar_auditoria_calidad,
    registrar_documento_calidad as sb_registrar_documento_calidad,
    registrar_hallazgo_auditoria as sb_registrar_hallazgo_auditoria,
    registrar_no_conformidad as sb_registrar_no_conformidad,
    registrar_version_documento as sb_registrar_version_documento,
    supabase_quality_enabled,
    actualizar_accion_calidad as sb_actualizar_accion_calidad,
    actualizar_estado_accion_calidad as sb_actualizar_estado_accion_calidad,
    actualizar_estado_no_conformidad as sb_actualizar_estado_no_conformidad,
    actualizar_no_conformidad as sb_actualizar_no_conformidad,
)
from supabase_users import (
    aprobar_usuario as sb_aprobar_usuario,
    autenticar_usuario as sb_autenticar_usuario,
    configure_supabase_users,
    crear_admin_inicial as sb_crear_admin_inicial,
    get_users_backend_label,
    listar_usuarios as sb_listar_usuarios,
    obtener_usuarios_pendientes as sb_obtener_usuarios_pendientes,
    registrar_usuario as sb_registrar_usuario,
    supabase_users_enabled,
    actualizar_rol_usuario as sb_actualizar_rol_usuario,
)
from supabase_biobanco import (
    configure_supabase_biobanco,
    contar_racks_activos as sb_contar_racks_activos,
    contar_visitas_pendientes as sb_contar_visitas_pendientes,
    contar_voluntarios as sb_contar_voluntarios,
    exportar_a_excel as sb_exportar_a_excel,
    get_biobanco_backend_label,
    inicializar_rack_suero as sb_inicializar_rack_suero,
    obtener_ocupacion_racks as sb_obtener_ocupacion_racks,
    registrar_voluntario as sb_registrar_voluntario,
    buscar_voluntario_por_id as sb_buscar_voluntario_por_id,
    supabase_biobanco_enabled,
    ver_alicuotas_pbmc_voluntario as sb_ver_alicuotas_pbmc_voluntario,
    ver_alicuotas_suero_voluntario as sb_ver_alicuotas_suero_voluntario,
    ver_rack_pbmc as sb_ver_rack_pbmc,
    ver_rack_suero as sb_ver_rack_suero,
    ver_racks as sb_ver_racks,
    ver_visitas as sb_ver_visitas,
)


def sumar_meses(fecha_str, meses):
    fecha = datetime.strptime(fecha_str, "%Y-%m-%d")
    anio = fecha.year + (fecha.month - 1 + meses) // 12
    mes = (fecha.month - 1 + meses) % 12 + 1
    dia = min(fecha.day, calendar.monthrange(anio, mes)[1])
    nueva_fecha = datetime(anio, mes, dia)
    return nueva_fecha.strftime("%Y-%m-%d")


def inicializar_rack_suero():
    if biobanco_usa_supabase():
        return sb_inicializar_rack_suero()

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM racks WHERE id_rack = ?", ("SUERO_1",))
    rack = cursor.fetchone()

    if rack is None:
        cursor.execute("""
            INSERT INTO racks (id_rack, tipo_banco, capacidad, ocupadas)
            VALUES (?, ?, ?, ?)
        """, ("SUERO_1", "suero", 81, 0))
        conn.commit()

    conn.close()


def _obtener_o_crear_rack_disponible_cursor(cursor, tipo_banco="suero"):
    cursor.execute("""
        SELECT id_rack, ocupadas, capacidad
        FROM racks
        WHERE tipo_banco = ?
        ORDER BY id_rack
    """, (tipo_banco,))

    racks = cursor.fetchall()

    for id_rack, ocupadas, capacidad in racks:
        if ocupadas < capacidad:
            return id_rack

    numero_nuevo = len(racks) + 1
    id_rack_nuevo = f"{tipo_banco.upper()}_{numero_nuevo}"

    cursor.execute("""
        INSERT INTO racks (id_rack, tipo_banco, capacidad, ocupadas)
        VALUES (?, ?, ?, ?)
    """, (id_rack_nuevo, tipo_banco, 81, 0))

    return id_rack_nuevo


def obtener_o_crear_rack_disponible(tipo_banco="suero"):
    conn = conectar_db()
    cursor = conn.cursor()
    id_rack = _obtener_o_crear_rack_disponible_cursor(cursor, tipo_banco)
    conn.commit()
    conn.close()
    return id_rack


def _obtener_siguiente_posicion_rack_cursor(cursor, id_rack):
    cursor.execute("""
        SELECT ocupadas, capacidad
        FROM racks
        WHERE id_rack = ?
    """, (id_rack,))
    rack = cursor.fetchone()

    if rack is None:
        raise ValueError("El rack no existe.")

    ocupadas, capacidad = rack

    if ocupadas >= capacidad:
        raise ValueError("El rack está lleno.")

    posicion = ocupadas
    fila = posicion // 9 + 1
    columna = posicion % 9 + 1
    return fila, columna


def obtener_siguiente_posicion_rack(id_rack):
    conn = conectar_db()
    cursor = conn.cursor()
    fila, columna = _obtener_siguiente_posicion_rack_cursor(cursor, id_rack)
    conn.close()
    return fila, columna


def aumentar_ocupacion_rack(id_rack, cantidad=1):
    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE racks
        SET ocupadas = ocupadas + ?
        WHERE id_rack = ?
    """, (cantidad, id_rack))

    conn.commit()
    conn.close()


def _aumentar_ocupacion_rack_cursor(cursor, id_rack, cantidad=1):
    cursor.execute("""
        UPDATE racks
        SET ocupadas = ocupadas + ?
        WHERE id_rack = ?
    """, (cantidad, id_rack))


def _asignar_alicuotas_suero_cursor(cursor, id_voluntario, tipo_toma, fecha_ingreso, cantidad=6):
    for numero_alicuota in range(1, cantidad + 1):
        id_rack = _obtener_o_crear_rack_disponible_cursor(cursor, "suero")
        fila, columna = _obtener_siguiente_posicion_rack_cursor(cursor, id_rack)

        cursor.execute("""
            INSERT INTO alicuotas_suero (
                id_voluntario, tipo_toma, numero_alicuota,
                fecha_ingreso, id_rack, fila, columna
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            id_voluntario,
            tipo_toma,
            numero_alicuota,
            fecha_ingreso,
            id_rack,
            fila,
            columna
        ))

        _aumentar_ocupacion_rack_cursor(cursor, id_rack, 1)


def _asignar_alicuotas_pbmc_cursor(cursor, id_voluntario, tipo_toma, fecha_ingreso, cantidad, conteo_celular):
    for numero_alicuota in range(1, cantidad + 1):
        id_rack = _obtener_o_crear_rack_disponible_cursor(cursor, "pbmc")
        fila, columna = _obtener_siguiente_posicion_rack_cursor(cursor, id_rack)

        cursor.execute("""
            INSERT INTO alicuotas_pbmc (
                id_voluntario, tipo_toma, numero_alicuota,
                conteo_celular, fecha_ingreso, id_rack, fila, columna
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            id_voluntario,
            tipo_toma,
            numero_alicuota,
            conteo_celular,
            fecha_ingreso,
            id_rack,
            fila,
            columna
        ))

        _aumentar_ocupacion_rack_cursor(cursor, id_rack, 1)


def registrar_voluntario(datos):
    if biobanco_usa_supabase():
        return sb_registrar_voluntario(datos)

    conn = conectar_db()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO voluntarios (
                id_voluntario, expediente, fecha_toma1,
                apellido_paterno, apellido_materno, nombre,
                genero, residencia, fecha_nacimiento, edad,
                peso, estatura, tubos_amarillos, tubos_verdes,
                correo, telefono, patologias, observaciones
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datos["id_voluntario"],
            datos["expediente"],
            datos["fecha_toma1"],
            datos["apellido_paterno"],
            datos["apellido_materno"],
            datos["nombre"],
            datos["genero"],
            datos["residencia"],
            datos["fecha_nacimiento"],
            datos["edad"],
            datos["peso"],
            datos["estatura"],
            datos["tubos_amarillos"],
            datos["tubos_verdes"],
            datos["correo"],
            datos["telefono"],
            datos["patologias"],
            datos["observaciones"]
        ))

        fechas = {
            "T1": datos["fecha_toma1"],
            "T2": sumar_meses(datos["fecha_toma1"], 2),
            "T3": sumar_meses(datos["fecha_toma1"], 4),
            "T4": sumar_meses(datos["fecha_toma1"], 6)
        }

        for tipo_toma, fecha in fechas.items():
            estado = "Realizada" if tipo_toma == "T1" else "Pendiente"
            fecha_real = fecha if tipo_toma == "T1" else None

            cursor.execute("""
                INSERT INTO visitas (
                    id_voluntario, tipo_toma, fecha_programada, fecha_real, estado
                )
                VALUES (?, ?, ?, ?, ?)
            """, (
                datos["id_voluntario"],
                tipo_toma,
                fecha,
                fecha_real,
                estado
            ))

        _asignar_alicuotas_suero_cursor(
            cursor=cursor,
            id_voluntario=datos["id_voluntario"],
            tipo_toma="T1",
            fecha_ingreso=datos["fecha_toma1"],
            cantidad=6
        )

        cantidad_pbmc = datos.get("cantidad_pbmc", 0)
        conteo_celular = datos.get("conteo_celular", "")

        if cantidad_pbmc > 0:
            _asignar_alicuotas_pbmc_cursor(
                cursor=cursor,
                id_voluntario=datos["id_voluntario"],
                tipo_toma="T1",
                fecha_ingreso=datos["fecha_toma1"],
                cantidad=cantidad_pbmc,
                conteo_celular=conteo_celular
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def asignar_alicuotas_suero(id_voluntario, tipo_toma, fecha_ingreso, cantidad=6):
    conn = conectar_db()
    cursor = conn.cursor()
    try:
        _asignar_alicuotas_suero_cursor(cursor, id_voluntario, tipo_toma, fecha_ingreso, cantidad)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ver_visitas(id_voluntario):
    if biobanco_usa_supabase():
        return sb_ver_visitas(id_voluntario)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT tipo_toma, fecha_programada, fecha_real, estado
        FROM visitas
        WHERE id_voluntario = ?
        ORDER BY tipo_toma
    """, (id_voluntario,))

    resultados = cursor.fetchall()
    conn.close()

    return resultados


def ver_alicuotas_suero(id_voluntario):
    if biobanco_usa_supabase():
        filas = sb_ver_alicuotas_suero_voluntario(id_voluntario)
        return [(numero, fecha, rack, fila, columna) for numero, fecha, rack, fila, columna, _ in filas]

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT numero_alicuota, fecha_ingreso, id_rack, fila, columna
        FROM alicuotas_suero
        WHERE id_voluntario = ?
        ORDER BY numero_alicuota
    """, (id_voluntario,))

    resultados = cursor.fetchall()
    conn.close()

    return resultados


def ver_rack_suero(id_rack="SUERO_1"):
    if biobanco_usa_supabase():
        return sb_ver_rack_suero(id_rack)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id_voluntario, numero_alicuota, fila, columna
        FROM alicuotas_suero
        WHERE id_rack = ?
    """, (id_rack,))

    registros = cursor.fetchall()
    conn.close()

    rack = []
    for _ in range(9):
        fila = []
        for _ in range(9):
            fila.append("LIBRE")
        rack.append(fila)

    for id_voluntario, numero_alicuota, fila, columna in registros:
        texto = f"{id_voluntario}-A{numero_alicuota}"
        rack[fila - 1][columna - 1] = texto

    return rack


def imprimir_rack(rack):
    print("\nRACK 9x9:\n")

    for i, fila in enumerate(rack, start=1):
        fila_texto = []
        for celda in fila:
            texto = celda[:10]
            fila_texto.append(f"{texto:^10}")
        print(f"Fila {i}: " + " | ".join(fila_texto))


def ver_racks():
    if biobanco_usa_supabase():
        return sb_ver_racks()

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id_rack, tipo_banco, capacidad, ocupadas
        FROM racks
        ORDER BY id_rack
    """)

    resultados = cursor.fetchall()
    conn.close()

    return resultados


def generar_voluntarios_prueba(cantidad):
    voluntarios = []

    for i in range(1, cantidad + 1):
        voluntario = {
            "id_voluntario": f"VOL{i:03}",
            "expediente": f"EXP{i:03}",
            "fecha_toma1": "2026-03-23",
            "apellido_paterno": f"ApellidoP{i}",
            "apellido_materno": f"ApellidoM{i}",
            "nombre": f"Nombre{i}",
            "genero": "F" if i % 2 == 0 else "M",
            "residencia": "CDMX",
            "fecha_nacimiento": "1998-01-01",
            "edad": 27,
            "peso": 60.0 + i,
            "estatura": 1.60,
            "tubos_amarillos": 2,
            "tubos_verdes": 1,
            "correo": f"vol{i}@example.com",
            "telefono": f"550000{i:04}",
            "patologias": "Ninguna",
            "observaciones": "Registro de prueba"
        }
        voluntarios.append(voluntario)

    return voluntarios


def asignar_alicuotas_pbmc(id_voluntario, tipo_toma, fecha_ingreso, cantidad, conteo_celular):
    conn = conectar_db()
    cursor = conn.cursor()
    try:
        _asignar_alicuotas_pbmc_cursor(
            cursor,
            id_voluntario,
            tipo_toma,
            fecha_ingreso,
            cantidad,
            conteo_celular
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ver_rack_pbmc(id_rack):
    if biobanco_usa_supabase():
        return sb_ver_rack_pbmc(id_rack)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id_voluntario, numero_alicuota, fila, columna
        FROM alicuotas_pbmc
        WHERE id_rack = ?
    """, (id_rack,))

    registros = cursor.fetchall()
    conn.close()

    rack = [["LIBRE" for _ in range(9)] for _ in range(9)]

    for id_voluntario, numero_alicuota, fila, columna in registros:
        rack[fila - 1][columna - 1] = f"{id_voluntario}-P{numero_alicuota}"

    return rack


def obtener_tabla(nombre_tabla):
    conn = conectar_db()
    query = f"SELECT * FROM {nombre_tabla}"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def exportar_a_excel(nombre_archivo="reporte_biobanco.xlsx"):
    if biobanco_usa_supabase():
        return sb_exportar_a_excel(nombre_archivo)

    df_voluntarios = obtener_tabla("voluntarios")
    df_visitas = obtener_tabla("visitas")
    df_suero = obtener_tabla("alicuotas_suero")
    df_pbmc = obtener_tabla("alicuotas_pbmc")
    df_racks = obtener_tabla("racks")

    with pd.ExcelWriter(nombre_archivo, engine="openpyxl") as writer:
        df_voluntarios.to_excel(writer, sheet_name="Voluntarios", index=False)
        df_visitas.to_excel(writer, sheet_name="Visitas", index=False)
        df_suero.to_excel(writer, sheet_name="Suero", index=False)
        df_pbmc.to_excel(writer, sheet_name="PBMC", index=False)
        df_racks.to_excel(writer, sheet_name="Racks", index=False)

    return nombre_archivo


def buscar_voluntario_por_id(id_voluntario):
    if biobanco_usa_supabase():
        return sb_buscar_voluntario_por_id(id_voluntario)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT *
        FROM voluntarios
        WHERE id_voluntario = ?
    """, (id_voluntario,))

    resultado = cursor.fetchone()
    conn.close()

    return resultado


def ver_alicuotas_suero_voluntario(id_voluntario):
    if biobanco_usa_supabase():
        return sb_ver_alicuotas_suero_voluntario(id_voluntario)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT numero_alicuota, fecha_ingreso, id_rack, fila, columna, tipo_toma
        FROM alicuotas_suero
        WHERE id_voluntario = ?
        ORDER BY numero_alicuota
    """, (id_voluntario,))

    resultados = cursor.fetchall()
    conn.close()

    return resultados


def ver_alicuotas_pbmc_voluntario(id_voluntario):
    if biobanco_usa_supabase():
        return sb_ver_alicuotas_pbmc_voluntario(id_voluntario)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT numero_alicuota, conteo_celular, fecha_ingreso, id_rack, fila, columna, tipo_toma
        FROM alicuotas_pbmc
        WHERE id_voluntario = ?
        ORDER BY numero_alicuota
    """, (id_voluntario,))

    resultados = cursor.fetchall()
    conn.close()

    return resultados


def contar_voluntarios():
    if biobanco_usa_supabase():
        return sb_contar_voluntarios()

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM voluntarios")
    total = cursor.fetchone()[0]

    conn.close()
    return total


def contar_visitas_pendientes():
    if biobanco_usa_supabase():
        return sb_contar_visitas_pendientes()

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*)
        FROM visitas
        WHERE estado = 'Pendiente'
    """)
    total = cursor.fetchone()[0]

    conn.close()
    return total


def contar_racks_activos():
    if biobanco_usa_supabase():
        return sb_contar_racks_activos()

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM racks")
    total = cursor.fetchone()[0]

    conn.close()
    return total


def obtener_ocupacion_racks():
    if biobanco_usa_supabase():
        return sb_obtener_ocupacion_racks()

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id_rack, tipo_banco, capacidad, ocupadas
        FROM racks
        ORDER BY id_rack
    """)
    resultados = cursor.fetchall()

    conn.close()
    return resultados


def hash_password(password: str) -> str:
    password_bytes = password.encode("utf-8")
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password_bytes, salt)
    return hashed.decode("utf-8")


def verificar_password(password: str, password_hash: str) -> bool:
    return bcrypt.checkpw(
        password.encode("utf-8"),
        password_hash.encode("utf-8")
    )


ROLES_CALIDAD = ["captura", "responsable", "auditor", "calidad", "admin"]


def normalizar_rol(rol: str | None, es_admin: bool = False) -> str:
    if es_admin:
        return "admin"
    rol_normalizado = (rol or "captura").strip().lower()
    return rol_normalizado if rol_normalizado in ROLES_CALIDAD else "captura"


def usuario_tiene_rol(rol_usuario: str | None, *roles_permitidos: str, es_admin: bool = False) -> bool:
    rol_normalizado = normalizar_rol(rol_usuario, es_admin)
    permitidos = {normalizar_rol(rol) for rol in roles_permitidos}
    return rol_normalizado == "admin" or rol_normalizado in permitidos


def configurar_persistencia_calidad_supabase(
    *,
    url: str | None,
    key: str | None,
    enabled: bool = False,
    evidencias_bucket: str = "calidad-evidencias",
    documentos_bucket: str = "calidad-documentos",
):
    configure_supabase_quality(
        url=url,
        key=key,
        enabled=enabled,
        evidencias_bucket=evidencias_bucket,
        documentos_bucket=documentos_bucket,
    )


def calidad_usa_supabase() -> bool:
    return supabase_quality_enabled()


def obtener_backend_calidad() -> str:
    return get_quality_backend_label()


def configurar_persistencia_usuarios_supabase(
    *,
    url: str | None,
    key: str | None,
    enabled: bool = False,
    table_name: str = "usuarios_app",
):
    configure_supabase_users(
        url=url,
        key=key,
        enabled=enabled,
        table_name=table_name,
    )


def usuarios_usan_supabase() -> bool:
    return supabase_users_enabled()


def obtener_backend_usuarios() -> str:
    return get_users_backend_label()


def configurar_persistencia_biobanco_supabase(
    *,
    url: str | None,
    key: str | None,
    enabled: bool = False,
):
    configure_supabase_biobanco(
        url=url,
        key=key,
        enabled=enabled,
    )


def biobanco_usa_supabase() -> bool:
    return supabase_biobanco_enabled()


def obtener_backend_biobanco() -> str:
    return get_biobanco_backend_label()


def descargar_evidencia_calidad(ruta_archivo: str) -> bytes:
    if calidad_usa_supabase():
        return descargar_archivo_storage("calidad-evidencias", ruta_archivo)
    with open(ruta_archivo, "rb") as archivo:
        return archivo.read()


def descargar_documento_calidad(ruta_archivo: str) -> bytes:
    if calidad_usa_supabase():
        return descargar_archivo_storage("calidad-documentos", ruta_archivo)
    with open(ruta_archivo, "rb") as archivo:
        return archivo.read()


def registrar_usuario(email: str, password: str, rol: str = "captura"):
    if usuarios_usan_supabase():
        return sb_registrar_usuario(email, password, normalizar_rol(rol))

    conn = conectar_db()
    cursor = conn.cursor()

    password_hash = hash_password(password)
    rol_normalizado = normalizar_rol(rol)

    cursor.execute("""
        INSERT INTO usuarios (email, password_hash, aprobado, es_admin, rol)
        VALUES (?, ?, 0, 0, ?)
    """, (email.strip().lower(), password_hash, rol_normalizado))

    conn.commit()
    conn.close()


def autenticar_usuario(email: str, password: str):
    if usuarios_usan_supabase():
        return sb_autenticar_usuario(email, password, normalizar_rol)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id_usuario, email, password_hash, aprobado, es_admin, rol
        FROM usuarios
        WHERE email = ?
    """, (email.strip().lower(),))

    usuario = cursor.fetchone()
    conn.close()

    if not usuario:
        return {"ok": False, "mensaje": "Usuario no encontrado."}

    id_usuario, email_db, password_hash, aprobado, es_admin, rol = usuario

    if not verificar_password(password, password_hash):
        return {"ok": False, "mensaje": "Contraseña incorrecta."}

    if aprobado != 1:
        return {"ok": False, "mensaje": "Tu cuenta aún no ha sido aprobada."}

    return {
        "ok": True,
        "id_usuario": id_usuario,
        "email": email_db,
        "es_admin": bool(es_admin),
        "rol": normalizar_rol(rol, bool(es_admin)),
    }


def obtener_usuarios_pendientes():
    if usuarios_usan_supabase():
        return sb_obtener_usuarios_pendientes()

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id_usuario, email, fecha_registro
        FROM usuarios
        WHERE aprobado = 0
        ORDER BY fecha_registro
    """)

    resultados = cursor.fetchall()
    conn.close()
    return resultados


def listar_usuarios():
    if usuarios_usan_supabase():
        return sb_listar_usuarios()

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id_usuario, email, aprobado, es_admin, rol, fecha_registro
        FROM usuarios
        ORDER BY es_admin DESC, aprobado DESC, email
    """)

    resultados = cursor.fetchall()
    conn.close()
    return resultados


def aprobar_usuario(id_usuario: int, rol: str = "captura"):
    if usuarios_usan_supabase():
        return sb_aprobar_usuario(id_usuario, normalizar_rol(rol, rol == "admin"))

    conn = conectar_db()
    cursor = conn.cursor()
    rol_normalizado = normalizar_rol(rol)

    cursor.execute("""
        UPDATE usuarios
        SET aprobado = 1,
            rol = ?,
            es_admin = CASE WHEN ? = 'admin' THEN 1 ELSE es_admin END
        WHERE id_usuario = ?
    """, (rol_normalizado, rol_normalizado, id_usuario))

    conn.commit()
    conn.close()


def actualizar_rol_usuario(id_usuario: int, rol: str):
    if usuarios_usan_supabase():
        return sb_actualizar_rol_usuario(id_usuario, normalizar_rol(rol, rol == "admin"))

    conn = conectar_db()
    cursor = conn.cursor()
    rol_normalizado = normalizar_rol(rol, rol == "admin")

    cursor.execute("""
        UPDATE usuarios
        SET rol = ?,
            es_admin = CASE WHEN ? = 'admin' THEN 1 ELSE 0 END
        WHERE id_usuario = ?
    """, (rol_normalizado, rol_normalizado, id_usuario))

    conn.commit()
    conn.close()

def crear_admin_inicial(email: str, password: str):
    if usuarios_usan_supabase():
        return sb_crear_admin_inicial(email, password)

    conn = conectar_db()
    cursor = conn.cursor()

    password_hash = hash_password(password)

    cursor.execute("""
        INSERT OR IGNORE INTO usuarios (email, password_hash, aprobado, es_admin, rol)
        VALUES (?, ?, 1, 1, 'admin')
    """, (email.strip().lower(), password_hash))

    cursor.execute("""
        UPDATE usuarios
        SET es_admin = 1,
            rol = 'admin'
        WHERE email = ?
    """, (email.strip().lower(),))

    conn.commit()
    conn.close()


def registrar_no_conformidad(datos):
    if calidad_usa_supabase():
        return sb_registrar_no_conformidad(datos)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO calidad_no_conformidades (
            codigo, titulo, descripcion, origen, area, severidad,
            estado, detectado_por, responsable, fecha_deteccion,
            fecha_compromiso, causa_raiz
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datos["codigo"],
        datos["titulo"],
        datos["descripcion"],
        datos["origen"],
        datos["area"],
        datos["severidad"],
        datos.get("estado", "Abierta"),
        datos["detectado_por"],
        datos["responsable"],
        datos["fecha_deteccion"],
        datos.get("fecha_compromiso"),
        datos.get("causa_raiz"),
    ))

    id_no_conformidad = cursor.lastrowid

    if datos.get("usuario_email"):
        _registrar_evento_calidad_cursor(
            cursor,
            entidad_tipo="no_conformidad",
            entidad_id=id_no_conformidad,
            accion="Creación",
            detalle=f"Se registró la no conformidad {datos['codigo']} con estado inicial {datos.get('estado', 'Abierta')}.",
            usuario_email=datos["usuario_email"],
        )

    conn.commit()
    conn.close()


def registrar_accion_calidad(datos):
    if calidad_usa_supabase():
        return sb_registrar_accion_calidad(datos)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO calidad_acciones (
            id_no_conformidad, titulo, descripcion, tipo_accion,
            responsable, estado, fecha_inicio, fecha_compromiso
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datos["id_no_conformidad"],
        datos["titulo"],
        datos["descripcion"],
        datos["tipo_accion"],
        datos["responsable"],
        datos.get("estado", "Abierta"),
        datos["fecha_inicio"],
        datos.get("fecha_compromiso"),
    ))

    id_accion = cursor.lastrowid

    if datos.get("usuario_email"):
        _registrar_evento_calidad_cursor(
            cursor,
            entidad_tipo="accion",
            entidad_id=id_accion,
            accion="Creación",
            detalle=f"Se registró la acción '{datos['titulo']}' con estado inicial {datos.get('estado', 'Abierta')}.",
            usuario_email=datos["usuario_email"],
        )

    conn.commit()
    conn.close()


def actualizar_no_conformidad(datos):
    if calidad_usa_supabase():
        return sb_actualizar_no_conformidad(datos)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE calidad_no_conformidades
        SET titulo = ?,
            descripcion = ?,
            origen = ?,
            area = ?,
            severidad = ?,
            detectado_por = ?,
            responsable = ?,
            fecha_deteccion = ?,
            fecha_compromiso = ?,
            causa_raiz = ?
        WHERE id_no_conformidad = ?
    """, (
        datos["titulo"],
        datos["descripcion"],
        datos["origen"],
        datos["area"],
        datos["severidad"],
        datos["detectado_por"],
        datos["responsable"],
        datos["fecha_deteccion"],
        datos["fecha_compromiso"],
        datos.get("causa_raiz"),
        datos["id_no_conformidad"],
    ))

    if datos.get("usuario_email"):
        _registrar_evento_calidad_cursor(
            cursor,
            entidad_tipo="no_conformidad",
            entidad_id=datos["id_no_conformidad"],
            accion="Edición",
            detalle=f"Se actualizaron los datos base de la no conformidad {datos['codigo']}.",
            usuario_email=datos["usuario_email"],
        )

    conn.commit()
    conn.close()


def actualizar_accion_calidad(datos):
    if calidad_usa_supabase():
        return sb_actualizar_accion_calidad(datos)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE calidad_acciones
        SET titulo = ?,
            descripcion = ?,
            tipo_accion = ?,
            responsable = ?,
            fecha_inicio = ?,
            fecha_compromiso = ?
        WHERE id_accion = ?
    """, (
        datos["titulo"],
        datos["descripcion"],
        datos["tipo_accion"],
        datos["responsable"],
        datos["fecha_inicio"],
        datos["fecha_compromiso"],
        datos["id_accion"],
    ))

    if datos.get("usuario_email"):
        _registrar_evento_calidad_cursor(
            cursor,
            entidad_tipo="accion",
            entidad_id=datos["id_accion"],
            accion="Edición",
            detalle=f"Se actualizaron los datos base de la acción '{datos['titulo']}'.",
            usuario_email=datos["usuario_email"],
        )

    conn.commit()
    conn.close()


def listar_no_conformidades():
    if calidad_usa_supabase():
        return sb_listar_no_conformidades()

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            id_no_conformidad, codigo, titulo, descripcion, origen, area, severidad,
            estado, detectado_por, responsable, fecha_deteccion,
            fecha_compromiso, causa_raiz, fecha_cierre, aprobado_por, fecha_aprobacion, comentario_final
        FROM calidad_no_conformidades
        ORDER BY datetime(created_at) DESC, id_no_conformidad DESC
    """)

    resultados = cursor.fetchall()
    conn.close()
    return resultados


def listar_acciones_calidad():
    if calidad_usa_supabase():
        return sb_listar_acciones_calidad()

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            a.id_accion,
            a.id_no_conformidad,
            nc.codigo,
            a.titulo,
            a.descripcion,
            a.tipo_accion,
            a.responsable,
            a.estado,
            a.fecha_inicio,
            a.fecha_compromiso,
            a.fecha_cierre,
            a.aprobado_por,
            a.fecha_aprobacion,
            a.comentario_final
        FROM calidad_acciones a
        INNER JOIN calidad_no_conformidades nc
            ON nc.id_no_conformidad = a.id_no_conformidad
        ORDER BY datetime(a.created_at) DESC, a.id_accion DESC
    """)

    resultados = cursor.fetchall()
    conn.close()
    return resultados


def contar_no_conformidades_abiertas():
    if calidad_usa_supabase():
        return sb_contar_no_conformidades_abiertas()

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*)
        FROM calidad_no_conformidades
        WHERE estado != 'Cerrada'
    """)
    total = cursor.fetchone()[0]
    conn.close()
    return total


def contar_acciones_abiertas():
    if calidad_usa_supabase():
        return sb_contar_acciones_abiertas()

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*)
        FROM calidad_acciones
        WHERE estado != 'Cerrada'
    """)
    total = cursor.fetchone()[0]
    conn.close()
    return total


def contar_acciones_vencidas():
    if calidad_usa_supabase():
        return sb_contar_acciones_vencidas()

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*)
        FROM calidad_acciones
        WHERE fecha_compromiso IS NOT NULL
          AND date(fecha_compromiso) < date('now')
          AND estado != 'Cerrada'
    """)
    total = cursor.fetchone()[0]
    conn.close()
    return total


def _registrar_evento_calidad_cursor(
    cursor,
    *,
    entidad_tipo: str,
    entidad_id: int | None,
    accion: str,
    detalle: str,
    usuario_email: str,
):
    cursor.execute("""
        INSERT INTO calidad_bitacora (
            entidad_tipo, entidad_id, accion, detalle, usuario_email
        )
        VALUES (?, ?, ?, ?, ?)
    """, (
        entidad_tipo,
        entidad_id,
        accion,
        detalle,
        usuario_email,
    ))


def listar_bitacora_calidad(limit: int = 200):
    if calidad_usa_supabase():
        return sb_listar_bitacora_calidad(limit)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id_evento, entidad_tipo, entidad_id, accion, detalle, usuario_email, created_at
        FROM calidad_bitacora
        ORDER BY datetime(created_at) DESC, id_evento DESC
        LIMIT ?
    """, (limit,))

    resultados = cursor.fetchall()
    conn.close()
    return resultados


def actualizar_estado_no_conformidad(
    id_no_conformidad: int,
    nuevo_estado: str,
    causa_raiz: str | None = None,
    verificacion_cierre: str | None = None,
    es_admin: bool = False,
    rol_usuario: str | None = None,
    usuario_email: str | None = None,
):
    if nuevo_estado == "Cerrada" and not usuario_tiene_rol(rol_usuario, "calidad", es_admin=es_admin):
        raise PermissionError("Solo usuarios de calidad o administradores pueden cerrar una no conformidad.")

    if calidad_usa_supabase():
        return sb_actualizar_estado_no_conformidad(
            id_no_conformidad=id_no_conformidad,
            nuevo_estado=nuevo_estado,
            causa_raiz=causa_raiz,
            verificacion_cierre=verificacion_cierre,
            usuario_email=usuario_email,
        )

    conn = conectar_db()
    cursor = conn.cursor()
    fecha_cierre = datetime.now().strftime("%Y-%m-%d") if nuevo_estado == "Cerrada" else None

    cursor.execute("""
        UPDATE calidad_no_conformidades
        SET estado = ?,
            causa_raiz = COALESCE(?, causa_raiz),
            verificacion_cierre = CASE
                WHEN ? IS NOT NULL AND ? != '' THEN ?
                ELSE verificacion_cierre
            END,
            fecha_cierre = ?
        WHERE id_no_conformidad = ?
    """, (
        nuevo_estado,
        causa_raiz,
        verificacion_cierre,
        verificacion_cierre,
        verificacion_cierre,
        fecha_cierre,
        id_no_conformidad,
    ))

    if usuario_email:
        detalle = f"Se actualizó el estado de la no conformidad a {nuevo_estado}."
        if verificacion_cierre and nuevo_estado == "Cerrada":
            detalle += " Se registró verificación de cierre."
        _registrar_evento_calidad_cursor(
            cursor,
            entidad_tipo="no_conformidad",
            entidad_id=id_no_conformidad,
            accion="Cambio de estado",
            detalle=detalle,
            usuario_email=usuario_email,
        )

    conn.commit()
    conn.close()


def actualizar_estado_accion_calidad(
    id_accion: int,
    nuevo_estado: str,
    verificacion_eficacia: str | None = None,
    es_admin: bool = False,
    rol_usuario: str | None = None,
    usuario_email: str | None = None,
):
    if nuevo_estado == "Cerrada" and not usuario_tiene_rol(rol_usuario, "calidad", es_admin=es_admin):
        raise PermissionError("Solo usuarios de calidad o administradores pueden cerrar una acción.")

    if calidad_usa_supabase():
        return sb_actualizar_estado_accion_calidad(
            id_accion=id_accion,
            nuevo_estado=nuevo_estado,
            verificacion_eficacia=verificacion_eficacia,
            usuario_email=usuario_email,
        )

    conn = conectar_db()
    cursor = conn.cursor()
    fecha_cierre = datetime.now().strftime("%Y-%m-%d") if nuevo_estado == "Cerrada" else None

    cursor.execute("""
        UPDATE calidad_acciones
        SET estado = ?,
            verificacion_eficacia = CASE
                WHEN ? IS NOT NULL AND ? != '' THEN ?
                ELSE verificacion_eficacia
            END,
            fecha_cierre = ?
        WHERE id_accion = ?
    """, (
        nuevo_estado,
        verificacion_eficacia,
        verificacion_eficacia,
        verificacion_eficacia,
        fecha_cierre,
        id_accion,
    ))

    if usuario_email:
        detalle = f"Se actualizó el estado de la acción a {nuevo_estado}."
        if verificacion_eficacia and nuevo_estado == "Cerrada":
            detalle += " Se registró verificación de eficacia."
        _registrar_evento_calidad_cursor(
            cursor,
            entidad_tipo="accion",
            entidad_id=id_accion,
            accion="Cambio de estado",
            detalle=detalle,
            usuario_email=usuario_email,
        )

    conn.commit()
    conn.close()


def aprobar_cierre_no_conformidad(
    *,
    id_no_conformidad: int,
    aprobado_por: str,
    comentario_final: str,
    verificacion_cierre: str | None = None,
    es_admin: bool = False,
    rol_usuario: str | None = None,
):
    if not usuario_tiene_rol(rol_usuario, "calidad", es_admin=es_admin):
        raise PermissionError("Solo usuarios de calidad o administradores pueden aprobar el cierre formal de una no conformidad.")

    if calidad_usa_supabase():
        return sb_aprobar_cierre_no_conformidad(
            id_no_conformidad=id_no_conformidad,
            aprobado_por=aprobado_por,
            comentario_final=comentario_final,
            verificacion_cierre=verificacion_cierre,
        )

    conn = conectar_db()
    cursor = conn.cursor()
    fecha_actual = datetime.now().strftime("%Y-%m-%d")

    cursor.execute("""
        UPDATE calidad_no_conformidades
        SET estado = 'Cerrada',
            fecha_cierre = ?,
            aprobado_por = ?,
            fecha_aprobacion = ?,
            comentario_final = ?,
            verificacion_cierre = CASE
                WHEN ? IS NOT NULL AND ? != '' THEN ?
                ELSE verificacion_cierre
            END
        WHERE id_no_conformidad = ?
    """, (
        fecha_actual,
        aprobado_por,
        fecha_actual,
        comentario_final,
        verificacion_cierre,
        verificacion_cierre,
        verificacion_cierre,
        id_no_conformidad,
    ))

    _registrar_evento_calidad_cursor(
        cursor,
        entidad_tipo="no_conformidad",
        entidad_id=id_no_conformidad,
        accion="Cierre formal",
        detalle=f"Cierre aprobado por {aprobado_por}. Comentario final registrado.",
        usuario_email=aprobado_por,
    )

    conn.commit()
    conn.close()


def aprobar_cierre_accion(
    *,
    id_accion: int,
    aprobado_por: str,
    comentario_final: str,
    verificacion_eficacia: str | None = None,
    es_admin: bool = False,
    rol_usuario: str | None = None,
):
    if not usuario_tiene_rol(rol_usuario, "calidad", es_admin=es_admin):
        raise PermissionError("Solo usuarios de calidad o administradores pueden aprobar el cierre formal de una acción.")

    if calidad_usa_supabase():
        return sb_aprobar_cierre_accion(
            id_accion=id_accion,
            aprobado_por=aprobado_por,
            comentario_final=comentario_final,
            verificacion_eficacia=verificacion_eficacia,
        )

    conn = conectar_db()
    cursor = conn.cursor()
    fecha_actual = datetime.now().strftime("%Y-%m-%d")

    cursor.execute("""
        UPDATE calidad_acciones
        SET estado = 'Cerrada',
            fecha_cierre = ?,
            aprobado_por = ?,
            fecha_aprobacion = ?,
            comentario_final = ?,
            verificacion_eficacia = CASE
                WHEN ? IS NOT NULL AND ? != '' THEN ?
                ELSE verificacion_eficacia
            END
        WHERE id_accion = ?
    """, (
        fecha_actual,
        aprobado_por,
        fecha_actual,
        comentario_final,
        verificacion_eficacia,
        verificacion_eficacia,
        verificacion_eficacia,
        id_accion,
    ))

    _registrar_evento_calidad_cursor(
        cursor,
        entidad_tipo="accion",
        entidad_id=id_accion,
        accion="Cierre formal",
        detalle=f"Cierre aprobado por {aprobado_por}. Comentario final registrado.",
        usuario_email=aprobado_por,
    )

    conn.commit()
    conn.close()


def guardar_evidencia_calidad(
    *,
    tipo_entidad: str,
    id_entidad: int,
    nombre_archivo_original: str,
    contenido_archivo: bytes,
    descripcion: str,
    subido_por: str,
):
    if calidad_usa_supabase():
        return sb_guardar_evidencia_calidad(
            tipo_entidad=tipo_entidad,
            id_entidad=id_entidad,
            nombre_archivo_original=nombre_archivo_original,
            contenido_archivo=contenido_archivo,
            descripcion=descripcion,
            subido_por=subido_por,
        )

    extension = Path(nombre_archivo_original).suffix
    nombre_guardado = f"{tipo_entidad}_{id_entidad}_{uuid.uuid4().hex}{extension}"
    ruta_destino = EVIDENCIAS_DIR / nombre_guardado

    with open(ruta_destino, "wb") as archivo:
        archivo.write(contenido_archivo)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO calidad_evidencias (
            tipo_entidad, id_entidad, nombre_archivo, ruta_archivo, descripcion, subido_por
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        tipo_entidad,
        id_entidad,
        nombre_archivo_original,
        str(ruta_destino),
        descripcion,
        subido_por,
    ))

    _registrar_evento_calidad_cursor(
        cursor,
        entidad_tipo=tipo_entidad,
        entidad_id=id_entidad,
        accion="Carga de evidencia",
        detalle=f"Se adjuntó el archivo '{nombre_archivo_original}'.",
        usuario_email=subido_por,
    )

    conn.commit()
    conn.close()


def listar_evidencias_calidad(tipo_entidad: str, id_entidad: int):
    if calidad_usa_supabase():
        return sb_listar_evidencias_calidad(tipo_entidad, id_entidad)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id_evidencia, nombre_archivo, descripcion, subido_por, ruta_archivo, created_at
        FROM calidad_evidencias
        WHERE tipo_entidad = ? AND id_entidad = ?
        ORDER BY datetime(created_at) DESC, id_evidencia DESC
    """, (tipo_entidad, id_entidad))

    resultados = cursor.fetchall()
    conn.close()
    return resultados


def registrar_documento_calidad(datos):
    if calidad_usa_supabase():
        return sb_registrar_documento_calidad(datos)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO calidad_documentos (
            codigo, nombre, proceso_area, tipo_documento, estado,
            version_actual, vigente_desde, vigente_hasta, observaciones
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datos["codigo"],
        datos["nombre"],
        datos["proceso_area"],
        datos["tipo_documento"],
        datos.get("estado", "Borrador"),
        datos.get("version_actual"),
        datos.get("vigente_desde"),
        datos.get("vigente_hasta"),
        datos.get("observaciones"),
    ))

    id_documento = cursor.lastrowid

    if datos.get("usuario_email"):
        _registrar_evento_calidad_cursor(
            cursor,
            entidad_tipo="documento",
            entidad_id=id_documento,
            accion="Creación",
            detalle=f"Se registró el documento {datos['codigo']} en estado {datos.get('estado', 'Borrador')}.",
            usuario_email=datos["usuario_email"],
        )

    conn.commit()
    conn.close()


def listar_documentos_calidad():
    if calidad_usa_supabase():
        return sb_listar_documentos_calidad()

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            id_documento, codigo, nombre, proceso_area, tipo_documento,
            estado, version_actual, vigente_desde, vigente_hasta,
            aprobado_por, fecha_aprobacion, observaciones
        FROM calidad_documentos
        ORDER BY datetime(created_at) DESC, id_documento DESC
    """)

    resultados = cursor.fetchall()
    conn.close()
    return resultados


def registrar_version_documento(
    *,
    id_documento: int,
    version: str,
    cambios_resumen: str,
    elaborado_por: str,
    nombre_archivo_original: str | None = None,
    contenido_archivo: bytes | None = None,
):
    if calidad_usa_supabase():
        return sb_registrar_version_documento(
            id_documento=id_documento,
            version=version,
            cambios_resumen=cambios_resumen,
            elaborado_por=elaborado_por,
            nombre_archivo_original=nombre_archivo_original,
            contenido_archivo=contenido_archivo,
        )

    ruta_destino = None

    if nombre_archivo_original and contenido_archivo is not None:
        extension = Path(nombre_archivo_original).suffix
        nombre_guardado = f"documento_{id_documento}_{version}_{uuid.uuid4().hex}{extension}"
        ruta_destino = DOCUMENTOS_DIR / nombre_guardado
        with open(ruta_destino, "wb") as archivo:
            archivo.write(contenido_archivo)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO calidad_documento_versiones (
            id_documento, version, nombre_archivo, ruta_archivo,
            cambios_resumen, elaborado_por, es_vigente
        )
        VALUES (?, ?, ?, ?, ?, ?, 0)
    """, (
        id_documento,
        version,
        nombre_archivo_original,
        str(ruta_destino) if ruta_destino else None,
        cambios_resumen,
        elaborado_por,
    ))

    cursor.execute("""
        UPDATE calidad_documentos
        SET version_actual = COALESCE(?, version_actual)
        WHERE id_documento = ?
    """, (version, id_documento))

    _registrar_evento_calidad_cursor(
        cursor,
        entidad_tipo="documento",
        entidad_id=id_documento,
        accion="Nueva versión",
        detalle=f"Se registró la versión {version} del documento.",
        usuario_email=elaborado_por,
    )

    conn.commit()
    conn.close()


def listar_versiones_documento(id_documento: int):
    if calidad_usa_supabase():
        return sb_listar_versiones_documento(id_documento)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            id_version, version, nombre_archivo, ruta_archivo, cambios_resumen,
            elaborado_por, aprobado_por, fecha_aprobacion, es_vigente, created_at
        FROM calidad_documento_versiones
        WHERE id_documento = ?
        ORDER BY datetime(created_at) DESC, id_version DESC
    """, (id_documento,))

    resultados = cursor.fetchall()
    conn.close()
    return resultados


def aprobar_version_documento(
    *,
    id_documento: int,
    id_version: int,
    aprobado_por: str,
    vigente_desde: str | None = None,
    vigente_hasta: str | None = None,
    estado_documento: str = "Vigente",
    es_admin: bool = False,
    rol_usuario: str | None = None,
):
    if not usuario_tiene_rol(rol_usuario, "calidad", es_admin=es_admin):
        raise PermissionError("Solo usuarios de calidad o administradores pueden aprobar una versión documental.")

    if calidad_usa_supabase():
        return sb_aprobar_version_documento(
            id_documento=id_documento,
            id_version=id_version,
            aprobado_por=aprobado_por,
            vigente_desde=vigente_desde,
            vigente_hasta=vigente_hasta,
            estado_documento=estado_documento,
        )

    conn = conectar_db()
    cursor = conn.cursor()
    fecha_actual = datetime.now().strftime("%Y-%m-%d")

    cursor.execute("""
        UPDATE calidad_documento_versiones
        SET es_vigente = 0
        WHERE id_documento = ?
    """, (id_documento,))

    cursor.execute("""
        UPDATE calidad_documento_versiones
        SET aprobado_por = ?,
            fecha_aprobacion = ?,
            es_vigente = 1
        WHERE id_version = ?
    """, (
        aprobado_por,
        fecha_actual,
        id_version,
    ))

    cursor.execute("""
        UPDATE calidad_documentos
        SET estado = ?,
            vigente_desde = ?,
            vigente_hasta = ?,
            aprobado_por = ?,
            fecha_aprobacion = ?,
            version_actual = (
                SELECT version
                FROM calidad_documento_versiones
                WHERE id_version = ?
            )
        WHERE id_documento = ?
    """, (
        estado_documento,
        vigente_desde,
        vigente_hasta,
        aprobado_por,
        fecha_actual,
        id_version,
        id_documento,
    ))

    _registrar_evento_calidad_cursor(
        cursor,
        entidad_tipo="documento",
        entidad_id=id_documento,
        accion="Aprobación documental",
        detalle=f"Se aprobó formalmente una versión documental y se marcó como {estado_documento}.",
        usuario_email=aprobado_por,
    )

    conn.commit()
    conn.close()


def registrar_auditoria_calidad(datos):
    if calidad_usa_supabase():
        return sb_registrar_auditoria_calidad(datos)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO calidad_auditorias (
            codigo, titulo, area, auditor_lider, fecha_programada,
            alcance, criterios, estado, resultado
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datos["codigo"],
        datos["titulo"],
        datos["area"],
        datos["auditor_lider"],
        datos["fecha_programada"],
        datos.get("alcance"),
        datos.get("criterios"),
        datos.get("estado", "Programada"),
        datos.get("resultado"),
    ))

    id_auditoria = cursor.lastrowid

    if datos.get("usuario_email"):
        _registrar_evento_calidad_cursor(
            cursor,
            entidad_tipo="auditoria",
            entidad_id=id_auditoria,
            accion="Creación",
            detalle=f"Se programó la auditoría {datos['codigo']} con estado {datos.get('estado', 'Programada')}.",
            usuario_email=datos["usuario_email"],
        )

    conn.commit()
    conn.close()


def listar_auditorias_calidad():
    if calidad_usa_supabase():
        return sb_listar_auditorias_calidad()

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            id_auditoria, codigo, titulo, area, auditor_lider,
            fecha_programada, estado, resultado
        FROM calidad_auditorias
        ORDER BY date(fecha_programada) DESC, id_auditoria DESC
    """)

    resultados = cursor.fetchall()
    conn.close()
    return resultados


def registrar_hallazgo_auditoria(datos):
    if calidad_usa_supabase():
        return sb_registrar_hallazgo_auditoria(datos)

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO calidad_auditoria_hallazgos (
            id_auditoria, referencia, descripcion, severidad,
            estado, responsable, fecha_compromiso
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        datos["id_auditoria"],
        datos["referencia"],
        datos["descripcion"],
        datos["severidad"],
        datos.get("estado", "Abierto"),
        datos.get("responsable"),
        datos.get("fecha_compromiso"),
    ))

    id_hallazgo = cursor.lastrowid

    if datos.get("usuario_email"):
        _registrar_evento_calidad_cursor(
            cursor,
            entidad_tipo="hallazgo_auditoria",
            entidad_id=id_hallazgo,
            accion="Creación",
            detalle=f"Se registró el hallazgo '{datos['referencia']}' con estado {datos.get('estado', 'Abierto')}.",
            usuario_email=datos["usuario_email"],
        )

    conn.commit()
    conn.close()


def listar_hallazgos_auditoria():
    if calidad_usa_supabase():
        return sb_listar_hallazgos_auditoria()

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            h.id_hallazgo,
            h.id_auditoria,
            a.codigo,
            h.referencia,
            h.descripcion,
            h.severidad,
            h.estado,
            h.responsable,
            h.fecha_compromiso
        FROM calidad_auditoria_hallazgos h
        INNER JOIN calidad_auditorias a
            ON a.id_auditoria = h.id_auditoria
        ORDER BY datetime(h.created_at) DESC, h.id_hallazgo DESC
    """)

    resultados = cursor.fetchall()
    conn.close()
    return resultados
