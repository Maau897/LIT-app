from datetime import datetime
import calendar
from pathlib import Path
import uuid

import bcrypt
import pandas as pd

from database import EVIDENCIAS_DIR, conectar_db


def sumar_meses(fecha_str, meses):
    fecha = datetime.strptime(fecha_str, "%Y-%m-%d")
    anio = fecha.year + (fecha.month - 1 + meses) // 12
    mes = (fecha.month - 1 + meses) % 12 + 1
    dia = min(fecha.day, calendar.monthrange(anio, mes)[1])
    nueva_fecha = datetime(anio, mes, dia)
    return nueva_fecha.strftime("%Y-%m-%d")


def inicializar_rack_suero():
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
    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM voluntarios")
    total = cursor.fetchone()[0]

    conn.close()
    return total


def contar_visitas_pendientes():
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
    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM racks")
    total = cursor.fetchone()[0]

    conn.close()
    return total


def obtener_ocupacion_racks():
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


def registrar_usuario(email: str, password: str):
    conn = conectar_db()
    cursor = conn.cursor()

    password_hash = hash_password(password)

    cursor.execute("""
        INSERT INTO usuarios (email, password_hash, aprobado, es_admin)
        VALUES (?, ?, 0, 0)
    """, (email.strip().lower(), password_hash))

    conn.commit()
    conn.close()


def autenticar_usuario(email: str, password: str):
    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id_usuario, email, password_hash, aprobado, es_admin
        FROM usuarios
        WHERE email = ?
    """, (email.strip().lower(),))

    usuario = cursor.fetchone()
    conn.close()

    if not usuario:
        return {"ok": False, "mensaje": "Usuario no encontrado."}

    id_usuario, email_db, password_hash, aprobado, es_admin = usuario

    if not verificar_password(password, password_hash):
        return {"ok": False, "mensaje": "Contraseña incorrecta."}

    if aprobado != 1:
        return {"ok": False, "mensaje": "Tu cuenta aún no ha sido aprobada."}

    return {
        "ok": True,
        "id_usuario": id_usuario,
        "email": email_db,
        "es_admin": bool(es_admin)
    }


def obtener_usuarios_pendientes():
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


def aprobar_usuario(id_usuario: int):
    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE usuarios
        SET aprobado = 1
        WHERE id_usuario = ?
    """, (id_usuario,))

    conn.commit()
    conn.close()

def crear_admin_inicial(email: str, password: str):
    conn = conectar_db()
    cursor = conn.cursor()

    password_hash = hash_password(password)

    cursor.execute("""
        INSERT OR IGNORE INTO usuarios (email, password_hash, aprobado, es_admin)
        VALUES (?, ?, 1, 1)
    """, (email.strip().lower(), password_hash))

    conn.commit()
    conn.close()


def registrar_no_conformidad(datos):
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
    usuario_email: str | None = None,
):
    if nuevo_estado == "Cerrada" and not es_admin:
        raise PermissionError("Solo un administrador puede cerrar una no conformidad.")

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
    usuario_email: str | None = None,
):
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
):
    if not es_admin:
        raise PermissionError("Solo un administrador puede aprobar el cierre formal de una no conformidad.")

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
):
    if not es_admin:
        raise PermissionError("Solo un administrador puede aprobar el cierre formal de una acción.")

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


def registrar_auditoria_calidad(datos):
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
