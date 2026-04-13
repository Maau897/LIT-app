from datetime import datetime
import calendar

import bcrypt
import pandas as pd

from database import conectar_db


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

    conn.commit()
    conn.close()


def listar_no_conformidades():
    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            id_no_conformidad, codigo, titulo, origen, area, severidad,
            estado, detectado_por, responsable, fecha_deteccion,
            fecha_compromiso, causa_raiz, fecha_cierre
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
            a.tipo_accion,
            a.responsable,
            a.estado,
            a.fecha_inicio,
            a.fecha_compromiso,
            a.fecha_cierre
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
