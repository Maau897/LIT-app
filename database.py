import sqlite3
from pathlib import Path

DB_NAME = Path(__file__).resolve().parent / "iner_voluntarios.db"
EVIDENCIAS_DIR = Path(__file__).resolve().parent / "evidencias_calidad"


def conectar_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def crear_tablas():
    EVIDENCIAS_DIR.mkdir(exist_ok=True)
    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS voluntarios (
        id_voluntario TEXT PRIMARY KEY,
        expediente TEXT,
        fecha_toma1 TEXT,
        apellido_paterno TEXT,
        apellido_materno TEXT,
        nombre TEXT,
        genero TEXT,
        residencia TEXT,
        fecha_nacimiento TEXT,
        edad INTEGER,
        peso REAL,
        estatura REAL,
        tubos_amarillos INTEGER,
        tubos_verdes INTEGER,
        correo TEXT,
        telefono TEXT,
        patologias TEXT,
        observaciones TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS visitas (
        id_visita INTEGER PRIMARY KEY AUTOINCREMENT,
        id_voluntario TEXT,
        tipo_toma TEXT,
        fecha_programada TEXT,
        fecha_real TEXT,
        estado TEXT,
        FOREIGN KEY (id_voluntario) REFERENCES voluntarios(id_voluntario)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS racks (
        id_rack TEXT PRIMARY KEY,
        tipo_banco TEXT,
        capacidad INTEGER,
        ocupadas INTEGER DEFAULT 0
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS alicuotas_suero (
        id_alicuota INTEGER PRIMARY KEY AUTOINCREMENT,
        id_voluntario TEXT,
        tipo_toma TEXT,
        numero_alicuota INTEGER,
        fecha_ingreso TEXT,
        id_rack TEXT,
        fila INTEGER,
        columna INTEGER,
        FOREIGN KEY (id_voluntario) REFERENCES voluntarios(id_voluntario),
        FOREIGN KEY (id_rack) REFERENCES racks(id_rack)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS alicuotas_pbmc (
        id_pbmc INTEGER PRIMARY KEY AUTOINCREMENT,
        id_voluntario TEXT,
        tipo_toma TEXT,
        numero_alicuota INTEGER,
        conteo_celular TEXT,
        fecha_ingreso TEXT,
        id_rack TEXT,
        fila INTEGER,
        columna INTEGER,
        FOREIGN KEY (id_voluntario) REFERENCES voluntarios(id_voluntario),
        FOREIGN KEY (id_rack) REFERENCES racks(id_rack)
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS usuarios (
        id_usuario INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        aprobado INTEGER DEFAULT 0,
        es_admin INTEGER DEFAULT 0,
        fecha_registro TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS calidad_no_conformidades (
        id_no_conformidad INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT UNIQUE NOT NULL,
        titulo TEXT NOT NULL,
        descripcion TEXT NOT NULL,
        origen TEXT NOT NULL,
        area TEXT NOT NULL,
        severidad TEXT NOT NULL,
        estado TEXT NOT NULL DEFAULT 'Abierta',
        detectado_por TEXT NOT NULL,
        responsable TEXT NOT NULL,
        fecha_deteccion TEXT NOT NULL,
        fecha_compromiso TEXT,
        causa_raiz TEXT,
        verificacion_cierre TEXT,
        fecha_cierre TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS calidad_acciones (
        id_accion INTEGER PRIMARY KEY AUTOINCREMENT,
        id_no_conformidad INTEGER NOT NULL,
        titulo TEXT NOT NULL,
        descripcion TEXT NOT NULL,
        tipo_accion TEXT NOT NULL,
        responsable TEXT NOT NULL,
        estado TEXT NOT NULL DEFAULT 'Abierta',
        fecha_inicio TEXT NOT NULL,
        fecha_compromiso TEXT,
        fecha_cierre TEXT,
        verificacion_eficacia TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (id_no_conformidad) REFERENCES calidad_no_conformidades(id_no_conformidad)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS calidad_evidencias (
        id_evidencia INTEGER PRIMARY KEY AUTOINCREMENT,
        tipo_entidad TEXT NOT NULL,
        id_entidad INTEGER NOT NULL,
        nombre_archivo TEXT NOT NULL,
        ruta_archivo TEXT NOT NULL,
        descripcion TEXT,
        subido_por TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS calidad_auditorias (
        id_auditoria INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT UNIQUE NOT NULL,
        titulo TEXT NOT NULL,
        area TEXT NOT NULL,
        auditor_lider TEXT NOT NULL,
        fecha_programada TEXT NOT NULL,
        alcance TEXT,
        criterios TEXT,
        estado TEXT NOT NULL DEFAULT 'Programada',
        resultado TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS calidad_auditoria_hallazgos (
        id_hallazgo INTEGER PRIMARY KEY AUTOINCREMENT,
        id_auditoria INTEGER NOT NULL,
        referencia TEXT NOT NULL,
        descripcion TEXT NOT NULL,
        severidad TEXT NOT NULL,
        estado TEXT NOT NULL DEFAULT 'Abierto',
        responsable TEXT,
        fecha_compromiso TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (id_auditoria) REFERENCES calidad_auditorias(id_auditoria)
    )
    """)

    conn.commit()
    conn.close()
    print("Base de datos y tablas creadas correctamente.")


    
