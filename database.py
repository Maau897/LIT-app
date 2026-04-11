import sqlite3
from pathlib import Path

DB_NAME = Path(__file__).resolve().parent / "iner_voluntarios.db"


def conectar_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def crear_tablas():
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
    conn.commit()
    conn.close()
    print("Base de datos y tablas creadas correctamente.")


    
