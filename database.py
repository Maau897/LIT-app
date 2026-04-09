import sqlite3

DB_NAME = "iner_voluntarios.db"

def conectar_db():
    return sqlite3.connect(DB_NAME)

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

    conn.commit()
    conn.close()
    print("Base de datos y tablas creadas correctamente.")