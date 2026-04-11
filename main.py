import sqlite3
from logic import hash_password
from database import conectar_db
from database import crear_tablas
from logic import (
    inicializar_rack_suero,
    registrar_voluntario,
    ver_racks,
    ver_rack_suero,
    ver_rack_pbmc,
    imprimir_rack,
    generar_voluntarios_prueba
)

crear_tablas()

def ver_tablas():
    conn = sqlite3.connect("iner_voluntarios.db")
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tablas = cursor.fetchall()

    conn.close()

    print("\nTABLAS EN LA BASE:")
    for tabla in tablas:
        print(tabla)


def main():
    # 1. Crear tablas
    crear_tablas()

    # 2. Verificar que sí existan todas
    ver_tablas()

    # 3. Inicializar rack de suero
    inicializar_rack_suero()

    # 4. Generar voluntarios de prueba
    #voluntarios = generar_voluntarios_prueba(15)

    # 5. Registrar voluntarios
    #for datos_voluntario in voluntarios:
        #registrar_voluntario(datos_voluntario)

    # 6. Mostrar racks registrados
    print("\nRACKS REGISTRADOS:")
    for rack in ver_racks():
        print(rack)

    # 7. Mostrar rack de suero 1
    print("\nVISUALIZACIÓN DE SUERO_1:")
    rack1 = ver_rack_suero("SUERO_1")
    imprimir_rack(rack1)

    # 8. Mostrar rack de suero 2
    print("\nVISUALIZACIÓN DE SUERO_2:")
    rack2 = ver_rack_suero("SUERO_2")
    imprimir_rack(rack2)

    # 9. Mostrar rack de PBMC
    print("\nVISUALIZACIÓN DE PBMC_1:")
    rack_pbmc = ver_rack_pbmc("PBMC_1")
    imprimir_rack(rack_pbmc)

if __name__ == "__main__":
    main()
