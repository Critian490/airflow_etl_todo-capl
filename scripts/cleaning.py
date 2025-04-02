import sqlite3

#Pistas
# En este paso la idea sería quitar duplicados, manejar nulos, pero como no es el objetivo, 
# Vamos a hacer una copia espejo de los datos, simulando que los datos ya están limpios.
# 1.Conectarse a la base de datos ecommerce.db ubicada en /opt/airflow/dags/data
# 2: Elimine la tabla Silver si ya existe, cree una tabla nueva Silver copiando 
#    todo el contenido de su tabla Bronze correspondiente
#    Cada bloque debe hacer una copia de la tabla Bronze a una nueva tabla Silver
# 3: Guardar los cambios y cerrar la conexión
# 4: Usa print() para mostrar el estado del proceso

def cleaning_data():
    db_path = '/opt/airflow/dags/data/ecommerce.db'

    try:
        # 1 Conectar a la base de datos
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print("Conexión exitosa")

        # 2 Eliminar la tabla Silver si ya existe
        cursor.execute("DROP TABLE IF EXISTS Silver;")
        print("Ejecución de eliminación de la tabla Silver exitosa.")

        # 3 Crear la tabla Silver copiando los datos de la tabla Bronze
        cursor.execute("CREATE TABLE Silver AS SELECT * FROM Bronze;")
        print("Tabla Silver creada como copia de Bronze.")

        # Guardar los cambios y cerrar la conexión
        conn.commit()
        conn.close()
        print("Los cambios fueron guardados y la conexión está cerrada.")

    except sqlite3.Error as e:
        print(f"Error en cleaning_data: {e}")