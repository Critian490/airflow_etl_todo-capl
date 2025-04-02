import sqlite3
import os

# Pistas 
# 1. Conectarse a la base de datos donde están las tablas Silver
# 2. Guarda los queries realizados en el trabajo pasado como un string

def transform_data():
    db_path = '/opt/airflow/dags/data/ecommerce.db'

    try:
        # 1️⃣ Conectar a la base de datos
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print("✅ Conectado a la base de datos.")

        # 2️⃣ Query 1: Top 10 estados con mayor ingreso
        query1 = """
        DROP TABLE IF EXISTS gold_top_states;
        CREATE TABLE gold_top_states AS
        SELECT state, SUM(total_amount) AS total_revenue
        FROM Silver
        GROUP BY state
        ORDER BY total_revenue DESC
        LIMIT 10;
        """
        
        # 3️⃣ Query 2: Comparación de tiempos reales vs estimados por mes y año
        query2 = """
        DROP TABLE IF EXISTS gold_delivery_comparison;
        CREATE TABLE gold_delivery_comparison AS
        SELECT 
            strftime('%Y-%m', delivery_date) AS month_year,
            AVG(actual_delivery_time - estimated_delivery_time) AS avg_delay
        FROM Silver
        GROUP BY month_year
        ORDER BY month_year;
        """

        print("🚀 Ejecutando queries para crear tablas Gold...")
        cursor.executescript(query1)  # Ejecutar Query 1
        cursor.executescript(query2)  # Ejecutar Query 2

        # 4️⃣ Guardar los cambios y cerrar la conexión
        conn.commit()
        conn.close()
        print("✅ Tablas Gold creadas en ecommerce.db: 'gold_top_states' y 'gold_delivery_comparison'")

    except sqlite3.Error as e:
        print(f"❌ Error en transform_data: {e}")