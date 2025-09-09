# etl_polars.py
# Explorador de tesoros con Polars ⚡

from datetime import datetime
import polars as pl
import os
print("Estoy ejecutando desde:", os.getcwd())

# 1. Ruta del archivo
archivo = "data/ventas.csv"

try:
    # 2. Leer el archivo CSV con Polars
    df = pl.read_csv(archivo)

    # 3. Mostrar las primeras filas
    print("=== Primer vistazo al tesoro de datos ===")
    print(df.head())

    # 4. Mostrar la estructura de columnas
    print("\n=== ¿Qué columnas tiene nuestro cofre? ===")
    print(df.columns)

    # 5. Calcular un tesoro: el total de ventas (cantidad * precio)
    df = df.with_columns(
        (pl.col("cantidad") * pl.col("precio")).alias("total")
    )

    print("\n=== Ahora con columna 'total' incluida ===")
    print(df)

    # 6. Otro tesoro: ¿cuánto vendimos en total?
    suma_total = df["total"].sum()
    print(f"\n💰 Tesoro encontrado: el total de ventas es ${suma_total:,}")

except FileNotFoundError:
    print(f"El archivo {archivo} no existe. Colócalo en la carpeta 'data'.")

# 7. ¿Cuál producto generó más ingresos?
top_producto = (
    df.group_by("producto")
      .agg(pl.col("total").sum().alias("ingresos"))
      .sort("ingresos", descending=True)
      .head(1)
)

print("\n👑 Producto top por ingresos:")
print(top_producto)

# 8. Agrupar por fecha: total vendido y ticket promedio
ventas_diarias = (
    df.group_by("fecha")
      .agg([
          pl.col("total").sum().alias("ventas_totales"),
          (pl.col("total").sum() / pl.col("cantidad").sum()).alias("ticket_promedio")
      ])
    .sort("fecha")
)

print("\n📆 Ventas agrupadas por fecha:")
print(ventas_diarias)

# 9. Exportar resultados a CSV con timestamp en el nombre

# Crear timestamp (ej: 2025-09-02_1530)
timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")

# Ruta del archivo exportado
nombre_archivo = f"output/reporte_ventas_diarias_{timestamp}.csv"

# Guardar el DataFrame agrupado
ventas_diarias.write_csv(nombre_archivo)

print(f"\n📁 Reporte exportado correctamente a: {nombre_archivo}")
