import os
import sys

# Asegurar que en Windows las rutas con espacios no rompan los workers de Python en Spark
if sys.platform == "win32":
    import ctypes
    _buf = ctypes.create_unicode_buffer(260)
    if ctypes.windll.kernel32.GetShortPathNameW(sys.executable, _buf, 260) > 0:
        _py = _buf.value
    else:
        _py = sys.executable
    os.environ["PYSPARK_PYTHON"] = _py
    os.environ["PYSPARK_DRIVER_PYTHON"] = _py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, DoubleType
from threading import Lock

# Evita que las consultas mezclen sus encabezados y tablas en la consola.
salida_consola = Lock()


def mostrar_alertas(nombre, lote, batch_id):
    if lote.isEmpty():
        return
    with salida_consola:
        print("\n" + "-" * 50)
        print(f"{nombre} — batch {batch_id}")
        print("-" * 50)
        lote.show(truncate=False)


def mostrar_velocidad(lote, batch_id):
    mostrar_alertas("Velocidad", lote, batch_id)

#1. SparkSession con el conector de kafka y pocas particiones de shuffle
spark = (SparkSession.builder
         .appName("ConsumidorFlota") # Asigna un nombre a la aplicación.
         .master("local[*]") # Ejecuta Spark, usando todos los núcleos disponibles.
         .config("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
            # Carga el conector que permite a Spark trabajar con Kafka.
         .config("spark.sql.shuffle.partitions","4")
         .config("spark.ui.showConsoleProgress", "false") # Desactiva la barra de progreso en la consola.
         .config("spark.python.worker.reuse", "false") # Evita problemas de conexión en Windows
         .getOrCreate()) # Obtiene una sesión existente o crea una nueva.
spark.sparkContext.setLogLevel("ERROR")

#2. Leer el flujo desde kafka
crudo = (spark.readStream # indica que los datos llegarán continuamente.
         .format("kafka") # selecciona Kafka como fuente.
         .option("kafka.bootstrap.servers","localhost:9092") # dirección para conectarse.
         .option("subscribe","flota") # topic que va a leer.
         .option("startingOffsets","latest") # una consulta nueva empieza desde el final del topic
         .load()) # crea el DataFrame de streaming llamado crudo.

#3. El valor viene como bytes y lo tenemos que pasar a texto -> JSON -> columnas

# Le explica a Spark qué campos debe encontrar dentro del JSON y sus tipos
esquema = (StructType()
           .add("id_vehiculo", StringType())
           .add("velocidad", DoubleType())
           .add("combustible", DoubleType())
           .add("lat", DoubleType())
           .add("lon", DoubleType()))


# Conversión de bytes a columnas
tx = crudo.select(
    "timestamp",
    F.from_json(
        F.col("value").cast("string"),  # Selecciona value y convierte sus bytes en texto.
        esquema,  # Interpreta ese texto JSON usando el esquema, se convierte en struct.
    ).alias("d"),
).select("timestamp", "d.*")

# Vista común para las reglas que usen este margen de atraso.
# El watermark permite cerrar ventanas antiguas y liberar su estado.
# Avanza con el tiempo de los eventos, con 5 segundos de margen para atrasos.
tx = tx.withWatermark("timestamp", "5 seconds")
tx.createOrReplaceTempView("flota")


#4. EXCESO DE VELOCIDAD SOSTENIDO
# alertar cuando un vehículo supera los 100 km/h en 3 o más lecturas dentro de 
# una ventana de tiempo (no un pico aislado, sino sostenido).

sql_velocidad = """
    SELECT
        id_vehiculo,
        window(timestamp, '15 seconds', '1 second') AS intervalo,
        SUM(CASE WHEN velocidad > 100 THEN 1 ELSE 0 END) AS lecturas_exceso
    FROM flota
    GROUP BY id_vehiculo, window(timestamp, '15 seconds', '1 second')
    HAVING lecturas_exceso >= 3
"""
# Ventanas de 15 segundos, que se mueven cada segundo.
# Se usa CASE en lugar de WHERE para que los reportes de velocidad normal 
# también hagan avanzar el watermark, sin sumar al conteo de excesos.

alertas_velocidad = spark.sql(sql_velocidad)

# Append muestra las alertas cuando el watermark permite cerrar la ventana.
# Los mismos excesos pueden generar alertas en varias ventanas solapadas.
consulta_velocidad = (
    alertas_velocidad.writeStream
    .outputMode("append")
    .foreachBatch(mostrar_velocidad)
    .start()
)

#5. CAÍDA ANÓMALA DE COMBUSTIBLE
# alertar cuando el combustible de un vehículo baja mucho más rápido de lo normal 
# entre lecturas (posible robo o fuga). Pista: necesitan comparar cada lectura con la 
# anterior del mismo vehículo. Se genera alerta cuando la caida es mayor a 2.

# Una lectura por vehículo, guardada en memoria entre batches.
ultimas_lecturas_combustible = []


def revisar_combustible(lote, batch_id):
    global ultimas_lecturas_combustible # Para actualizar la lista definida fuera de la función.
    sesion = lote.sparkSession
    datos = lote.select("id_vehiculo", "timestamp", "combustible") # Solo las columnas necesarias.
    datos.createOrReplaceTempView("lote_actual") # Las lecturas nuevas que llegaron en ese batch.

    # 1. Unir las lecturas anteriores con las nuevas
    if ultimas_lecturas_combustible:
        sesion.createDataFrame(ultimas_lecturas_combustible, datos.schema).createOrReplaceTempView("ultima_lectura")
        sesion.sql("""
            SELECT * FROM ultima_lectura
            UNION ALL
            SELECT * FROM lote_actual
        """).createOrReplaceTempView("lecturas_combustible")
    else:
        datos.createOrReplaceTempView("lecturas_combustible")

    # 2. Comparar con la lectura anterior y mostrar caídas mayores a 2.
    # Ejecuta la consulta y guarda el resultado en un DataFrame.

    # LAG copia el valor de combustible de la fila anterior (según el orden por timestamp) para cada vehículo.
    alertas = sesion.sql("""
        WITH comparaciones AS (
            SELECT *, LAG(combustible) OVER (
                PARTITION BY id_vehiculo ORDER BY timestamp
            ) AS combustible_anterior
            FROM lecturas_combustible
        )
        SELECT id_vehiculo, timestamp, combustible_anterior, combustible,
               combustible_anterior - combustible AS caida
        FROM comparaciones
        WHERE combustible_anterior - combustible > 2
    """)
    mostrar_alertas("Combustible", alertas, batch_id)

    # 3. Conservar solo la última lectura de cada vehículo para el próximo batch en una lista.
    ultimas_lecturas_combustible = sesion.sql("""
        WITH ordenadas AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY id_vehiculo ORDER BY timestamp DESC
            ) AS posicion
            FROM lecturas_combustible
        )
        SELECT id_vehiculo, timestamp, combustible
        FROM ordenadas
        WHERE posicion = 1
    """).collect() # Ejecuta la consulta y devuelve sus filas como una lista de objetos Row de Python.


consulta_combustible = (
    spark.table("flota") # obtiene como DataFrame la vista flota.
    .writeStream # prepara la escritura de streaming.
    .foreachBatch(revisar_combustible) # Spark llama a la funcion y le pasa los parametros.
    .start() # Spark arranca a procesar los datos disponibles y llama a la función para cada batch.
)

#6. VEHÍCULO EN SILENCIO
# alertar cuando un vehículo deja de emitir telemetría (posible falla o accidente).
# Pista: rastreen la última emisión de cada vehículo. Se alerta si la inactividad supera 10 segundos.

ultimas_emisiones = {} # Guarda la última emisión y posición conocida de cada vehículo: {id: (ts, lat, lon)}
UMBRAL_SILENCIO_SEGUNDOS = 10.0 # Telemetría normal llega cada 1s; 10s indica silencio crítico.

def revisar_silencio(lote, batch_id):
    global ultimas_emisiones
    sesion = lote.sparkSession

    # 1. Obtener la última emisión y coordenadas de cada vehículo presente en el lote actual
    recientes = (lote.select("id_vehiculo", "timestamp", "lat", "lon")
                 .groupBy("id_vehiculo")
                 .agg(F.max("timestamp").alias("max_ts"),
                      F.last("lat").alias("lat"),
                      F.last("lon").alias("lon"))
                 .collect())

    tiempo_actual = None
    for r in recientes:
        ultimas_emisiones[r.id_vehiculo] = (r.max_ts, r.lat, r.lon)
        if tiempo_actual is None or r.max_ts > tiempo_actual:
            tiempo_actual = r.max_ts

    if not tiempo_actual or not ultimas_emisiones:
        return

    # 2. Comparar el tiempo actual del flujo contra la última emisión de cada vehículo
    alertas_silencio = []
    for v_id, (ts, lat, lon) in ultimas_emisiones.items():
        segundos_inactivo = (tiempo_actual - ts).total_seconds()
        if segundos_inactivo >= UMBRAL_SILENCIO_SEGUNDOS:
            alertas_silencio.append((
                v_id,
                ts.strftime("%Y-%m-%d %H:%M:%S") if hasattr(ts, "strftime") else str(ts),
                round(segundos_inactivo, 1),
                lat,
                lon
            ))

    # 3. Mostrar alertas si hay vehículos mudos
    if alertas_silencio:
        esquema_silencio = (StructType()
                            .add("id_vehiculo", StringType())
                            .add("ultimo_contacto", StringType())
                            .add("segundos_en_silencio", DoubleType())
                            .add("lat", DoubleType())
                            .add("lon", DoubleType()))
        df_silencio = sesion.createDataFrame(alertas_silencio, esquema_silencio)
        mostrar_alertas("Silencio", df_silencio, batch_id)


consulta_silencio = (
    spark.table("flota")
    .writeStream
    .foreachBatch(revisar_silencio)
    .start()
)

print("Detectores de velocidad, combustible y silencio corriendo. Ctrl+C para detener.")
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    pass
finally:
    consulta_silencio.stop()
    consulta_combustible.stop()
    consulta_velocidad.stop()
    spark.stop()
