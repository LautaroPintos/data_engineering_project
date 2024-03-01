from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, to_date
sc = SparkContext('local')
spark = SparkSession(sc)

from pyspark.sql import HiveContext
hc = HiveContext(sc)

##leemos la informacion que descargamos
df = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/examen_uno/informe_2021.csv")
df2 = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/examen_uno/informe_2022.csv")
df3 = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/examen_uno/aeropuertos_detalles.csv")

##Lo primero que hacemos es seleccionar y castear las columnas que necestiamos
##De las dos primeras tablas no vamos a utilizar la columna "calidad dato"
##De la tercer tabla no vamos a utilizar las columnas "inhab", "fir"

df_transf = df.select(
    to_date(col("Fecha"), "dd/MM/yyyy").alias("fecha"),
    col("Hora UTC").alias("horaUTC"),
    col("Clase de Vuelo (todos los vuelos)").alias("clase_de_vuelo"),
    col("Clasificación Vuelo").alias("clasificacion_de_vuelo"),
    col("Tipo de Movimiento").alias("tipo_de_movimiento"),
    col("Aeropuerto").alias("aeropuerto"),
    col("Origen / Destino").alias("origen_destino"),
    col("Aerolinea Nombre").alias("aerolinea_nombre"),
    col("Aeronave").alias("aeronave"),
    col("Pasajeros").cast("integer").alias("pasajeros")
)

df2_transf = df2.select(
    to_date(col("Fecha"), "dd/MM/yyyy").alias("fecha"),
    col("Hora UTC").alias("horaUTC"),
    col("Clase de Vuelo (todos los vuelos)").alias("clase_de_vuelo"),
    col("Clasificación Vuelo").alias("clasificacion_de_vuelo"),
    col("Tipo de Movimiento").alias("tipo_de_movimiento"),
    col("Aeropuerto").alias("aeropuerto"),
    col("Origen / Destino").alias("origen_destino"),
    col("Aerolinea Nombre").alias("aerolinea_nombre"),
    col("Aeronave").alias("aeronave"),
    col("Pasajeros").cast("integer").alias("pasajeros")
)

df3_transf = df3.select(
    col("local").alias("aeropuerto"),
    col("oaci").alias("oac"),
    col("iata"),
    col("tipo"),
    col("denominacion"),
    col("coordenadas"),
    col("latitud"),
    col("longitud"),
    col("elev").cast("float"),
    col("uom_elev"),
    col("ref"),
    col("distancia_ref").cast("float"),
    col("direccion_ref"),
    col("condicion"),
    col("control"),
    col("region"),
    col("uso"),
    col("trafico"),
    col("sna"),
    col("concesionado"),
    col("provincia")
)

##Para luego aplicar una unica vez los filtros y reemplazos de nulos lo que hacemos es ya hacer el union
df_total = df_transf.union(df2_transf)

##Aplicamos los filtros
df_total_new = df_total.filter(df_total.clasificacion_de_vuelo != "Internacional")

##Agregamos este filtro por las dudas
df_total_new = df_total_new.filter((df_total_new.fecha <= "2022-06-30") & (df_total_new.fecha >= "2021-01-01"))

##Reemplazamos los nulos que dice la consigna.
df_total_new = df_total_new.na.fill(0, ["pasajeros"])
df3_transf = df3_transf.na.fill(0, ["distancia_ref"])

##Enviamos la informacion a las tablas de hive creadas
df_total_new.write.mode("overwrite").format("hive").saveAsTable("aterrizajes_despegues.aeropuerto_tabla")
df3_transf.write.mode("overwrite").format("hive").saveAsTable("aterrizajes_despegues.aeropuerto_detalles_tabla")


