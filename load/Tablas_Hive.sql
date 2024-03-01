CREATE EXTERNAL TABLE aeropuerto_tabla (
  fecha DATE,
  horaUTC STRING,
  clase_de_vuelo STRING,
  clasificacion_de_vuelo STRING,
  tipo_de_movimiento STRING,
  aeropuerto STRING,
  origen_destino STRING,
  aerolinea_nombre STRING,
  aeronave STRING,
  pasajeros INT
)
comment 'Información de vuelos'
row format delimited
fields terminated by ','
stored as textfile
location '/table/informes_vuelos';

CREATE EXTERNAL TABLE aeropuerto_detalles_tabla (
  aeropuerto STRING,
  oac STRING,
  iata STRING,
  tipo STRING,
  denominacion STRING,
  coordenadas STRING,
  latitud STRING,
  longitud STRING,
  elev FLOAT,
  uom_elev STRING,
  ref STRING,
  distancia_ref FLOAT,
  direccion_ref STRING,
  condicion STRING,
  control STRING,
  region STRING,
  uso STRING,
  trafico STRING,
  sna STRING,
  concesionado STRING,
  provincia STRING
)
comment 'Información de areopuertos'
row format delimited
fields terminated by ','
stored as textfile
location '/table/informes_aeropuertos';