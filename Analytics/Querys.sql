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


-- 6. Determinar la cantidad de vuelos entre las fechas 01/12/2021 y 31/01/2022
select 
	*
from aeropuerto_tabla at2 
where at2.fecha BETWEEN '2021-12-01' and '2022-01-31';

select 
	count(*) as cantidad_vuelos
from aeropuerto_tabla at2 
where at2.fecha BETWEEN '2021-12-01' and '2022-01-31';


-- 7.Cantidad de pasajeros que viajaron en Aerolíneas Argentinas entre el 01/01/2021 y 30/06/2022.
select 
	sum(pasajeros) as cantidad
from aeropuerto_tabla at2 
where aerolinea_nombre = 'AEROLINEAS ARGENTINAS SA' and at2.fecha BETWEEN '2021-01-01' and '2022-06-30';


/* 8. Mostrar fecha, hora, código aeropuerto salida, ciudad de salida, código de aeropuerto
 	de arribo, ciudad de arribo, y cantidad de pasajeros de cada vuelo, entre el 01/01/2022
 	y el 30/06/2022 ordenados por fecha de manera descendiente */
select 
	at2.fecha ,
	at2.horautc ,
	at2.aeropuerto ,
	COALESCE (adt.ref, adt.denominacion, adt.provincia) as ciudad_origen,
	at2.origen_destino ,
	COALESCE (adt2.ref, adt2.denominacion, adt2.provincia) as ciudad_arribo,
	pasajeros
from aeropuerto_tabla at2 
left join aeropuerto_detalles_tabla adt on at2.aeropuerto = adt.aeropuerto 
left join aeropuerto_detalles_tabla adt2 on at2.origen_destino  = adt2.aeropuerto 
where at2.fecha BETWEEN '2022-01-01' and '2022-06-30'
order by at2.fecha desc;


/* 10. Cuales son las 10 aeronaves más utilizadas entre el 01/01/2021 y el 30/06/22 que
despegaron desde la Ciudad autónoma de Buenos Aires o de Buenos Aires,
exceptuando aquellas aeronaves que no cuentan con nombre */
select 
	at2.aeronave ,
	at2.aeropuerto ,
	adt.provincia as lugar_despegue,
	at2.origen_destino ,
	pasajeros
from aeropuerto_tabla at2 
left join aeropuerto_detalles_tabla adt on at2.aeropuerto = adt.aeropuerto 
where adt.provincia in ("BUENOS AIRES", "CIUDAD AUTÓNOMA DE BUENOS AIRES") and at2.fecha BETWEEN '2022-01-01' and '2022-06-30';
