
## Pipeline de datos - A.N. Aviación civil

El objetivo del presente repositorio es desarrollar un pipeline de datos esta la información disponible del ministerio de transporte, automatizarlo y
realizar distintos análisis de datos que permitan responder preguntas de negocio, y hacer sus recomendaciones con respecto al estado actual de los aterrizajes y despegues de Argentina.

El proyecto se llevó a cabo utilizando docker. Acá se creo un contenedor con todo lo necesario para poder correr el proyecto:

* Ecosistema Hadoop (Hdfs, Yarn, MapReduce, Hive)
* Spark
* Airflow

La forma de gestionar el contenedor es a través de la terminal mediante los comandos:

```bash

docker start hadoop

docker exec -it edvai_hadoop bash

```

Adicionalmente, se agrega al pipeline una etapa de Analytics a modo de ejemplo para que distintos tipos de usuarios (power users y regular users) puedan ejecutar consultas sobre la base de datos.

La forma de parar el contenedor es mediante: 

```bash

docker stop hadoop

```

Fuentes de datos:

* https://datos.transporte.gob.ar/dataset/aterrizajes-y-despegues-procesados-por-la-administracion-nacional-de-aviacion-civil-anac

* https://datos.transporte.gob.ar/dataset/lista-aeropuertos


