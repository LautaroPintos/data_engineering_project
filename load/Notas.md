## Arquitectura de la fase de Load

Lo primero que hacemos es crear una base de datos en el data warehouse de la compañía con el objetivo de poder responder preguntas de negocio y hacer recomendaciones del estado actual sobre los aterrizajes y despegues.

```sql

create database aterrizajes_despegues;

use database aterrizajes_despegues;

```

Luego, se realiza la creación de las tablas (externas) que se van a alojar en un repositorio de hdfs "/table/.*". La creación de las tablas se realiza en el código "Tablas_Hive.sql"
