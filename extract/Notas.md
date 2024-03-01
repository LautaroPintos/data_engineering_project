## Arquitectura de la fase de extract

El proyecto se trabajo dentro del ecosistema de Hadoop. Se definio en primera instancia
una zona de landing para luego hacer el ingest en Hadoop hdfs.

La zona de landing se encuentra en el directorio: "/home/hadoop/landing/examen_uno"

Luego se realiza el ingest en HDFS y se puede contrastar mediante:

```bash

hdfs dfs -ls /ingest/examen_uno

```
