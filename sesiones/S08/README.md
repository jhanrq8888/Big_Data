# S08 - Procesamiento en streaming con Spark: ventanas, watermarking y semántica de entrega

Equipo **LLSW3** — Proyecto Sello "Sistema de monitoreo Big Data del tráfico de red del campus universitario (UPeU Juliaca)".

| Archivo | Parte de la guía | Contenido |
|---|---|---|
| `08_streaming_estructurado_practica.ipynb` | 3.1 – 3.13 (guiada) | Spark Structured Streaming sobre `atmos-eventos` (simulador de S7): esquema explícito, ventana fija sin/con watermark, ventana deslizante, checkpoint, dato tardío descartado, `dropDuplicates` acotado, intervalos de disparo y salida a Parquet. Incluye las salidas de la corrida y los hallazgos (3.13). |
| `08_proyecto_red_telemetria.ipynb` | 4.1 (autónoma) | Lo mismo sobre el topic propio `red-telemetria` (lecturas reales de las tarjetas de red del nodo, publicadas por `sesiones/S07/uso-red`): ventana de 10 s con watermark de 10 s, lectura real tardía descartada, lectura real duplicada contada una vez, disparo 1 s vs 6 s, checkpoint, error de `update` a Parquet y persistencia en Parquet en dos corridas. |
| `pyspark/compose.kafka.yml` | 3.1 | Agrega la red `lambda26-kafka-net` al contenedor `pyspark` de `lambda26` (`docker compose -f compose.yml -f compose.kafka.yml up -d --build`). |

Los notebooks se ejecutan dentro del contenedor `lambda26-pyspark` (carpeta `sesiones/s08-streaming-estructurado/` de `lambda26`), con Kafka, `uso-atmos` y `uso-red` corriendo.
