# S09 - Observabilidad con Grafana y costos

Equipo **LLSW3** — Proyecto Sello "Sistema de monitoreo Big Data del tráfico de red del campus universitario (UPeU Juliaca)".

| Archivo | Parte | Contenido |
|---|---|---|
| `09_observabilidad_pipeline_grafana_costos.ipynb` | 3.4 – 3.11 (guiada) | `atmos-eventos` con `isValid`/`processedAt`/`latencyMs`, consola + Parquet, `lastProgress`, latencia por minuto, alertas con umbrales medidos y costos. |
| `09_proyecto_red_observabilidad.ipynb` | 4.1 (autónoma) | Lo mismo sobre el topic real `red-telemetria` (lecturas de la tarjeta de red del nodo, `sesiones/S07/uso-red`). |
| `obs/` | 3.1 | Prometheus (scrape de `kafka-exporter`) y Grafana con datasource provisto. |
| `obs/grafana/crear_tablero_y_alerta.py` | 4.1 | Crea por API el tablero "S9 - Proyecto LLSW3: pipeline red-telemetria" y la alerta "Agente de red sin datos". |
