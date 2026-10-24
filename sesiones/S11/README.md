# S11 - BI/ML distribuido con Spark

Equipo **LLSW3** — Proyecto Sello "Sistema de monitoreo Big Data del tráfico de red del campus universitario (UPeU Juliaca)".

| Archivo | Parte | Contenido |
|---|---|---|
| `11_bi_ml_distribuido.ipynb` | 3.1 – 3.11 (guiada) | KPIs del flujo de `atmos-eventos` (Parquet de S9) publicados una vez en el Pushgateway; modelo CE de S10 redesplegado sin reentrenar sobre `campo-electrico-eventos`, publicando cada micro-lote en vivo; confirmación con PromQL y auditoría al detener la consulta. |
| `11_proyecto_red_bi_ml.ipynb` | 4.1 (autónoma) | KPIs de negocio del flujo de red (lecturas, validez, latencia, tráfico promedio y pico) sobre el Parquet de observabilidad propio de S9; modelo de tráfico t+1 de S10 redesplegado sobre `red-telemetria`, publicando real, predicción y error en vivo por tarjeta; PromQL separado en el tiempo y auditoría Regla #37. |
| `obs/` | 3.1 | `compose.yml` con Prometheus, Grafana y **Pushgateway** (:49091) y el *scrape job* `pushgateway` con `honor_labels: true`. |
| `grafana/crear_tableros.py` | 3.11 / 4.1 | Crea por API los tableros `S11 - BI/ML distribuido` y `S11 - Proyecto LLSW3: BI/ML tráfico de red` (paneles *Stat* batch + *Time series* en vivo). |

## Métricas publicadas

| Grupo del Pushgateway | Tipo de *job* | Métricas |
|---|---|---|
| `job=lambda26_bi_flujo`, `instance=s11_batch` | batch (una vez) | `lambda26_bi_eventos_total`, `lambda26_bi_eventos_validos_ratio`, `lambda26_bi_latencia_promedio_ms` |
| `job=lambda26_ml_streaming`, `instance=<estacionId>` | continuo | `lambda26_ml_ce_real`, `lambda26_ml_prediccion_siguiente` |
| `job=llsw3_bi_red`, `instance=s11_batch` | batch (una vez) | `llsw3_bi_lecturas_total`, `llsw3_bi_lecturas_validas_ratio`, `llsw3_bi_latencia_promedio_ms`, `llsw3_bi_rx_promedio_bytes`, `llsw3_bi_rx_pico_bytes`, `llsw3_bi_utilizacion_enlace_ratio` |
| `job=llsw3_ml_red`, `instance=<sensorId>` | continuo | `llsw3_ml_rx_real`, `llsw3_ml_rx_prediccion_siguiente`, `llsw3_ml_error_abs_vivo` |

## Ejecución

```bash
cd obs && docker compose up -d            # Prometheus :49090, Grafana :43000, Pushgateway :49091
python grafana/crear_tableros.py           # tableros de Grafana
```

Los notebooks se ejecutan en el contenedor `lambda26-pyspark` (`/opt/s11-bi-ml-distribuido`) y leen los artefactos de S9 (`atmos_eventos_observabilidad`, `red_telemetria_observabilidad`) y S10 (`modelo_ce_lr`, `modelo_red_rx_t1`).
