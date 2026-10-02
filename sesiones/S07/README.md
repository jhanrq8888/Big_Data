# S07 - Ingesta de eventos IoT/sensores en tiempo real

Código de la sesión 7 del curso Big Data 2026-2 (UPeU) — equipo **LLSW3**, Proyecto Sello
"Sistema de monitoreo Big Data del tráfico de red del campus universitario (UPeU Juliaca)". Ambos módulos corren sobre el
Kafka del repositorio del curso [`lambda26`](https://github.com/262bigdata/lambda26)
(`kafka/compose.yml`), que crea la red Docker `lambda26-kafka-net`. Los comandos de cada
README asumen que la carpeta del módulo está dentro de una copia de `lambda26`.

| Carpeta | Parte de la guía | Qué hace |
|---|---|---|
| `uso-atmos/` | 3.1 – 3.8 (guiada) | Simulador de 3 sensores ESP32 → topic `atmos-eventos` (3 particiones); consumer con validación de esquema (`invalid`) y de rango físico (`alerta`); puente MQTT → Kafka; ESP32 en Wokwi (`wokwi/`) publicando por `test.mosquitto.org`. |
| `uso-red/` | 4.1 (autónoma) | Sensor real del Proyecto Sello: contadores de tráfico de la tarjeta de red (Intel Wi-Fi 6E AX211) del nodo de captura. Agente en Windows → Mosquitto propio → puente → topic `red-telemetria` → consumer con `consumed`, `invalid`, `alerta` y `saturacion`. |

Cambio respecto al repositorio del curso: en `uso-atmos/wokwi/diagram.json` el monitor serie
se conecta a `esp:TX`/`esp:RX` (la placa `board-esp32-devkit-c-v4` no tiene pines `TX0`/`RX0`;
con esos nombres el monitor serie no aparece).
