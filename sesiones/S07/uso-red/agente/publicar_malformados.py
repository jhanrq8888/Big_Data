"""Publica por MQTT mensajes que NO cumplen el contrato red.lectura, para
verificar que consumer_red.py los marca como invalid sin caerse."""
import json
import os
import time

import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "41883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "lambda26/red/llsw3/telemetria")

casos = {
    "texto plano (no es JSON)": "NIC desconectada: ERR_LINK",
    "JSON sin velocidadEnlaceMbps": json.dumps({
        "tipoEvento": "red.lectura", "sensorId": "llsw3-wi-fi-2", "interfaz": "Wi-Fi 2",
        "tipoInterfaz": "fisica", "bytesRecibidosSeg": 100.0, "bytesEnviadosSeg": 50.0,
        "paquetesRecibidosSeg": 2.0, "paquetesEnviadosSeg": 1.0, "erroresEntrada": 0,
        "erroresSalida": 0, "descartesEntrada": 0, "descartesSalida": 0,
        "timestamp": int(time.time() * 1000)}),
    "bytesRecibidosSeg como texto": json.dumps({
        "tipoEvento": "red.lectura", "sensorId": "llsw3-wi-fi-2", "interfaz": "Wi-Fi 2",
        "tipoInterfaz": "fisica", "velocidadEnlaceMbps": 144, "bytesRecibidosSeg": "mucho",
        "bytesEnviadosSeg": 50.0, "paquetesRecibidosSeg": 2.0, "paquetesEnviadosSeg": 1.0,
        "erroresEntrada": 0, "erroresSalida": 0, "descartesEntrada": 0, "descartesSalida": 0,
        "timestamp": int(time.time() * 1000)}),
}

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(MQTT_HOST, MQTT_PORT)
client.loop_start()
for nombre, payload in casos.items():
    client.publish(MQTT_TOPIC, payload).wait_for_publish()
    print(f"publicado [{nombre}]: {payload[:90]}")
    time.sleep(1)
client.loop_stop()
